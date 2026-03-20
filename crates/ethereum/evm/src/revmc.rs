//! revmc JIT compiler integration for EVM execution.
//!
//! Provides [`RevmcEvmFactory`] which produces [`RevmcEvm`] instances that use the revmc JIT
//! coordinator to execute compiled bytecode when available, falling back to the interpreter
//! otherwise.

use alloy_evm::{
    eth::{EthEvmBuilder, EthEvmContext},
    precompiles::PrecompilesMap,
    Database, Evm, EvmEnv, EvmFactory,
};
use alloy_primitives::{Address, Bytes};
use revm::{
    context::{BlockEnv, ContextSetters, Evm as RevmEvm, TxEnv},
    context_interface::result::{EVMError, HaltReason, ResultAndState},
    handler::{instructions::EthInstructions, EthFrame, EvmTr, FrameResult, Handler, ItemOrResult},
    inspector::NoOpInspector,
    interpreter::{interpreter::EthInterpreter, interpreter_action::FrameInit, InterpreterResult},
    primitives::hardfork::SpecId,
    ExecuteEvm, InspectEvm, Inspector, SystemCallEvm,
};
use revmc::runtime::{
    JitCoordinator, JitCoordinatorHandle, LookupDecision, LookupRequest, RuntimeConfig,
};

type InnerEvm<DB, I, P> =
    RevmEvm<EthEvmContext<DB>, I, EthInstructions<EthInterpreter, EthEvmContext<DB>>, P, EthFrame>;

/// Owns the [`JitCoordinator`] for the node's lifetime and provides handles.
///
/// The coordinator is not `Sync` (it owns `mpsc::Receiver`), so this type should be held in a
/// non-shared context (e.g. main thread). Use [`RevmcRuntime::handle`] or [`RevmcRuntime::factory`]
/// to get `Send + Sync` types for passing into the EVM pipeline.
#[expect(missing_debug_implementations)]
pub struct RevmcRuntime {
    coordinator: JitCoordinator,
}

impl RevmcRuntime {
    /// Starts the revmc runtime with the given configuration.
    pub fn start(config: RuntimeConfig) -> eyre::Result<Self> {
        let coordinator = JitCoordinator::start(config)?;
        Ok(Self { coordinator })
    }

    /// Returns a clonable handle for performing lookups.
    pub fn handle(&self) -> JitCoordinatorHandle {
        self.coordinator.handle()
    }

    /// Returns a [`RevmcEvmFactory`] that can be used with [`EthEvmConfig`].
    ///
    /// [`EthEvmConfig`]: crate::EthEvmConfig
    pub fn factory(&self) -> RevmcEvmFactory {
        RevmcEvmFactory { handle: self.handle() }
    }

    /// Shuts down the coordinator.
    pub fn shutdown(self) -> eyre::Result<()> {
        self.coordinator.shutdown()
    }
}

/// Factory producing [`RevmcEvm`] instances with JIT-compiled bytecode support.
///
/// Holds only the [`JitCoordinatorHandle`] which is `Send + Sync + Clone`.
#[derive(Clone)]
pub struct RevmcEvmFactory {
    handle: JitCoordinatorHandle,
}

impl core::fmt::Debug for RevmcEvmFactory {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RevmcEvmFactory").finish_non_exhaustive()
    }
}

impl RevmcEvmFactory {
    /// Creates a new factory from a coordinator handle.
    pub const fn new(handle: JitCoordinatorHandle) -> Self {
        Self { handle }
    }

    /// Creates a factory with JIT disabled (no coordinator running).
    ///
    /// Starts a coordinator with `enabled: false` so lookups always return `Interpret`.
    pub fn disabled() -> Self {
        let runtime = RevmcRuntime::start(RuntimeConfig::default())
            .expect("failed to start disabled revmc runtime");
        runtime.factory()
    }
}

impl EvmFactory for RevmcEvmFactory {
    type Evm<DB: Database, I: Inspector<EthEvmContext<DB>>> = RevmcEvm<DB, I, PrecompilesMap>;
    type Context<DB: Database> = EthEvmContext<DB>;
    type Tx = TxEnv;
    type Error<DBError: core::error::Error + Send + Sync + 'static> = EVMError<DBError>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = PrecompilesMap;

    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> Self::Evm<DB, NoOpInspector> {
        let inner = EthEvmBuilder::new(db, input).build().into_inner();
        RevmcEvm { inner, inspect: false, handle: self.handle.clone() }
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        let inner =
            EthEvmBuilder::new(db, input).activate_inspector(inspector).build().into_inner();
        RevmcEvm { inner, inspect: true, handle: self.handle.clone() }
    }
}

/// EVM wrapper that dispatches to revmc JIT-compiled bytecode when available.
///
/// When `inspect` is true (tracing/debugging), always falls back to the interpreter to preserve
/// inspector semantics.
#[expect(missing_debug_implementations)]
pub struct RevmcEvm<DB: Database, I, PRECOMPILE = PrecompilesMap> {
    inner: InnerEvm<DB, I, PRECOMPILE>,
    inspect: bool,
    handle: JitCoordinatorHandle,
}

impl<DB: Database, I, P> core::ops::Deref for RevmcEvm<DB, I, P> {
    type Target = EthEvmContext<DB>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner.ctx
    }
}

impl<DB: Database, I, P> core::ops::DerefMut for RevmcEvm<DB, I, P> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner.ctx
    }
}

/// Runs the execution loop with JIT dispatch on the concrete `RevmEvm` type.
///
/// Uses direct field access to avoid borrow conflicts between `frame_stack` and `ctx`.
fn run_exec_loop_jit<DB, I, P>(
    evm: &mut InnerEvm<DB, I, P>,
    handle: &JitCoordinatorHandle,
    first_frame_input: FrameInit,
) -> Result<FrameResult, EVMError<DB::Error>>
where
    DB: Database,
    I: Inspector<EthEvmContext<DB>>,
    P: revm::handler::PrecompileProvider<EthEvmContext<DB>, Output = InterpreterResult>,
{
    let res = evm.frame_init(first_frame_input)?;
    if let ItemOrResult::Result(frame_result) = res {
        return Ok(frame_result);
    }

    let spec_id = evm.ctx.cfg.spec;

    loop {
        let call_or_result = {
            let frame = evm.frame_stack.get();
            let bytecode_hash = frame.interpreter.bytecode.get_or_calculate_hash();
            let code = frame.interpreter.bytecode.original_byte_slice();

            let req = LookupRequest { code_hash: bytecode_hash, code, spec_id };
            match handle.lookup(req) {
                LookupDecision::Compiled(program) => {
                    let ctx = &mut evm.ctx;
                    let action =
                        unsafe { program.func.call_with_interpreter(&mut frame.interpreter, ctx) };
                    frame.process_next_action::<_, EVMError<DB::Error>>(ctx, action).inspect(
                        |i| {
                            if i.is_result() {
                                frame.set_finished(true);
                            }
                        },
                    )?
                }
                LookupDecision::Interpret(_) => evm.frame_run()?,
            }
        };

        let result = match call_or_result {
            ItemOrResult::Item(init) => match evm.frame_init(init)? {
                ItemOrResult::Item(_) => continue,
                ItemOrResult::Result(result) => result,
            },
            ItemOrResult::Result(result) => result,
        };

        if let Some(result) = evm.frame_return_result(result)? {
            return Ok(result);
        }
    }
}

/// Custom handler that overrides only `run_exec_loop` with JIT dispatch.
struct RevmcHandler<'a, DB: Database, I, P> {
    handle: &'a JitCoordinatorHandle,
    _phantom: core::marker::PhantomData<(DB, I, P)>,
}

impl<DB, I, P> Handler for RevmcHandler<'_, DB, I, P>
where
    DB: Database,
    I: Inspector<EthEvmContext<DB>>,
    P: revm::handler::PrecompileProvider<EthEvmContext<DB>, Output = InterpreterResult>,
{
    type Evm = InnerEvm<DB, I, P>;
    type Error = EVMError<DB::Error>;
    type HaltReason = HaltReason;

    fn run_exec_loop(
        &mut self,
        evm: &mut Self::Evm,
        first_frame_input: FrameInit,
    ) -> Result<FrameResult, Self::Error> {
        run_exec_loop_jit(evm, self.handle, first_frame_input)
    }
}

impl<DB, I, P> Evm for RevmcEvm<DB, I, P>
where
    DB: Database,
    I: Inspector<EthEvmContext<DB>>,
    P: revm::handler::PrecompileProvider<EthEvmContext<DB>, Output = InterpreterResult>,
{
    type DB = DB;
    type Tx = TxEnv;
    type Error = EVMError<DB::Error>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = P;
    type Inspector = I;

    fn block(&self) -> &BlockEnv {
        &self.inner.ctx.block
    }

    fn chain_id(&self) -> u64 {
        self.inner.ctx.cfg.chain_id
    }

    fn transact_raw(
        &mut self,
        tx: Self::Tx,
    ) -> Result<ResultAndState<Self::HaltReason>, Self::Error> {
        if self.inspect {
            self.inner.inspect_tx(tx)
        } else {
            self.inner.ctx.set_tx(tx);
            let mut handler: RevmcHandler<'_, DB, I, P> =
                RevmcHandler { handle: &self.handle, _phantom: core::marker::PhantomData };
            handler.run(&mut self.inner).map(|result| {
                let state = self.inner.finalize();
                ResultAndState::new(result, state)
            })
        }
    }

    fn transact_system_call(
        &mut self,
        caller: Address,
        contract: Address,
        data: Bytes,
    ) -> Result<ResultAndState<Self::HaltReason>, Self::Error> {
        self.inner.system_call_with_caller(caller, contract, data)
    }

    fn finish(self) -> (Self::DB, EvmEnv<Self::Spec>) {
        let revm::Context { block: block_env, cfg: cfg_env, journaled_state, .. } = self.inner.ctx;
        (journaled_state.database, EvmEnv { block_env, cfg_env })
    }

    fn set_inspector_enabled(&mut self, enabled: bool) {
        self.inspect = enabled;
    }

    fn components(&self) -> (&Self::DB, &Self::Inspector, &Self::Precompiles) {
        (&self.inner.ctx.journaled_state.database, &self.inner.inspector, &self.inner.precompiles)
    }

    fn components_mut(&mut self) -> (&mut Self::DB, &mut Self::Inspector, &mut Self::Precompiles) {
        (
            &mut self.inner.ctx.journaled_state.database,
            &mut self.inner.inspector,
            &mut self.inner.precompiles,
        )
    }
}
