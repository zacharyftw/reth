//! Contains a precompile cache backed by `schnellru::LruMap` (LRU by length).

use alloy_primitives::{
    map::{DefaultHashBuilder, FbBuildHasher},
    Bytes,
};
use moka::policy::EvictionPolicy;
use reth_evm::precompiles::{
    DynPrecompile, Precompile, PrecompileInput, PrecompileOutputExt, PrecompileResultExt,
};
use reth_primitives_traits::dashmap::DashMap;
use revm::{interpreter::gas::GasTracker, precompile::PrecompileId};
use revm_primitives::Address;
use std::{hash::Hash, sync::Arc};

/// Default max cache size for [`PrecompileCache`]
const MAX_CACHE_SIZE: u32 = 10_000;

/// Stores caches for each precompile.
#[derive(Debug, Clone, Default)]
pub struct PrecompileCacheMap<S>(Arc<DashMap<Address, PrecompileCache<S>, FbBuildHasher<20>>>)
where
    S: Eq + Hash + std::fmt::Debug + Send + Sync + Clone + 'static;

impl<S> PrecompileCacheMap<S>
where
    S: Eq + Hash + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    /// Get the precompile cache for the given address.
    pub fn cache_for_address(&self, address: Address) -> PrecompileCache<S> {
        // Try just using `.get` first to avoid acquiring a write lock.
        if let Some(cache) = self.0.get(&address) {
            return cache.clone();
        }
        // Otherwise, fallback to `.entry` and initialize the cache.
        //
        // This should be very rare as caches for all precompiles will be initialized as soon as
        // first EVM is created.
        self.0.entry(address).or_default().clone()
    }
}

/// Cache for precompiles, for each input stores the result.
#[derive(Debug, Clone)]
pub struct PrecompileCache<S>(moka::sync::Cache<Bytes, CacheEntry<S>, DefaultHashBuilder>)
where
    S: Eq + Hash + std::fmt::Debug + Send + Sync + Clone + 'static;

impl<S> Default for PrecompileCache<S>
where
    S: Eq + Hash + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    fn default() -> Self {
        Self(
            moka::sync::CacheBuilder::new(MAX_CACHE_SIZE as u64)
                .initial_capacity(MAX_CACHE_SIZE as usize)
                .eviction_policy(EvictionPolicy::lru())
                .build_with_hasher(Default::default()),
        )
    }
}

impl<S> PrecompileCache<S>
where
    S: Eq + Hash + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    fn get(&self, input: &[u8], spec: S) -> Option<CacheEntry<S>> {
        self.0.get(input).filter(|e| e.spec == spec)
    }

    /// Inserts the given key and value into the cache, returning the new cache size.
    fn insert(&self, input: Bytes, value: CacheEntry<S>) -> usize {
        self.0.insert(input, value);
        self.0.entry_count() as usize
    }
}

/// Cache entry, precompile successful output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry<S> {
    output: PrecompileOutputExt,
    spec: S,
}

impl<S> CacheEntry<S> {
    const fn regular_gas_used(&self) -> u64 {
        self.output.gas.limit() - self.output.gas.remaining()
    }

    /// Construct a result using the cached output bytes and regular gas cost,
    /// but with the *caller's current* gas limit and reservoir so we don't
    /// overwrite live state-gas accounting (`reservoir`, `state_gas_spent`).
    fn to_precompile_result(&self, gas_limit: u64, reservoir: u64) -> PrecompileResultExt {
        let gas_used = self.regular_gas_used();
        Ok(PrecompileOutputExt {
            gas: GasTracker::new(gas_limit, gas_limit - gas_used, reservoir),
            bytes: self.output.bytes.clone(),
            reverted: self.output.reverted,
        })
    }
}

/// A cache for precompile inputs / outputs.
#[derive(Debug)]
pub struct CachedPrecompile<S>
where
    S: Eq + Hash + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    /// Cache for precompile results and gas bounds.
    cache: PrecompileCache<S>,
    /// The precompile.
    precompile: DynPrecompile,
    /// Cache metrics.
    metrics: Option<CachedPrecompileMetrics>,
    /// Spec id associated to the EVM from which this cached precompile was created.
    spec_id: S,
}

impl<S> CachedPrecompile<S>
where
    S: Eq + Hash + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    /// `CachedPrecompile` constructor.
    pub const fn new(
        precompile: DynPrecompile,
        cache: PrecompileCache<S>,
        spec_id: S,
        metrics: Option<CachedPrecompileMetrics>,
    ) -> Self {
        Self { precompile, cache, spec_id, metrics }
    }

    /// Wrap the given precompile in a cached precompile.
    pub fn wrap(
        precompile: DynPrecompile,
        cache: PrecompileCache<S>,
        spec_id: S,
        metrics: Option<CachedPrecompileMetrics>,
    ) -> DynPrecompile {
        let precompile_id = precompile.precompile_id().clone();
        let wrapped = Self::new(precompile, cache, spec_id, metrics);
        (precompile_id, move |input: PrecompileInput<'_>| -> PrecompileResultExt {
            wrapped.call(input)
        })
            .into()
    }

    fn increment_by_one_precompile_cache_hits(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.precompile_cache_hits.increment(1);
        }
    }

    fn increment_by_one_precompile_cache_misses(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.precompile_cache_misses.increment(1);
        }
    }

    fn set_precompile_cache_size_metric(&self, to: f64) {
        if let Some(metrics) = &self.metrics {
            metrics.precompile_cache_size.set(to);
        }
    }

    fn increment_by_one_precompile_errors(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.precompile_errors.increment(1);
        }
    }
}

impl<S> Precompile for CachedPrecompile<S>
where
    S: Eq + Hash + std::fmt::Debug + Send + Sync + Clone + 'static,
{
    fn precompile_id(&self) -> &PrecompileId {
        self.precompile.precompile_id()
    }

    fn call(&self, input: PrecompileInput<'_>) -> PrecompileResultExt {
        if let Some(entry) = &self.cache.get(input.data, self.spec_id.clone()) &&
            input.gas >= entry.regular_gas_used()
        {
            self.increment_by_one_precompile_cache_hits();
            return entry.to_precompile_result(input.gas, input.reservoir);
        }

        let calldata = input.data;
        let result = self.precompile.call(input);

        match &result {
            Ok(output) => {
                let size = self.cache.insert(
                    Bytes::copy_from_slice(calldata),
                    CacheEntry { output: output.clone(), spec: self.spec_id.clone() },
                );
                self.set_precompile_cache_size_metric(size as f64);
                self.increment_by_one_precompile_cache_misses();
            }
            _ => {
                self.increment_by_one_precompile_errors();
            }
        }
        result
    }
}

/// Metrics for the cached precompile.
#[derive(reth_metrics::Metrics, Clone)]
#[metrics(scope = "sync.caching")]
pub struct CachedPrecompileMetrics {
    /// Precompile cache hits
    pub precompile_cache_hits: metrics::Counter,

    /// Precompile cache misses
    pub precompile_cache_misses: metrics::Counter,

    /// Precompile cache size. Uses the LRU cache length as the size metric.
    pub precompile_cache_size: metrics::Gauge,

    /// Precompile execution errors.
    pub precompile_errors: metrics::Counter,
}

impl CachedPrecompileMetrics {
    /// Creates a new instance of [`CachedPrecompileMetrics`] with the given address.
    ///
    /// Adds address as an `address` label padded with zeros to at least two hex symbols, prefixed
    /// by `0x`.
    pub fn new_with_address(address: Address) -> Self {
        Self::new_with_labels(&[("address", format!("0x{address:02x}"))])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_evm::{EthEvmFactory, Evm, EvmEnv, EvmFactory};
    use reth_revm::db::EmptyDB;
    use revm::{context::TxEnv, interpreter::gas::GasTracker};
    use revm_primitives::hardfork::SpecId;

    #[test]
    fn test_precompile_cache_basic() {
        let dyn_precompile: DynPrecompile = (|_input: PrecompileInput<'_>| -> PrecompileResultExt {
            Ok(PrecompileOutputExt {
                gas: GasTracker::new(0, 0, 0),
                bytes: Bytes::default(),
                reverted: false,
            })
        })
        .into();

        let cache =
            CachedPrecompile::new(dyn_precompile, PrecompileCache::default(), SpecId::PRAGUE, None);

        let output = PrecompileOutputExt {
            gas: GasTracker::new(50, 0, 0),
            bytes: alloy_primitives::Bytes::copy_from_slice(b"cached_result"),
            reverted: false,
        };

        let input = b"test_input";
        let expected = CacheEntry { output, spec: SpecId::PRAGUE };
        cache.cache.insert(input.into(), expected.clone());

        let actual = cache.cache.get(input, SpecId::PRAGUE).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_precompile_cache_map_separate_addresses() {
        let mut evm = EthEvmFactory::default().create_evm(EmptyDB::default(), EvmEnv::default());
        let input_data = b"same_input";
        let gas_limit = 100_000;

        let address1 = Address::repeat_byte(1);
        let address2 = Address::repeat_byte(2);

        let cache_map = PrecompileCacheMap::default();

        // create the first precompile with a specific output
        let precompile1: DynPrecompile = (PrecompileId::custom("custom"), {
            move |input: PrecompileInput<'_>| -> PrecompileResultExt {
                assert_eq!(input.data, input_data);

                Ok(PrecompileOutputExt {
                    gas: GasTracker::new(5000, 0, 0),
                    bytes: alloy_primitives::Bytes::copy_from_slice(b"output_from_precompile_1"),
                    reverted: false,
                })
            }
        })
            .into();

        // create the second precompile with a different output
        let precompile2: DynPrecompile = (PrecompileId::custom("custom"), {
            move |input: PrecompileInput<'_>| -> PrecompileResultExt {
                assert_eq!(input.data, input_data);

                Ok(PrecompileOutputExt {
                    gas: GasTracker::new(7000, 0, 0),
                    bytes: alloy_primitives::Bytes::copy_from_slice(b"output_from_precompile_2"),
                    reverted: false,
                })
            }
        })
            .into();

        let wrapped_precompile1 = CachedPrecompile::wrap(
            precompile1,
            cache_map.cache_for_address(address1),
            SpecId::PRAGUE,
            None,
        );
        let wrapped_precompile2 = CachedPrecompile::wrap(
            precompile2,
            cache_map.cache_for_address(address2),
            SpecId::PRAGUE,
            None,
        );

        let precompile1_address = Address::with_last_byte(1);
        let precompile2_address = Address::with_last_byte(2);

        evm.precompiles_mut().apply_precompile(&precompile1_address, |_| Some(wrapped_precompile1));
        evm.precompiles_mut().apply_precompile(&precompile2_address, |_| Some(wrapped_precompile2));

        // first invocation of precompile1 (cache miss)
        let result1 = evm
            .transact_raw(TxEnv {
                caller: Address::ZERO,
                gas_limit,
                data: input_data.into(),
                kind: precompile1_address.into(),
                ..Default::default()
            })
            .unwrap()
            .result
            .into_output()
            .unwrap();
        assert_eq!(result1.as_ref(), b"output_from_precompile_1");

        // first invocation of precompile2 with the same input (should be a cache miss)
        // if cache was incorrectly shared, we'd get precompile1's result
        let result2 = evm
            .transact_raw(TxEnv {
                caller: Address::ZERO,
                gas_limit,
                data: input_data.into(),
                kind: precompile2_address.into(),
                ..Default::default()
            })
            .unwrap()
            .result
            .into_output()
            .unwrap();
        assert_eq!(result2.as_ref(), b"output_from_precompile_2");

        // second invocation of precompile1 (should be a cache hit)
        let result3 = evm
            .transact_raw(TxEnv {
                caller: Address::ZERO,
                gas_limit,
                data: input_data.into(),
                kind: precompile1_address.into(),
                ..Default::default()
            })
            .unwrap()
            .result
            .into_output()
            .unwrap();
        assert_eq!(result3.as_ref(), b"output_from_precompile_1");
    }

    /// Cache hits must return the *caller's current* gas_limit and reservoir,
    /// not the stale values captured during the original (miss) execution.
    #[test]
    fn test_cache_hit_preserves_caller_reservoir() {
        let precompile_gas_cost = 5000u64;

        let cache_entry = CacheEntry {
            output: PrecompileOutputExt {
                // Original call had gas_limit=100_000, reservoir=800
                gas: GasTracker::new(100_000, 100_000 - precompile_gas_cost, 800),
                bytes: Bytes::copy_from_slice(b"result"),
                reverted: false,
            },
            spec: SpecId::PRAGUE,
        };

        assert_eq!(cache_entry.regular_gas_used(), precompile_gas_cost);

        // Simulate a cache hit where caller has different gas_limit and reservoir.
        let caller_gas = 50_000u64;
        let caller_reservoir = 200u64;
        let result = cache_entry.to_precompile_result(caller_gas, caller_reservoir).unwrap();

        // The returned GasTracker must reflect the *caller's* values.
        assert_eq!(result.gas.limit(), caller_gas);
        assert_eq!(result.gas.remaining(), caller_gas - precompile_gas_cost);
        assert_eq!(result.gas.reservoir(), caller_reservoir);
        assert_eq!(result.gas.state_gas_spent(), 0);
    }
}
