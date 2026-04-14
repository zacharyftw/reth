//! Persistence state management for background database operations.
//!
//! This module manages the state of background tasks that persist cached data
//! to the database. The persistence system works asynchronously to avoid blocking
//! block execution while ensuring data durability.
//!
//! ## Background Persistence
//!
//! The execution engine maintains an in-memory cache of canonical blocks and
//! their state changes. Rather than writing synchronously (which would slow
//! down block processing), persistence happens in background tasks.
//!
//! ## Persistence Actions
//!
//! - **Advancing db_tip**: Persist newly executed block structure to disk
//! - **Removing Blocks**: Remove invalid blocks during chain reorganizations
//!
//! ## Coordination
//!
//! The [`PersistenceState`] tracks ongoing persistence operations and coordinates
//! between the main execution thread and background persistence workers.

use crate::persistence::PersistenceResult;
use alloy_eips::BlockNumHash;
use alloy_primitives::B256;
use crossbeam_channel::Receiver as CrossbeamReceiver;
use reth_primitives_traits::FastInstant as Instant;
use tracing::trace;

/// The state of the persistence task.
#[derive(Debug)]
pub struct PersistenceState {
    /// Hash and number of the last block whose block structure was persisted.
    ///
    /// This tracks the on-disk block frontier (`B_db_tip`).
    pub(crate) db_tip: BlockNumHash,
    /// Hash and number of the last block whose state/trie are fully persisted.
    ///
    /// This tracks the fully persisted state frontier (`B_db_checkpoint`).
    pub(crate) db_checkpoint: BlockNumHash,
    /// Receiver end of channel where the result of the persistence task will be
    /// sent when done. A None value means there's no persistence task in progress.
    pub(crate) rx:
        Option<(CrossbeamReceiver<PersistenceResult>, Instant, CurrentPersistenceAction)>,
}

impl PersistenceState {
    /// Create a new persistence state with `db_tip == db_checkpoint`.
    pub(crate) const fn new(persisted: BlockNumHash) -> Self {
        Self { db_tip: persisted, db_checkpoint: persisted, rx: None }
    }

    /// Determines if there is a persistence task in progress by checking if the
    /// receiver is set.
    pub(crate) const fn in_progress(&self) -> bool {
        self.rx.is_some()
    }

    /// Sets the state for a block removal operation.
    pub(crate) fn start_remove(
        &mut self,
        new_tip_num: u64,
        rx: CrossbeamReceiver<PersistenceResult>,
    ) {
        self.rx =
            Some((rx, Instant::now(), CurrentPersistenceAction::RemovingBlocks { new_tip_num }));
    }

    /// Sets the state for a background save operation that advances the persisted frontiers.
    pub(crate) fn start_save_db_tip(
        &mut self,
        highest: BlockNumHash,
        rx: CrossbeamReceiver<PersistenceResult>,
    ) {
        self.rx = Some((rx, Instant::now(), CurrentPersistenceAction::SavingDbTip { highest }));
    }

    /// Returns the current persistence action. If there is no persistence task in progress, then
    /// this returns `None`.
    #[cfg(test)]
    pub(crate) fn current_action(&self) -> Option<&CurrentPersistenceAction> {
        self.rx.as_ref().map(|rx| &rx.2)
    }

    /// Sets state for a finished block removal task.
    pub(crate) fn finish_remove(&mut self, persisted_hash: B256, persisted_number: u64) {
        trace!(target: "engine::tree", block= %persisted_number, hash=%persisted_hash, "updating persistence state after disk removal");
        self.rx = None;
        let persisted = BlockNumHash::new(persisted_number, persisted_hash);
        self.db_tip = persisted;
        if self.db_checkpoint.number > persisted_number {
            self.db_checkpoint = persisted;
        }
    }

    /// Sets both frontiers to the same fully persisted head.
    pub(crate) fn finish_full(&mut self, persisted_hash: B256, persisted_number: u64) {
        trace!(target: "engine::tree", block= %persisted_number, hash=%persisted_hash, "updating full persistence state");
        self.rx = None;
        let persisted = BlockNumHash::new(persisted_number, persisted_hash);
        self.db_tip = persisted;
        self.db_checkpoint = persisted;
    }
}

/// The currently running persistence action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CurrentPersistenceAction {
    /// The persistence task is saving blocks and advancing the persisted frontiers.
    SavingDbTip {
        /// The highest block being saved to `db_tip`.
        highest: BlockNumHash,
    },
    /// The persistence task is removing blocks.
    RemovingBlocks {
        /// The tip, above which we are removing blocks.
        new_tip_num: u64,
    },
}
