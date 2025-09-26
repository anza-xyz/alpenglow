//! Logic detailing the migration from TowerBFT to Alpenglow
//!
//! The migration process will begin after a certain slot offset in the first epoch
//! where the `alpenglow` feature flag is active.
//!
//! Once the migration starts:
//! - We enter vote only mode, no user txs will be present in blocks
//! - We stop rooting or reporting OC/Finalizations
//!
//! During the migration:
//! 1) We track blocks which have `GENESIS_VOTE_THRESHOLD`% of stake's vote txs for the parent block
//! 2) Notice that all blocks that are eligble must be a part of the same fork in presence of
//!    less than `MIGRATION_MALICIOUS_THRESHOLD` double voters
//! 3) We find the earliest such block, `G` and cast a BLS vote (the genesis vote) via all to all
//! 4) If we observe `GENESIS_VOTE_THRESHOLD`% votes for an eligble block `G`:
//!    5a) We bump the shred version, and clear any TowerBFT blocks past `G`.
//!    5b) We propagate the Genesis certificate for `G` via all to all
//! 5) We initialize Votor with `G` as genesis, and disable TowerBFT for any slots past `G`
//! 6) We exit vote only mode, and reenable rooting and commitment reporting
//!
//! If at any point during the migration we see:
//! - A genesis certificate
//! - A finalization certificate
//!
//! It means the cluster has already switched to Alpenglow and our node is behind. We perform any appropriate
//! repairs and immediately transition to Alpenglow at the certified block.
use {
    crate::consensus_message::{Block, Certificate, CertificateMessage},
    log::*,
    solana_clock::Slot,
    spl_pod::solana_pubkey::Pubkey,
    std::sync::{
        atomic::{AtomicBool, AtomicU64 as AtomicSlot, Ordering},
        Arc, RwLock,
    },
};

/// The slot offset post feature flag activation to begin the migration.
/// Epoch boundaries induce heavy computation often resulting in forks. It's best to decouple the migration period
/// from the boundary. We require that a root is made between the epoch boundary and this migration slot offset.
pub const MIGRATION_SLOT_OFFSET: Slot = 5000;

/// We match Alpenglow's 20 + 20 model, by allowing a maximum of 20% malicious stake during the migration.
pub const MIGRATION_MALICIOUS_THRESHOLD: f64 = 20.0 / 100.0;

/// In order to rollback a block eligble for genesis vote, we need:
/// `SWITCH_FORK_THRESHOLD` - (1 - `GENESIS_VOTE_THRESHOLD`) = `MIGRATION_MALICIOUS_THRESHOLD` malicious stake.
///
/// Using 38% as the `SWITCH_FORK_THRESHOLD` gives us 82% for `GENESIS_VOTE_THRESHOLD`.
pub const GENESIS_VOTE_THRESHOLD: f64 = 82.0 / 100.0;

/// Tracks the details about the migrationary period and eligble genesis blocks
#[derive(Default, Debug)]
pub struct GenesisVoteTracker {
    /// The block which we think is the genesis and cast our genesis vote on
    pub(crate) genesis_block: Option<Block>,
    /// The genesis certificate received from the cluster
    pub(crate) genesis_certificate: Option<CertificateMessage>,
}

/// Keeps track of the current migration status
#[derive(Debug)]
pub struct MigrationStatus {
    /// Flag indicating whether PoH has shutdown
    pub shutdown_poh: AtomicBool,

    /// Flag indicating whether the block creation loop has started
    pub block_creation_loop_started: AtomicBool,

    /// The slot in which we entered the migrationary period
    /// u64::MAX if we do not yet know when the migration will start
    pub(crate) start_migration_slot: AtomicSlot,

    pub(crate) genesis_vote_tracker: RwLock<GenesisVoteTracker>,
}

impl Default for MigrationStatus {
    /// Create an empty MigrationStatus corresponding to pre Alpenglow ff activation
    fn default() -> Self {
        Self {
            shutdown_poh: AtomicBool::new(false),
            block_creation_loop_started: AtomicBool::new(false),
            start_migration_slot: AtomicSlot::new(u64::MAX),
            genesis_vote_tracker: RwLock::new(GenesisVoteTracker::default()),
        }
    }
}

impl MigrationStatus {
    /// The alpenglow feature flag has been activated in slot `slot`.
    /// This should only be called using the feature account of a *rooted* slot,
    /// as otherwise we might have diverging views of the migration slot.
    pub fn notify_feature_flag(&self, my_pubkey: &Pubkey, slot: Slot) {
        if self.migration_slot().is_some() {
            error!("{my_pubkey}: Attempting to set the migration slot but it is already set");
            return;
        }
        let migration_slot = slot.saturating_add(MIGRATION_SLOT_OFFSET);
        warn!(
            "{my_pubkey}: Alpenglow feature flag was activated in {slot}, migration will start at \
             {migration_slot}"
        );
        self.start_migration_slot
            .store(migration_slot, Ordering::Release);
    }

    /// The first slot of the migrationary period
    pub fn migration_slot(&self) -> Option<Slot> {
        let slot = self.start_migration_slot.load(Ordering::Relaxed);
        (slot != u64::MAX).then_some(slot)
    }

    /// Has the migration completed, and alpenglow enabled
    pub fn is_alpenglow_enabled(&self) -> bool {
        self.shutdown_poh.load(Ordering::Relaxed)
            && self.block_creation_loop_started.load(Ordering::Relaxed)
    }

    /// Genesis slot. All further slots should be handled via Alpenglow.
    /// Returns `None` if the migration is not complete.
    pub fn genesis_slot(&self) -> Option<Slot> {
        if !self.is_alpenglow_enabled() {
            return None;
        }
        let (genesis_slot, _) = self
            .genesis_vote_tracker
            .read()
            .unwrap()
            .genesis_block
            .expect("If alpenglow is enabled there must be a genesis");
        Some(genesis_slot)
    }

    /// Checks whether our view of the Alpenglow genesis block has been certified
    pub fn is_genesis_certified(&self) -> bool {
        let genesis_tracker_r = self.genesis_vote_tracker.read().unwrap();
        let Some(g_block) = genesis_tracker_r.genesis_block else {
            return false;
        };
        let Some(ref g_cert) = genesis_tracker_r.genesis_certificate else {
            return false;
        };

        // Check if the cert matches our genesis block. If it does not, we must wait for duplicate
        // block repair to fix this.
        g_block
            == g_cert
                .certificate
                .to_block()
                .expect("Genesis cert must contain a block")
    }

    /// Enable alpenglow:
    /// - Shutdown Poh
    /// - Start the block creation loop
    /// - If we are enabling post migrationary period (not from genesis):
    ///     - Reenable rooting
    ///     - Reenable OC/Finalized commitments
    pub fn enable_alpenglow(&self, my_pubkey: &Pubkey) {
        if self.is_alpenglow_enabled() {
            error!("{my_pubkey}: Attempting to enable alpenglow but it is already enabled");
            return;
        }

        warn!("Shutting down poh");

        self.shutdown_poh.store(true, Ordering::Relaxed);
        while !self.block_creation_loop_started.load(Ordering::Relaxed) {
            // Wait for PohService to shutdown poh and start the block creation loop
            std::hint::spin_loop();
        }

        warn!("{my_pubkey}: Alpenglow enabled!");
    }

    /// Set our view of the genesis block.
    ///
    /// If we have received a genesis certificate and it matches the genesis block, enable alpenglow
    pub fn set_genesis_block(&self, my_pubkey: &Pubkey, genesis: Block) {
        let mut genesis_tracker_w = self.genesis_vote_tracker.write().unwrap();
        if let Some(prev) = genesis_tracker_w.genesis_block.replace(genesis) {
            warn!("{my_pubkey} Setting new genesis block {genesis:?} from {prev:?}")
        } else {
            warn!("{my_pubkey} Setting genesis block {genesis:?}");
        }
        drop(genesis_tracker_w);

        if !self.is_alpenglow_enabled() && self.is_genesis_certified() {
            self.enable_alpenglow(my_pubkey);
        }
    }

    /// Set the genesis certificate. If one already exists, verify that it matches.
    /// This should only be called with certificates that have passed signature verification
    ///
    /// If the certificate matches our view of the genesis block, enable alpenglow
    pub fn set_genesis_certificate(&self, my_pubkey: &Pubkey, cert: Arc<CertificateMessage>) {
        match cert.certificate {
            Certificate::Finalize(_)
            | Certificate::FinalizeFast(_, _)
            | Certificate::Notarize(_, _)
            | Certificate::NotarizeFallback(_, _)
            | Certificate::Skip(_) => {
                unreachable!("Programmer error adding invalid genesis certificate")
            }
            Certificate::Genesis(slot, block_id) => {
                let mut genesis_tracker_w = self.genesis_vote_tracker.write().unwrap();
                genesis_tracker_w.genesis_certificate = Some((*cert).clone());
                let Some(genesis) = genesis_tracker_w.genesis_block else {
                    return;
                };
                if genesis != (slot, block_id) {
                    error!(
                        "{my_pubkey}: We cast a genesis vote on {genesis:?}, however we have \
                         received a genesis certificate for ({slot}, {block_id}). This indicates \
                         two possibilities:
                                1) There is significant malicious activity causing two distinct \
                         forks to reach the {GENESIS_VOTE_THRESHOLD}. We cannot recover without \
                         operator intervention.
                                2) We have received a duplicate block in slot {}, while the \
                         cluster received a version which does not meet the genesis conditions. \
                         Duplicate block resolution will repair the correct version and we will \
                         finish migration after repair succeeds.",
                        genesis.0
                    );
                    return;
                }
            }
        }

        if !self.is_alpenglow_enabled() && self.is_genesis_certified() {
            self.enable_alpenglow(my_pubkey);
        }
    }
}
