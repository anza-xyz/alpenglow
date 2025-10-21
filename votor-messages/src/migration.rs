//! Logic detailing the migration from TowerBFT to Alpenglow
//!
//! The migration process will begin after a certain slot offset in the first epoch
//! where the `alpenglow` feature flag is active.
//!
//! Once the migration starts:
//! - We enter vote only mode, no user txs will be present in blocks
//! - We stop rooting or reporting OC/Finalizations
//!
//! During the migration starting at slot `s`:
//! 1) We track blocks which have `GENESIS_VOTE_THRESHOLD`% of stake's vote txs for the parent block.
//!    The parent block is referred to as reaching super OC.
//! 2) Notice that all super OC blocks that must be a part of the same fork in presence of
//!    less than `MIGRATION_MALICIOUS_THRESHOLD` double voters
//! 3) We find the latest ancestor of the super OC block < `s`, `G` and cast a BLS vote (the genesis vote) via all to all
//! 4) If we observe `GENESIS_VOTE_THRESHOLD`% votes for the ancestor block `G`:
//!    5a) We clear any TowerBFT blocks past `G`.
//!    5b) We propagate the Genesis certificate for `G` via all to all
//! 5) We initialize Votor with `G` as genesis, and disable TowerBFT for any slots past `G`
//! 6) We exit vote only mode, and reenable rooting and commitment reporting
//!
//! If at any point during the migration we see a:
//! - A genesis certificate
//! - or a finalization certificate (fast finalization or a slow finalization with notarization)
//!
//! It means the cluster has already switched to Alpenglow and our node is behind. We perform any appropriate
//! repairs and immediately transition to Alpenglow at the certified block.
use {
    crate::consensus_message::{Block, Certificate, CertificateType},
    log::*,
    solana_clock::Slot,
    spl_pod::solana_pubkey::Pubkey,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64 as AtomicSlot, Ordering},
            Arc, Condvar, Mutex, RwLock,
        },
        time::Duration,
    },
};
#[cfg(feature = "dev-context-only-utils")]
use {solana_bls_signatures::Signature as BLSSignature, solana_hash::Hash};

/// The slot offset post feature flag activation to begin the migration.
/// Epoch boundaries induce heavy computation often resulting in forks. It's best to decouple the migration period
/// from the boundary. We require that a root is made between the epoch boundary and this migration slot offset.
#[cfg(not(feature = "dev-context-only-utils"))]
pub const MIGRATION_SLOT_OFFSET: Slot = 5000;

/// Small offset for tests
#[cfg(feature = "dev-context-only-utils")]
pub const MIGRATION_SLOT_OFFSET: Slot = 32;

/// We match Alpenglow's 20 + 20 model, by allowing a maximum of 20% malicious stake during the migration.
pub const MIGRATION_MALICIOUS_THRESHOLD: f64 = 20.0 / 100.0;

/// In order to rollback a block eligble for genesis vote, we need:
/// `SWITCH_FORK_THRESHOLD` - (1 - `GENESIS_VOTE_THRESHOLD`) = `MIGRATION_MALICIOUS_THRESHOLD` malicious stake.
///
/// Using 38% as the `SWITCH_FORK_THRESHOLD` gives us 82% for `GENESIS_VOTE_THRESHOLD`.
pub const GENESIS_VOTE_THRESHOLD: f64 = 82.0 / 100.0;

/// The interval at which we refresh our genesis vote
pub const GENESIS_VOTE_REFRESH: Duration = Duration::from_millis(400);

/// Tracks the details about the migrationary period and eligble genesis block
#[derive(Default, Debug)]
pub struct GenesisTracker {
    /// The block which we think is the genesis and cast our genesis vote on
    pub(crate) genesis_block: Option<Block>,
    /// The genesis certificate received from the cluster
    pub(crate) genesis_certificate: Option<Certificate>,
}

/// Keeps track of the current migration status
#[derive(Debug)]
pub struct MigrationStatus {
    /// The pubkey of this node
    pub(crate) my_pubkey: Pubkey,

    /// Flag indicating whether PoH has shutdown
    pub shutdown_poh: AtomicBool,

    /// Flag indicating whether the block creation loop has started
    pub block_creation_loop_started: AtomicBool,

    /// The slot in which we entered the migrationary period
    /// u64::MAX if we do not yet know when the migration will start
    pub(crate) start_migration_slot: AtomicSlot,

    pub(crate) genesis_tracker: RwLock<GenesisTracker>,
    pub(crate) genesis_wait: (Mutex<()>, Condvar),
}

impl Default for MigrationStatus {
    /// Create an empty MigrationStatus corresponding to pre Alpenglow ff activation
    fn default() -> Self {
        Self::new(Pubkey::new_unique())
    }
}

impl MigrationStatus {
    /// Create a new MigrationStatus with the given pubkey
    pub fn new(my_pubkey: Pubkey) -> Self {
        Self {
            my_pubkey,
            shutdown_poh: AtomicBool::new(false),
            block_creation_loop_started: AtomicBool::new(false),
            start_migration_slot: AtomicSlot::new(u64::MAX),
            genesis_wait: (Mutex::new(()), Condvar::new()),
            genesis_tracker: RwLock::new(GenesisTracker::default()),
        }
    }

    /// Creates a post migration status for use in tests
    #[cfg(feature = "dev-context-only-utils")]
    pub fn post_migration_status() -> Self {
        let genesis_tracker = GenesisTracker {
            genesis_block: Some((0, Hash::default())),
            genesis_certificate: Some(Certificate {
                cert_type: CertificateType::Genesis(0, Hash::default()),
                signature: BLSSignature::default(),
                bitmap: vec![],
            }),
        };
        Self {
            my_pubkey: Pubkey::new_unique(),
            shutdown_poh: AtomicBool::new(true),
            block_creation_loop_started: AtomicBool::new(true),
            start_migration_slot: AtomicSlot::new(0),
            genesis_wait: (Mutex::new(()), Condvar::new()),
            genesis_tracker: RwLock::new(genesis_tracker),
        }
    }

    /// The alpenglow feature flag has been activated in slot `slot`.
    /// This should only be called using the feature account of a *rooted* slot,
    /// as otherwise we might have diverging views of the migration slot.
    pub fn record_feature_activation(&self, slot: Slot) {
        if self.migration_slot().is_some() {
            error!(
                "{}: Attempting to set the migration slot but it is already set",
                self.my_pubkey
            );
            return;
        }
        let migration_slot = slot.saturating_add(MIGRATION_SLOT_OFFSET);
        warn!(
            "{}: Alpenglow feature flag was activated in {slot}, migration will start at \
             {migration_slot}",
            self.my_pubkey
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
        self.genesis_block().map(|(slot, _)| slot)
    }

    /// Genesis block. All further slots should be handled via Alpenglow.
    /// Returns `None` if the migration is not complete.
    pub fn genesis_block(&self) -> Option<Block> {
        if !self.is_alpenglow_enabled() {
            return None;
        }
        Some(
            self.genesis_tracker
                .read()
                .unwrap()
                .genesis_block
                .expect("If alpenglow is enabled there must be a genesis"),
        )
    }

    /// The block that is eligble to be the genesis block, which we wish to cast our genesis vote for.
    /// Returns `None` if the migration is already complete or we have not yet received an eligble block.
    pub fn eligble_genesis_block(&self) -> Option<Block> {
        if self.is_alpenglow_enabled() {
            return None;
        }
        self.genesis_tracker.read().unwrap().genesis_block
    }

    /// Checks whether our view of the Alpenglow genesis block has been certified
    pub fn is_genesis_certified(&self) -> bool {
        let genesis_tracker_r = self.genesis_tracker.read().unwrap();
        let Some(g_block) = genesis_tracker_r.genesis_block else {
            return false;
        };
        let Some(ref g_cert) = genesis_tracker_r.genesis_certificate else {
            return false;
        };

        // Check if the cert matches our genesis block.
        g_block
            == g_cert
                .cert_type
                .to_block()
                .expect("Genesis cert must contain a block")
    }

    /// Enable alpenglow:
    /// - Shutdown Poh
    /// - Start the block creation loop
    /// - Notify all threads that are waiting for the migration
    pub fn enable_alpenglow(&self) {
        if self.is_alpenglow_enabled() {
            error!(
                "{}: Attempting to enable alpenglow but it is already enabled",
                self.my_pubkey
            );
            return;
        }
        assert!(self.is_genesis_certified());

        warn!("{}: Shutting down poh", self.my_pubkey);

        self.shutdown_poh.store(true, Ordering::Relaxed);
        while !self.block_creation_loop_started.load(Ordering::Relaxed) {
            // Wait for PohService to shutdown poh and start the block creation loop
            std::hint::spin_loop();
        }

        let (lock, cvar) = &self.genesis_wait;
        let _guard = lock.lock().unwrap();
        cvar.notify_all();

        warn!("{}: Alpenglow enabled!", self.my_pubkey);
    }

    /// Wait for migration to complete and alpenglow to be enabled or the exit flag.
    /// If successful returns the genesis block.
    pub fn wait_for_migration_or_exit(&self, exit: &AtomicBool) -> Option<Block> {
        let (lock, cvar) = &self.genesis_wait;
        while !self.is_alpenglow_enabled() && !exit.load(Ordering::Relaxed) {
            let _guard = cvar
                .wait_timeout(lock.lock().unwrap(), Duration::from_secs(5))
                .unwrap();
        }
        self.genesis_block()
    }

    /// Set our view of the genesis block. This is the ancestor of the super-oc block prior to the migration slot.
    pub fn set_genesis_block(&self, genesis @ (genesis_slot, _): Block) {
        assert!(genesis_slot < self.migration_slot().expect("Migration must have started"));
        let mut genesis_tracker_w = self.genesis_tracker.write().unwrap();
        match genesis_tracker_w.genesis_block.replace(genesis) {
            Some(prev) => panic!(
                "{} Attempting to overwrite genesis block {genesis:?} from {prev:?}. Programmer \
                 error",
                self.my_pubkey
            ),
            None => warn!("{} Setting genesis block {genesis:?}", self.my_pubkey),
        }
    }

    /// Set the genesis certificate. If one already exists, verify that it matches.
    /// This should only be called with certificates that have passed signature verification
    pub fn set_genesis_certificate(&self, cert: Arc<Certificate>) {
        match cert.cert_type {
            CertificateType::Finalize(_)
            | CertificateType::FinalizeFast(_, _)
            | CertificateType::Notarize(_, _)
            | CertificateType::NotarizeFallback(_, _)
            | CertificateType::Skip(_) => {
                unreachable!("Programmer error adding invalid genesis certificate")
            }
            CertificateType::Genesis(slot, block_id) => {
                let mut genesis_tracker_w = self.genesis_tracker.write().unwrap();
                genesis_tracker_w.genesis_certificate = Some((*cert).clone());
                let Some(genesis) = genesis_tracker_w.genesis_block else {
                    return;
                };
                if genesis != (slot, block_id) {
                    panic!(
                        "{}: We cast a genesis vote on {genesis:?}, however we have received a \
                         genesis certificate for ({slot}, {block_id}). This means there is \
                         significant malicious activity causing two distinct forks to reach the \
                         {GENESIS_VOTE_THRESHOLD}. We cannot recover without operator \
                         intervention.",
                        self.my_pubkey
                    );
                }
            }
        }
    }
}
