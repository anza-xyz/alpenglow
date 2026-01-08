use {
    crossbeam_channel::{Receiver, Sender},
    solana_clock::Slot,
    solana_hash::Hash,
    solana_runtime::bank::Bank,
    solana_votor_messages::consensus_message::Block,
    std::{sync::Arc, time::Instant},
};

pub type RepairEventSender = Sender<RepairEvent>;
pub type RepairEventReceiver = Receiver<RepairEvent>;

/// Events sent by votor to the block id repair service for informed repair
#[derive(Debug, Copy, Clone)]
pub enum RepairEvent {
    /// We require that this block be fetched. This can happen for the following reasons:
    /// - The block has received a NotarizeFallback certificate or stronger
    /// - The specific SafetoNotar case requires parent information, we must repair at least the block header
    /// - We received a block through turbine which specifies a parent that we do not currently have
    FetchBlock { slot: Slot, block_id: Hash },
}

impl RepairEvent {
    pub fn slot(&self) -> Slot {
        match self {
            RepairEvent::FetchBlock { slot, .. } => *slot,
        }
    }
}

pub type SwitchBlockEventSender = Sender<SwitchBlockEvent>;
pub type SwitchBlockEventReceiver = Receiver<SwitchBlockEvent>;

/// Events sent to replay_stage when a block becomes canonical or needs to be switched
#[derive(Debug, Copy, Clone)]
pub enum SwitchBlockEvent {
    /// The block has become canonical (notarized or finalized)
    Canonical { slot: Slot, block_id: Hash },
    /// We need to switch to a different block version
    Switch { slot: Slot, block_id: Hash },
}

#[derive(Debug, Clone)]
pub struct CompletedBlock {
    pub slot: Slot,
    // TODO: once we have the async execution changes this can be (block_id, parent_block_id) instead
    pub bank: Arc<Bank>,
}

/// Context for the block creation loop to start a leader window
#[derive(Copy, Clone, Debug)]
pub struct LeaderWindowInfo {
    pub start_slot: Slot,
    pub end_slot: Slot,
    pub parent_block: Block,
    pub block_timer: Instant,
}

pub type VotorEventSender = Sender<VotorEvent>;
pub type VotorEventReceiver = Receiver<VotorEvent>;

/// Events that trigger actions in Votor
/// TODO: remove bank hash once we update votes
#[derive(Debug, Clone)]
pub enum VotorEvent {
    /// A block has completed replay and is ready for voting
    Block(CompletedBlock),

    /// The block has received a notarization certificate
    BlockNotarized(Block),

    /// The block has received a notarize-fallback certificate
    BlockNotarizeFallback(Block),

    /// Received the first shred for the slot.
    FirstShred(Slot),

    /// The pool has marked the given block as a ready parent for `slot`
    ParentReady { slot: Slot, parent_block: Block },

    //// Timeout to early detect that a honest that has crashed and
    /// if the leader window should be skipped.
    TimeoutCrashedLeader(Slot),

    /// Timeout to inspect whether the remaining leader window should be skipped.
    Timeout(Slot),

    /// The given block has reached the safe to notar status
    SafeToNotar(Block),

    /// The given slot has reached the safe to skip status
    SafeToSkip(Slot),

    /// We are the leader for this window and have reached the parent ready status
    /// Produce the window
    ProduceWindow(LeaderWindowInfo),

    /// The block has received a slow or fast finalization certificate and is eligble for rooting
    /// The second bool indicates whether the block is a fast finalization
    Finalized(Block, bool),

    /// We have not observed a finalization and reached the standstill timeout
    /// The slot is the highest finalized slot
    Standstill(Slot),

    /// The identity keypair has changed due to an operator calling set-identity
    SetIdentity,
}

impl VotorEvent {
    /// Ignore old events
    pub(crate) fn should_ignore(&self, root: Slot) -> bool {
        match self {
            VotorEvent::Block(completed_block) => completed_block.slot <= root,
            VotorEvent::Timeout(s)
            | VotorEvent::SafeToSkip(s)
            | VotorEvent::TimeoutCrashedLeader(s)
            | VotorEvent::FirstShred(s)
            | VotorEvent::SafeToNotar((s, _))
            | VotorEvent::Finalized((s, _), _)
            | VotorEvent::BlockNotarized((s, _))
            | VotorEvent::BlockNotarizeFallback((s, _))
            | VotorEvent::ParentReady {
                slot: s,
                parent_block: _,
            } => s <= &root,
            VotorEvent::ProduceWindow(_) => false,
            VotorEvent::Standstill(_) => false,
            VotorEvent::SetIdentity => false,
        }
    }
}
