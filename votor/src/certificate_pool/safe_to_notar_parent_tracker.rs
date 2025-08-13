use {
    crate::event::BlockParentInfo, solana_clock::Slot, solana_hash::Hash,
    solana_ledger::leader_schedule_utils::first_of_consecutive_leader_slots,
    solana_votor_messages::bls_message::Block, std::collections::BTreeMap,
};

#[derive(Debug, Default)]
struct BlockStatus {
    is_notarized: bool,
    safe_to_notar_stake_reached: bool,
    parent_id: Option<Block>,
    event_sent: bool,
}

pub struct SafeToNotarParentTracker {
    block_status: BTreeMap<Block, BlockStatus>,
}

impl SafeToNotarParentTracker {
    pub fn new() -> Self {
        Self {
            block_status: BTreeMap::new(),
        }
    }

    pub fn add_block_parent_info(&mut self, block_parent_info: &BlockParentInfo) {
        self.block_status
            .entry(block_parent_info.my_block)
            .or_default()
            .parent_id = Some(block_parent_info.parent_block);
    }

    pub fn mark_block_notarized(&mut self, block: Block) {
        self.block_status.entry(block).or_default().is_notarized = true;
    }

    pub fn mark_safe_to_notar_stake_reached(
        &mut self,
        slot: Slot,
        new_safe_to_notar_staked_reached: Vec<Hash>,
    ) {
        for hash in new_safe_to_notar_staked_reached {
            let block = (slot, hash);
            self.block_status
                .entry(block)
                .or_default()
                .safe_to_notar_stake_reached = true;
        }
    }

    pub fn prune_old_state(&mut self, root_slot: Slot) {
        let split_off_key = (root_slot, Hash::default());
        self.block_status.split_off(&split_off_key);
    }

    pub fn get_new_safe_to_notar(&mut self) -> Vec<Block> {
        let candidates: Vec<(Option<Block>, Block)> = self
            .block_status
            .iter_mut()
            .filter_map(|(block, status)| {
                if status.event_sent || !status.safe_to_notar_stake_reached {
                    return None;
                }
                let slot = block.0;
                if first_of_consecutive_leader_slots(slot) == slot {
                    return Some((None, *block));
                }
                let parent_block = status.parent_id.as_ref()?;
                Some((Some(*parent_block), *block))
            })
            .collect();
        let new_safe_to_notar = candidates
            .into_iter()
            .filter_map(|(parent_block, block)| {
                match parent_block {
                    Some(parent_block) => {
                        if let Some(status) = self.block_status.get(&parent_block) {
                            if status.is_notarized {
                                return Some(block);
                            }
                        }
                        None
                    }
                    None => Some(block), // No parent means it's the first slot in the window
                }
            })
            .collect();
        for block in &new_safe_to_notar {
            if let Some(status) = self.block_status.get_mut(block) {
                status.event_sent = true;
            }
        }
        new_safe_to_notar
    }
}
