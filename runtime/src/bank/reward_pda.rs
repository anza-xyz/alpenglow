use {
    super::Bank, solana_account::AccountSharedData, solana_clock::Epoch, solana_pubkey::Pubkey,
    solana_rent::Rent, solana_sdk_ids::system_program,
};

/// The state that is stored in the program derived account (PDA) for storing vote rewards.
#[derive(Deserialize, Serialize)]
struct RewardPdaState {
    /// History of vote rewards earned by the end of each epoch in lamports.
    /// Each tuple is (Epoch, rewards for current_epoch, rewards for previous_epoch).
    pub earned_vote_rewards: Vec<(Epoch, u128, u128)>,
}

impl RewardPdaState {
    /// Looks up the reward PDA address for the given voter.
    fn get_address(voter: &Pubkey) -> Pubkey {
        let (reward_account_addr, _) = Pubkey::find_program_address(&[b"vote_reward"], voter);
        reward_account_addr
    }

    /// Looks up the reward PDA account state for the given voter.
    //
    // TODO: handle errors.
    fn get_state(bank: &Bank, voter: &Pubkey) -> Self {
        let reward_account_addr = Self::get_address(voter);
        bank.get_account(&reward_account_addr)
            .map(|acct| acct.deserialize_data().unwrap())
            .unwrap()
    }

    /// Stores the reward PDA account state back into the bank.
    fn set_state(&self, voter: &Pubkey, bank: &Bank) {
        let size = bincode::serialized_size(self).unwrap();
        let lamports = Rent::default().minimum_balance(size as usize);
        let account = AccountSharedData::new_data(lamports, self, &system_program::ID).unwrap();

        let reward_account_addr = Self::get_address(voter);
        bank.store_account_and_update_capitalization(&reward_account_addr, &account);
    }

    /// This should be identical to VoteState::increment_credits().
    fn increment_reward(&mut self, _epoch: Epoch, _reward: u128) {
        unimplemented!()
    }
}

impl Bank {
    /// Pay per slot vote reward.
    pub(crate) fn pay_vote_reward(&self, voter: Pubkey, reward: u128) {
        let mut state = RewardPdaState::get_state(self, &voter);
        state.increment_reward(self.epoch(), reward);
        state.set_state(&voter, self);
    }
}
