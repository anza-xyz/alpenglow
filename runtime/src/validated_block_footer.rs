use {
    crate::{bank::Bank, block_component_processor::BlockComponentProcessor},
    solana_entry::block_component::BlockFooterV1,
    solana_hash::Hash,
    thiserror::Error,
};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValidatedBlockFooterError {
    #[error("clock out of bounds")]
    ClockOutOfBounds,
    #[error("clock overflow")]
    ClockOverflow,
    #[error("bank hash mismatch")]
    BankHashMismatch,
}

pub struct ValidatedBlockFooter {
    bank_hash: Hash,
    block_producer_time_ns: i64,
    block_user_agent: Vec<u8>,
}

impl ValidatedBlockFooter {
    /// Creates a new [`ValidatedBlockFooter`] for the block the block producer.
    ///
    /// Warning!  No validation checks are performed, this should not be called during replay but only during block production.
    pub fn new_unchecked_for_block_producer(footer: BlockFooterV1) -> Self {
        let BlockFooterV1 {
            bank_hash,
            block_producer_time_nanos,
            block_user_agent,
        } = footer;
        Self {
            bank_hash,
            block_producer_time_ns: block_producer_time_nanos.try_into().unwrap(),
            block_user_agent,
        }
    }

    /// Creates a [`ValidatedBlockFooter`] and fails if the footer is found to be invalid.
    pub fn try_new(
        parent_bank: &Bank,
        bank: &Bank,
        footer: BlockFooterV1,
    ) -> Result<Self, ValidatedBlockFooterError> {
        let BlockFooterV1 {
            bank_hash,
            block_producer_time_nanos,
            block_user_agent,
        } = footer;
        let block_producer_time_ns = validate_clock(parent_bank, bank, block_producer_time_nanos)?;
        if bank_hash != bank.hash() {
            return Err(ValidatedBlockFooterError::BankHashMismatch);
        }
        Ok(Self {
            bank_hash,
            block_producer_time_ns,
            block_user_agent,
        })
    }

    pub fn block_producer_time_ns(&self) -> i64 {
        self.block_producer_time_ns
    }

    pub fn bank_hash(&self) -> Hash {
        self.bank_hash
    }

    pub fn block_user_agent(&self) -> &[u8] {
        &self.block_user_agent
    }
}

impl From<ValidatedBlockFooter> for BlockFooterV1 {
    fn from(footer: ValidatedBlockFooter) -> Self {
        let ValidatedBlockFooter {
            bank_hash,
            block_producer_time_ns,
            block_user_agent,
        } = footer;
        let block_producer_time_nanos = block_producer_time_ns
            .try_into()
            .expect("i64 to u64 should not overflow");
        BlockFooterV1 {
            bank_hash,
            block_producer_time_nanos,
            block_user_agent,
        }
    }
}

fn validate_clock(
    parent_bank: &Bank,
    bank: &Bank,
    block_producer_time_ns: u64,
) -> Result<i64, ValidatedBlockFooterError> {
    let block_producer_time_ns = block_producer_time_ns
        .try_into()
        .map_err(|_| ValidatedBlockFooterError::ClockOverflow)?;

    // Get parent time from nanosecond clock account
    // If nanosecond clock hasn't been populated, don't enforce the bounds; note that the
    // nanosecond clock is populated as soon as Alpenglow migration is complete.
    let Some(parent_time_nanos) = parent_bank.get_nanosecond_clock() else {
        return Ok(block_producer_time_ns);
    };

    let parent_slot = parent_bank.slot();
    let current_slot = bank.slot();

    let (lower_bound_ns, upper_bound_ns) = BlockComponentProcessor::nanosecond_time_bounds(
        parent_slot,
        parent_time_nanos,
        current_slot,
    );

    let is_valid =
        lower_bound_ns <= block_producer_time_ns && block_producer_time_ns <= upper_bound_ns;

    match is_valid {
        true => Ok(block_producer_time_ns),
        false => Err(ValidatedBlockFooterError::ClockOutOfBounds),
    }
}
