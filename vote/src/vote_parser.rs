use {
    crate::vote_transaction::VoteTransaction,
    alpenglow_vote::{self, vote::Vote as AlpenglowVote},
    solana_bincode::limited_deserialize,
    solana_clock::Slot,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_svm_transaction::svm_transaction::SVMTransaction,
    solana_transaction::{
        versioned::{sanitized::SanitizedVersionedTransaction, VersionedTransaction},
        Transaction,
    },
    solana_vote_interface::instruction::VoteInstruction,
};

/// Represents a parsed vote transaction, which can be either a traditional Tower
/// vote or an Alpenglow vote. This enum allows unified handling of different vote types.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum ParsedVoteTransaction {
    Tower(VoteTransaction),
    Alpenglow(AlpenglowVote),
}

impl ParsedVoteTransaction {
    pub fn slots(&self) -> Vec<Slot> {
        match self {
            ParsedVoteTransaction::Tower(tx) => tx.slots(),
            ParsedVoteTransaction::Alpenglow(tx) => vec![tx.slot()],
        }
    }
    pub fn last_voted_slot(&self) -> Option<Slot> {
        match self {
            ParsedVoteTransaction::Tower(tx) => tx.last_voted_slot(),
            ParsedVoteTransaction::Alpenglow(tx) => Some(tx.slot()),
        }
    }

    pub fn last_voted_slot_hash(&self) -> Option<(Slot, Hash)> {
        match self {
            ParsedVoteTransaction::Tower(tx) => tx.last_voted_slot_hash(),
            ParsedVoteTransaction::Alpenglow(tx) => match tx {
                AlpenglowVote::Notarize(vote) => Some((vote.slot(), *vote.replayed_bank_hash())),
                AlpenglowVote::Finalize(_vote) => None,
                AlpenglowVote::NotarizeFallback(vote) => {
                    Some((vote.slot(), *vote.replayed_bank_hash()))
                }
                AlpenglowVote::Skip(_vote) => None,
                AlpenglowVote::SkipFallback(_vote) => None,
            },
        }
    }

    pub fn is_alpenglow_vote(&self) -> bool {
        matches!(self, ParsedVoteTransaction::Alpenglow(_))
    }

    pub fn is_full_tower_vote(&self) -> bool {
        match self {
            ParsedVoteTransaction::Tower(tx) => tx.is_full_tower_vote(),
            ParsedVoteTransaction::Alpenglow(_tx) => false,
        }
    }

    pub fn as_tower_transaction(self) -> Option<VoteTransaction> {
        match self {
            ParsedVoteTransaction::Tower(tx) => Some(tx),
            ParsedVoteTransaction::Alpenglow(_tx) => None,
        }
    }

    pub fn as_alpenglow_transaction(self) -> Option<AlpenglowVote> {
        match self {
            ParsedVoteTransaction::Tower(_tx) => None,
            ParsedVoteTransaction::Alpenglow(tx) => Some(tx),
        }
    }

    pub fn as_tower_transaction_ref(&self) -> Option<&VoteTransaction> {
        match self {
            ParsedVoteTransaction::Tower(tx) => Some(tx),
            ParsedVoteTransaction::Alpenglow(_tx) => None,
        }
    }

    pub fn as_alpenglow_transaction_ref(&self) -> Option<&AlpenglowVote> {
        match self {
            ParsedVoteTransaction::Tower(_tx) => None,
            ParsedVoteTransaction::Alpenglow(tx) => Some(tx),
        }
    }
}

impl From<VoteTransaction> for ParsedVoteTransaction {
    fn from(value: VoteTransaction) -> Self {
        ParsedVoteTransaction::Tower(value)
    }
}

impl From<AlpenglowVote> for ParsedVoteTransaction {
    fn from(value: alpenglow_vote::vote::Vote) -> Self {
        ParsedVoteTransaction::Alpenglow(value)
    }
}

pub type ParsedVote = (Pubkey, ParsedVoteTransaction, Option<Hash>, Signature);

// Used for locally forwarding processed vote transactions to consensus
pub fn parse_sanitized_vote_transaction(tx: &impl SVMTransaction) -> Option<ParsedVote> {
    // Check first instruction for a vote
    let (program_id, first_instruction) = tx.program_instructions_iter().next()?;
    if !solana_sdk_ids::vote::check_id(program_id) {
        return None;
    }
    let first_account = usize::from(*first_instruction.accounts.first()?);
    let key = tx.account_keys().get(first_account)?;
    let (vote, switch_proof_hash) = parse_vote_instruction_data(first_instruction.data)?;
    let signature = tx.signatures().first().cloned().unwrap_or_default();
    Some((
        *key,
        ParsedVoteTransaction::Tower(vote),
        switch_proof_hash,
        signature,
    ))
}

// Used for parsing gossip vote transactions
pub fn parse_vote_transaction(tx: &Transaction) -> Option<ParsedVote> {
    // Check first instruction for a vote
    let message = tx.message();
    let first_instruction = message.instructions.first()?;
    let program_id_index = usize::from(first_instruction.program_id_index);
    let program_id = message.account_keys.get(program_id_index)?;
    if !solana_sdk_ids::vote::check_id(program_id) {
        return None;
    }
    let first_account = usize::from(*first_instruction.accounts.first()?);
    let key = message.account_keys.get(first_account)?;
    let (vote, switch_proof_hash) = parse_vote_instruction_data(&first_instruction.data)?;
    let signature = tx.signatures.first().cloned().unwrap_or_default();
    Some((
        *key,
        ParsedVoteTransaction::Tower(vote),
        switch_proof_hash,
        signature,
    ))
}

// Used for locally forwarding processed vote transactions to consensus
pub fn parse_sanitized_alpenglow_vote_transaction(tx: &impl SVMTransaction) -> Option<ParsedVote> {
    // Check first instruction for a vote
    let (program_id, first_instruction) = tx.program_instructions_iter().next()?;
    if program_id != &alpenglow_vote::id() {
        return None;
    }
    let first_account = usize::from(*first_instruction.accounts.first()?);
    let key = tx.account_keys().get(first_account)?;
    let alpenglow_vote = parse_alpenglow_vote_instruction_data(first_instruction.data)?;
    let signature = tx.signatures().first().cloned().unwrap_or_default();
    Some((
        *key,
        ParsedVoteTransaction::Alpenglow(alpenglow_vote),
        None,
        signature,
    ))
}

pub fn is_alpenglow_vote_transaction(tx: &Transaction) -> bool {
    let message = tx.message();
    let Some(first_instruction) = message.instructions.first() else {
        return false;
    };
    let program_id_index = usize::from(first_instruction.program_id_index);
    let Some(program_id) = message.account_keys.get(program_id_index) else {
        return false;
    };
    program_id == &alpenglow_vote::id()
}

pub fn parse_alpenglow_vote_transaction_from_sanitized(
    tx: &SanitizedVersionedTransaction,
) -> Option<(AlpenglowVote, Pubkey, VersionedTransaction)> {
    // Check first instruction for a vote
    let message = tx.get_message();
    let first_instruction = message.instructions().first()?;
    for (program_id, _) in message.program_instructions_iter() {
        if program_id != &alpenglow_vote::id() {
            return None;
        }
    }
    let first_account = usize::from(*first_instruction.accounts.first()?);
    let key = message.message.static_account_keys().get(first_account)?;
    let alpenglow_vote = parse_alpenglow_vote_instruction_data(&first_instruction.data)?;
    let (signatures, message) = tx.clone().destruct();
    Some((
        alpenglow_vote,
        *key,
        VersionedTransaction {
            signatures,
            message: message.message,
        },
    ))
}

pub fn parse_alpenglow_vote_transaction(tx: &Transaction) -> Option<ParsedVote> {
    // Check first instruction for a vote
    let message = tx.message();
    let first_instruction = message.instructions.first()?;
    let program_id_index = usize::from(first_instruction.program_id_index);
    let program_id = message.account_keys.get(program_id_index)?;
    if program_id != &alpenglow_vote::id() {
        return None;
    }
    let first_account = usize::from(*first_instruction.accounts.first()?);
    let key = message.account_keys.get(first_account)?;
    let alpenglow_vote = parse_alpenglow_vote_instruction_data(&first_instruction.data)?;
    let signature = tx.signatures.first().cloned().unwrap_or_default();
    Some((
        *key,
        ParsedVoteTransaction::Alpenglow(alpenglow_vote),
        None,
        signature,
    ))
}

fn parse_vote_instruction_data(
    vote_instruction_data: &[u8],
) -> Option<(VoteTransaction, Option<Hash>)> {
    match limited_deserialize(
        vote_instruction_data,
        solana_packet::PACKET_DATA_SIZE as u64,
    )
    .ok()?
    {
        VoteInstruction::Vote(vote) => Some((VoteTransaction::from(vote), None)),
        VoteInstruction::VoteSwitch(vote, hash) => Some((VoteTransaction::from(vote), Some(hash))),
        VoteInstruction::UpdateVoteState(vote_state_update) => {
            Some((VoteTransaction::from(vote_state_update), None))
        }
        VoteInstruction::UpdateVoteStateSwitch(vote_state_update, hash) => {
            Some((VoteTransaction::from(vote_state_update), Some(hash)))
        }
        VoteInstruction::CompactUpdateVoteState(vote_state_update) => {
            Some((VoteTransaction::from(vote_state_update), None))
        }
        VoteInstruction::CompactUpdateVoteStateSwitch(vote_state_update, hash) => {
            Some((VoteTransaction::from(vote_state_update), Some(hash)))
        }
        VoteInstruction::TowerSync(tower_sync) => Some((VoteTransaction::from(tower_sync), None)),
        VoteInstruction::TowerSyncSwitch(tower_sync, hash) => {
            Some((VoteTransaction::from(tower_sync), Some(hash)))
        }
        VoteInstruction::Authorize(_, _)
        | VoteInstruction::AuthorizeChecked(_)
        | VoteInstruction::AuthorizeWithSeed(_)
        | VoteInstruction::AuthorizeCheckedWithSeed(_)
        | VoteInstruction::InitializeAccount(_)
        | VoteInstruction::UpdateCommission(_)
        | VoteInstruction::UpdateValidatorIdentity
        | VoteInstruction::Withdraw(_) => None,
    }
}

fn parse_alpenglow_vote_instruction_data(vote_instruction_data: &[u8]) -> Option<AlpenglowVote> {
    AlpenglowVote::deserialize_simple_vote(vote_instruction_data).ok()
}

#[cfg(test)]
mod test {
    use {
        super::*,
        solana_clock::Slot,
        solana_keypair::Keypair,
        solana_sha256_hasher::hash,
        solana_signer::Signer,
        solana_vote_interface::{instruction as vote_instruction, state::Vote},
    };

    // Reimplemented locally from Vote program.
    fn new_vote_transaction(
        slots: Vec<Slot>,
        bank_hash: Hash,
        blockhash: Hash,
        node_keypair: &Keypair,
        vote_keypair: &Keypair,
        authorized_voter_keypair: &Keypair,
        switch_proof_hash: Option<Hash>,
    ) -> Transaction {
        let votes = Vote::new(slots, bank_hash);
        let vote_ix = if let Some(switch_proof_hash) = switch_proof_hash {
            vote_instruction::vote_switch(
                &vote_keypair.pubkey(),
                &authorized_voter_keypair.pubkey(),
                votes,
                switch_proof_hash,
            )
        } else {
            vote_instruction::vote(
                &vote_keypair.pubkey(),
                &authorized_voter_keypair.pubkey(),
                votes,
            )
        };

        let mut vote_tx = Transaction::new_with_payer(&[vote_ix], Some(&node_keypair.pubkey()));

        vote_tx.partial_sign(&[node_keypair], blockhash);
        vote_tx.partial_sign(&[authorized_voter_keypair], blockhash);
        vote_tx
    }

    fn run_test_parse_vote_transaction(input_hash: Option<Hash>) {
        let node_keypair = Keypair::new();
        let vote_keypair = Keypair::new();
        let auth_voter_keypair = Keypair::new();
        let bank_hash = Hash::default();
        let vote_tx = new_vote_transaction(
            vec![42],
            bank_hash,
            Hash::default(),
            &node_keypair,
            &vote_keypair,
            &auth_voter_keypair,
            input_hash,
        );
        let (key, vote, hash, signature) = parse_vote_transaction(&vote_tx).unwrap();
        assert_eq!(hash, input_hash);
        assert_eq!(
            vote,
            ParsedVoteTransaction::Tower(VoteTransaction::from(Vote::new(vec![42], bank_hash)))
        );
        assert_eq!(key, vote_keypair.pubkey());
        assert_eq!(signature, vote_tx.signatures[0]);

        // Test bad program id fails
        let mut vote_ix = vote_instruction::vote(
            &vote_keypair.pubkey(),
            &auth_voter_keypair.pubkey(),
            Vote::new(vec![1, 2], Hash::default()),
        );
        vote_ix.program_id = Pubkey::default();
        let vote_tx = Transaction::new_with_payer(&[vote_ix], Some(&node_keypair.pubkey()));
        assert!(parse_vote_transaction(&vote_tx).is_none());
    }

    #[test]
    fn test_parse_vote_transaction() {
        run_test_parse_vote_transaction(None);
        run_test_parse_vote_transaction(Some(hash(&[42u8])));
    }

    fn new_alpenglow_vote_transaction(
        vote: AlpenglowVote,
        node_keypair: &Keypair,
        vote_keypair: &Keypair,
        authorized_voter_keypair: &Keypair,
    ) -> Transaction {
        let vote_ix =
            vote.to_vote_instruction(vote_keypair.pubkey(), authorized_voter_keypair.pubkey());

        let mut vote_tx = Transaction::new_with_payer(&[vote_ix], Some(&node_keypair.pubkey()));

        vote_tx.partial_sign(&[node_keypair], Hash::default());
        vote_tx.partial_sign(&[authorized_voter_keypair], Hash::default());
        vote_tx
    }

    fn run_test_parse_alpenglow_vote_transaction_from_sanitized(
        vote: AlpenglowVote,
        node_keypair: &Keypair,
        vote_keypair: &Keypair,
        authorized_voter_keypair: &Keypair,
    ) {
        let vote_tx = new_alpenglow_vote_transaction(
            vote,
            node_keypair,
            vote_keypair,
            authorized_voter_keypair,
        );
        let versioned_tx = VersionedTransaction::from(vote_tx);
        let sanitized_tx = SanitizedVersionedTransaction::try_from(versioned_tx.clone()).unwrap();
        let (parsed_vote, key, parsed_versioned_tx) =
            parse_alpenglow_vote_transaction_from_sanitized(&sanitized_tx).unwrap();
        assert_eq!(vote, parsed_vote);
        assert_eq!(vote_keypair.pubkey(), key);
        assert_eq!(parsed_versioned_tx, versioned_tx);
    }

    #[test]
    fn test_parse_alpenglow_vote_transaction_from_sanitized() {
        let node_keypair = Keypair::new();
        let vote_keypair = Keypair::new();
        let authorized_voter_keypair = Keypair::new();
        let bank_hash = Hash::new_unique();
        let block_id = Hash::new_unique();
        run_test_parse_alpenglow_vote_transaction_from_sanitized(
            AlpenglowVote::new_notarization_vote(42, block_id, bank_hash),
            &node_keypair,
            &vote_keypair,
            &authorized_voter_keypair,
        );

        run_test_parse_alpenglow_vote_transaction_from_sanitized(
            AlpenglowVote::new_notarization_vote(42, block_id, bank_hash),
            &node_keypair,
            &vote_keypair,
            &authorized_voter_keypair,
        );

        run_test_parse_alpenglow_vote_transaction_from_sanitized(
            AlpenglowVote::new_finalization_vote(43),
            &node_keypair,
            &vote_keypair,
            &authorized_voter_keypair,
        );

        run_test_parse_alpenglow_vote_transaction_from_sanitized(
            AlpenglowVote::new_skip_vote(35),
            &node_keypair,
            &vote_keypair,
            &authorized_voter_keypair,
        );

        // Test bad program_id fails
        let mut vote_ix = vote_instruction::vote(
            &vote_keypair.pubkey(),
            &authorized_voter_keypair.pubkey(),
            Vote::new(vec![1, 2], Hash::default()),
        );
        vote_ix.program_id = solana_sdk_ids::vote::id();
        let vote_tx = Transaction::new_with_payer(&[vote_ix], Some(&node_keypair.pubkey()));
        let versioned_tx = VersionedTransaction::from(vote_tx);
        let sanitized_tx = SanitizedVersionedTransaction::try_from(versioned_tx.clone()).unwrap();
        assert!(parse_alpenglow_vote_transaction_from_sanitized(&sanitized_tx).is_none());
    }

    fn run_test_parse_alpenglow_vote_transaction(
        vote: AlpenglowVote,
        node_keypair: &Keypair,
        vote_keypair: &Keypair,
        authorized_voter_keypair: &Keypair,
    ) {
        let vote_tx = new_alpenglow_vote_transaction(
            vote,
            node_keypair,
            vote_keypair,
            authorized_voter_keypair,
        );
        let (key, parsed_vote, hash, signature) =
            parse_alpenglow_vote_transaction(&vote_tx).unwrap();
        assert_eq!(ParsedVoteTransaction::Alpenglow(vote), parsed_vote);
        assert_eq!(vote_keypair.pubkey(), key);
        assert_eq!(hash, None);
        assert_eq!(vote_tx.signatures[0], signature);
    }

    #[test]
    fn test_parse_alpenglow_vote_transaction() {
        let node_keypair = Keypair::new();
        let vote_keypair = Keypair::new();
        let authorized_voter_keypair = Keypair::new();
        let bank_hash = Hash::new_unique();
        let block_id = Hash::new_unique();
        run_test_parse_alpenglow_vote_transaction(
            AlpenglowVote::new_notarization_vote(42, block_id, bank_hash),
            &node_keypair,
            &vote_keypair,
            &authorized_voter_keypair,
        );

        run_test_parse_alpenglow_vote_transaction(
            AlpenglowVote::new_notarization_vote(42, block_id, bank_hash),
            &node_keypair,
            &vote_keypair,
            &authorized_voter_keypair,
        );

        run_test_parse_alpenglow_vote_transaction(
            AlpenglowVote::new_finalization_vote(43),
            &node_keypair,
            &vote_keypair,
            &authorized_voter_keypair,
        );

        run_test_parse_alpenglow_vote_transaction(
            AlpenglowVote::new_skip_vote(35),
            &node_keypair,
            &vote_keypair,
            &authorized_voter_keypair,
        );

        // Test bad program id fails
        let mut vote_ix = vote_instruction::vote(
            &vote_keypair.pubkey(),
            &authorized_voter_keypair.pubkey(),
            Vote::new(vec![1, 2], Hash::default()),
        );
        vote_ix.program_id = solana_sdk_ids::vote::id();
        let vote_tx = Transaction::new_with_payer(&[vote_ix], Some(&node_keypair.pubkey()));
        assert!(parse_alpenglow_vote_transaction(&vote_tx).is_none());
    }
}
