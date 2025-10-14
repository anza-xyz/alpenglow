use {
    crate::consensus_pool::vote_certificate_builder::VoteCertificateBuilder,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::leader_schedule_cache::LeaderScheduleCache,
    solana_votor_messages::consensus_message::{CertificateMessage, VoteMessage},
};

/// Different variants of a rewards entry.
enum EntryType {
    /// It can be none if we have not seen any messages or the certificate builder for it.
    None,
    /// We have received a certificate builder and may have also aggregated some messages into it.
    Builder(VoteCertificateBuilder),
    /// We have received some messages but not the builder yet.
    Messages(Vec<VoteMessage>),
}

impl EntryType {
    /// Adds a builder to an existing entry.
    ///
    /// Any existing messages are aggregated into the builder.
    ///
    /// # Panics
    /// Panics if the entry already contains a builder.
    fn add_builder(&mut self, mut builder: VoteCertificateBuilder) {
        match self {
            Self::None => *self = Self::Builder(builder),
            Self::Builder(_) => unreachable!(),
            Self::Messages(msgs) => {
                // XXX: handle error
                builder.aggregate(msgs).unwrap();
                *self = Self::Builder(builder);
            }
        }
    }

    /// Adds a message to an existing entry.
    ///
    /// If the entry already contains a builder, then the message is aggregated into it.
    /// Otherwise, the message is queued up waiting for a builder.
    fn add_msg(&mut self, msg: VoteMessage) {
        match self {
            Self::None => *self = Self::Messages(vec![msg]),
            Self::Builder(ref mut builder) => {
                // XXX: handle error
                builder.aggregate(&[msg]).unwrap();
            }
            Self::Messages(msgs) => msgs.push(msg),
        }
    }

    fn generate_cert(self) -> Option<CertificateMessage> {
        match self {
            Self::None => None,
            // XXX: handle error
            Self::Builder(builder) => Some(builder.build_for_rewards().unwrap()),
            Self::Messages(_msgs) => {
                // XXX: only saw msgs but never saw a builder.  We can probably construct a builder and aggregate the msgs into it.
                unimplemented!()
            }
        }
    }
}

/// Storage for notar and skip votes and certificate builders for rewards purposes.
pub(super) struct RewardsEntry {
    notar: EntryType,
    skip: EntryType,
}

impl RewardsEntry {
    /// Constructs a storage when a notar builder was seen.
    pub(super) fn with_notar_builder(builder: VoteCertificateBuilder) -> Self {
        Self {
            notar: EntryType::Builder(builder),
            skip: EntryType::None,
        }
    }

    /// Constructs a storage when a skip builder was seen.
    pub(super) fn with_skip_builder(builder: VoteCertificateBuilder) -> Self {
        Self {
            notar: EntryType::None,
            skip: EntryType::Builder(builder),
        }
    }

    /// Adds a notar builder to an existing storage.
    ///
    /// Panics if the storage already contains a notar builder.
    pub(super) fn add_notar_builder(&mut self, builder: VoteCertificateBuilder) {
        self.notar.add_builder(builder);
    }

    /// Adds a skip builder to an existing storage.
    ///
    /// Panics if the storage already contains a skip builder.
    pub(super) fn add_skip_builder(&mut self, builder: VoteCertificateBuilder) {
        self.skip.add_builder(builder);
    }

    /// Adds a notar message to the storage.
    pub(super) fn add_notar_msg(&mut self, msg: VoteMessage) {
        self.notar.add_msg(msg);
    }

    /// Adds a skip message to the storage.
    pub(super) fn add_skip_msg(&mut self, msg: VoteMessage) {
        self.skip.add_msg(msg);
    }

    /// Genreate certificates consuming the storage.
    pub(super) fn generate_certs(self) -> Vec<CertificateMessage> {
        let mut ret = vec![];
        if let Some(c) = self.notar.generate_cert() {
            ret.push(c);
        }
        if let Some(c) = self.skip.generate_cert() {
            ret.push(c);
        }
        ret
    }
}

impl Default for RewardsEntry {
    fn default() -> Self {
        Self {
            notar: EntryType::None,
            skip: EntryType::None,
        }
    }
}
