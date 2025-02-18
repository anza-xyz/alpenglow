use {
    super::vote_history::*,
    solana_sdk::{
        pubkey::Pubkey,
        signature::{Signature, Signer},
    },
    std::{
        fs::{self, File},
        io::{self, BufReader},
        path::PathBuf,
    },
};

pub type Result<T> = std::result::Result<T, VoteHistoryError>;

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum SavedVoteHistoryVersions {
    Current(SavedVoteHistory),
}

impl SavedVoteHistoryVersions {
    fn try_into_vote_history(&self, node_pubkey: &Pubkey) -> Result<VoteHistory> {
        // This method assumes that `self` was just deserialized
        assert_eq!(self.pubkey(), Pubkey::default());

        let vote_history = match self {
            SavedVoteHistoryVersions::Current(t) => {
                if !t.signature.verify(node_pubkey.as_ref(), &t.data) {
                    return Err(VoteHistoryError::InvalidSignature);
                }
                bincode::deserialize(&t.data).map(VoteHistoryVersions::Current)
            }
        };
        vote_history
            .map_err(|e| e.into())
            .and_then(|vote_history: VoteHistoryVersions| {
                let vote_history = vote_history.convert_to_current();
                if vote_history.node_pubkey != *node_pubkey {
                    return Err(VoteHistoryError::WrongVoteHistory(format!(
                        "node_pubkey is {:?} but found vote history for {:?}",
                        node_pubkey, vote_history.node_pubkey
                    )));
                }
                Ok(vote_history)
            })
    }

    fn serialize_into(&self, file: &mut File) -> Result<()> {
        bincode::serialize_into(file, self).map_err(|e| e.into())
    }

    fn pubkey(&self) -> Pubkey {
        match self {
            SavedVoteHistoryVersions::Current(t) => t.node_pubkey,
        }
    }
}

impl From<SavedVoteHistory> for SavedVoteHistoryVersions {
    fn from(vote_history: SavedVoteHistory) -> SavedVoteHistoryVersions {
        SavedVoteHistoryVersions::Current(vote_history)
    }
}

#[cfg_attr(
    feature = "frozen-abi",
    derive(AbiExample),
    frozen_abi(digest = "Po74P8NvXw5FkTnF6XqC9CenLUpDLYwQjyLgH51qTzP")
)]
#[derive(Default, Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct SavedVoteHistory {
    signature: Signature,
    data: Vec<u8>,
    #[serde(skip)]
    node_pubkey: Pubkey,
}

impl SavedVoteHistory {
    pub fn new<T: Signer>(vote_history: &VoteHistory, keypair: &T) -> Result<Self> {
        let node_pubkey = keypair.pubkey();
        if vote_history.node_pubkey != node_pubkey {
            return Err(VoteHistoryError::WrongVoteHistory(format!(
                "node_pubkey is {:?} but found vote history for {:?}",
                node_pubkey, vote_history.node_pubkey
            )));
        }

        let data = bincode::serialize(&vote_history)?;
        let signature = keypair.sign_message(&data);
        Ok(Self {
            signature,
            data,
            node_pubkey,
        })
    }
}

pub trait VoteHistoryStorage: Sync + Send {
    fn load(&self, node_pubkey: &Pubkey) -> Result<VoteHistory>;
    fn store(&self, saved_vote_history: &SavedVoteHistoryVersions) -> Result<()>;
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct NullVoteHistoryStorage {}

impl VoteHistoryStorage for NullVoteHistoryStorage {
    fn load(&self, _node_pubkey: &Pubkey) -> Result<VoteHistory> {
        Err(VoteHistoryError::IoError(io::Error::new(
            io::ErrorKind::Other,
            "NullVoteHistoryStorage::load() not available",
        )))
    }

    fn store(&self, _saved_vote_history: &SavedVoteHistoryVersions) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FileVoteHistoryStorage {
    pub vote_history_path: PathBuf,
}

impl FileVoteHistoryStorage {
    pub fn new(vote_history_path: PathBuf) -> Self {
        Self { vote_history_path }
    }

    pub fn filename(&self, node_pubkey: &Pubkey) -> PathBuf {
        self.vote_history_path
            .join(format!("vote_history-{node_pubkey}"))
            .with_extension("bin")
    }
}

impl VoteHistoryStorage for FileVoteHistoryStorage {
    fn load(&self, node_pubkey: &Pubkey) -> Result<VoteHistory> {
        let filename = self.filename(node_pubkey);
        trace!("load {}", filename.display());

        // Ensure to create parent dir here, because restore() precedes save() always
        fs::create_dir_all(filename.parent().unwrap())?;

        // New format
        let file = File::open(&filename)?;
        let mut stream = BufReader::new(file);

        bincode::deserialize_from(&mut stream)
            .map_err(|e| e.into())
            .and_then(|t: SavedVoteHistoryVersions| t.try_into_vote_history(node_pubkey))
    }

    fn store(&self, saved_vote_history: &SavedVoteHistoryVersions) -> Result<()> {
        let pubkey = saved_vote_history.pubkey();
        let filename = self.filename(&pubkey);
        trace!("store: {}", filename.display());
        let new_filename = filename.with_extension("bin.new");

        {
            // overwrite anything if exists
            let mut file = File::create(&new_filename)?;
            saved_vote_history.serialize_into(&mut file)?;
            // file.sync_all() hurts performance; pipeline sync-ing and submitting votes to the cluster!
        }
        fs::rename(&new_filename, &filename)?;
        // self.path.parent().sync_all() hurts performance same as the above sync
        Ok(())
    }
}

pub struct EtcdVoteHistoryStorage {
    client: tokio::sync::Mutex<etcd_client::Client>,
    instance_id: [u8; 8],
    runtime: tokio::runtime::Runtime,
}

pub struct EtcdTlsConfig {
    pub domain_name: String,
    pub ca_certificate: Vec<u8>,
    pub identity_certificate: Vec<u8>,
    pub identity_private_key: Vec<u8>,
}

impl EtcdVoteHistoryStorage {
    pub fn new<E: AsRef<str>, S: AsRef<[E]>>(
        endpoints: S,
        tls_config: Option<EtcdTlsConfig>,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();

        let client = runtime
            .block_on(etcd_client::Client::connect(
                endpoints,
                tls_config.map(|tls_config| {
                    etcd_client::ConnectOptions::default().with_tls(
                        etcd_client::TlsOptions::new()
                            .domain_name(tls_config.domain_name)
                            .ca_certificate(etcd_client::Certificate::from_pem(
                                tls_config.ca_certificate,
                            ))
                            .identity(etcd_client::Identity::from_pem(
                                tls_config.identity_certificate,
                                tls_config.identity_private_key,
                            )),
                    )
                }),
            ))
            .map_err(Self::etdc_to_vote_history_error)?;

        Ok(Self {
            client: tokio::sync::Mutex::new(client),
            instance_id: solana_sdk::timing::timestamp().to_le_bytes(),
            runtime,
        })
    }

    fn get_keys(node_pubkey: &Pubkey) -> (String, String) {
        let instance_key = format!("{node_pubkey}/instance");
        let vote_history_key = format!("{node_pubkey}/vote history");
        (instance_key, vote_history_key)
    }

    fn etdc_to_vote_history_error(error: etcd_client::Error) -> VoteHistoryError {
        VoteHistoryError::IoError(io::Error::new(io::ErrorKind::Other, error.to_string()))
    }
}

impl VoteHistoryStorage for EtcdVoteHistoryStorage {
    fn load(&self, node_pubkey: &Pubkey) -> Result<VoteHistory> {
        let (instance_key, vote_history_key) = Self::get_keys(node_pubkey);

        let txn = etcd_client::Txn::new().and_then(vec![etcd_client::TxnOp::put(
            instance_key.clone(),
            self.instance_id,
            None,
        )]);
        self.runtime
            .block_on(async { self.client.lock().await.txn(txn).await })
            .map_err(|err| {
                error!("Failed to acquire etcd instance lock: {}", err);
                Self::etdc_to_vote_history_error(err)
            })?;

        let txn = etcd_client::Txn::new()
            .when(vec![etcd_client::Compare::value(
                instance_key,
                etcd_client::CompareOp::Equal,
                self.instance_id,
            )])
            .and_then(vec![etcd_client::TxnOp::get(vote_history_key, None)]);

        let response = self
            .runtime
            .block_on(async { self.client.lock().await.txn(txn).await })
            .map_err(|err| {
                error!("Failed to read etcd saved vote history: {}", err);
                Self::etdc_to_vote_history_error(err)
            })?;

        if !response.succeeded() {
            return Err(VoteHistoryError::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!("Lost etcd instance lock for {node_pubkey}"),
            )));
        }

        for op_response in response.op_responses() {
            if let etcd_client::TxnOpResponse::Get(get_response) = op_response {
                if let Some(kv) = get_response.kvs().first() {
                    return bincode::deserialize_from(kv.value())
                        .map_err(|e| e.into())
                        .and_then(|t: SavedVoteHistoryVersions| {
                            t.try_into_vote_history(node_pubkey)
                        });
                }
            }
        }

        // Should never happen...
        Err(VoteHistoryError::IoError(io::Error::new(
            io::ErrorKind::Other,
            "Saved vote history response missing".to_string(),
        )))
    }

    fn store(&self, saved_vote_history: &SavedVoteHistoryVersions) -> Result<()> {
        let (instance_key, vote_history_key) = Self::get_keys(&saved_vote_history.pubkey());

        let txn = etcd_client::Txn::new()
            .when(vec![etcd_client::Compare::value(
                instance_key,
                etcd_client::CompareOp::Equal,
                self.instance_id,
            )])
            .and_then(vec![etcd_client::TxnOp::put(
                vote_history_key,
                bincode::serialize(&saved_vote_history)?,
                None,
            )]);

        let response = self
            .runtime
            .block_on(async { self.client.lock().await.txn(txn).await })
            .map_err(|err| {
                error!("Failed to write etcd saved vote history: {}", err);
                err
            })
            .map_err(Self::etdc_to_vote_history_error)?;

        if !response.succeeded() {
            return Err(VoteHistoryError::IoError(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Lost etcd instance lock for {}",
                    saved_vote_history.pubkey()
                ),
            )));
        }
        Ok(())
    }
}
