use {
    serde::{Deserialize, Serialize},
    solana_hash::Hash,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceRoot(pub Hash);

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlpenglowBlockId(pub Hash);
