use solana_hash::Hash;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SliceRoot(pub Hash);

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct AlpenglowBlockId(pub Hash);
