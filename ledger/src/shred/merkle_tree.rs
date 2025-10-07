use {
    crate::shred::Error,
    solana_hash::Hash,
    //solana_runtime::bank::{AlpenglowBlockId,
    solana_runtime::bank::SliceRoot,
    solana_sha256_hasher::hashv,
    static_assertions::const_assert_eq,
};

pub(crate) const SIZE_OF_MERKLE_ROOT: usize = std::mem::size_of::<Hash>();
const_assert_eq!(SIZE_OF_MERKLE_ROOT, 32);
const_assert_eq!(SIZE_OF_MERKLE_PROOF_ENTRY, 20);
pub(crate) const SIZE_OF_MERKLE_PROOF_ENTRY: usize = std::mem::size_of::<MerkleProofEntry>();
// Number of proof entries for the standard 64 shred batch.
pub(crate) const PROOF_ENTRIES_FOR_32_32_BATCH: u8 = 6;

// Defense against second preimage attack:
// https://en.wikipedia.org/wiki/Merkle_tree#Second_preimage_attack
// Following Certificate Transparency, 0x00 and 0x01 bytes are prepended to
// hash data when computing leaf and internal node hashes respectively.
pub(crate) const MERKLE_HASH_PREFIX_LEAF: &[u8] = b"\x00SOLANA_MERKLE_SHREDS_LEAF";
pub(crate) const MERKLE_HASH_PREFIX_NODE: &[u8] = b"\x01SOLANA_MERKLE_SHREDS_NODE";

pub(crate) type MerkleProofEntry = [u8; 20];
pub(crate) struct SliceMerkleTree {
    pub hashes: Vec<Option<Hash>>,
    pub root: SliceRoot,
}

/*
pub(crate) struct DoubleMerkleTree {
    pub hashes: Vec<Option<Hash>>,
    pub parent_id: AlpenglowBlockId,
    pub block_id: AlpenglowBlockId,
}
*/

fn make_basic_merkle_tree<I>(items: I) -> Result<Vec<Option<Hash>>, Error>
where
    I: IntoIterator<Item = Result<Hash, Error>>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
{
    let items = items.into_iter();
    let num_items = items.len();
    let non_leaf_num = get_non_leaf_num(num_items);
    let mut nodes = vec![None; num_items + non_leaf_num];
    let mut leaf_index = non_leaf_num;
    for item in items {
        nodes[leaf_index] = Some(item?);
        leaf_index += 1;
    }
    for i in (1..=(non_leaf_num - 1)).rev() {
        if nodes[2 * i].is_none() && nodes[2 * i + 1].is_none() {
            nodes[i] = None;
        } else if nodes[2 * i + 1].is_none() {
            nodes[i] = Some(join_nodes(nodes[2 * i].unwrap(), nodes[2 * i].unwrap()));
        } else {
            nodes[i] = Some(join_nodes(nodes[2 * i].unwrap(), nodes[2 * i + 1].unwrap()));
        }
    }
    Ok(nodes)
}

// The tree for a slice (erasure set).
pub fn make_merkle_tree<I>(shreds: I) -> Result<SliceMerkleTree, Error>
where
    I: IntoIterator<Item = Result<Hash, Error>>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
{
    let nodes = make_basic_merkle_tree(shreds)?;
    let root = nodes[1].unwrap();
    Ok(SliceMerkleTree {
        hashes: nodes,
        root: SliceRoot(root),
    })
}

// The tree on top of slice trees for Alpenglow block id/repair.
/*
pub fn make_double_merkle_tree<I>(
    slice_roots: I,
    parent_id: AlpenglowBlockId,
) -> Result<DoubleMerkleTree, Error>
where
    I: IntoIterator<Item = Result<SliceRoot, Error>>,
    <I as IntoIterator>::IntoIter: ExactSizeIterator,
{
    let hashes: Vec<Result<Hash, Error>> = slice_roots
        .into_iter()
        .map(|x| x.map(|sr| sr.0))
        .chain(std::iter::once(Ok(parent_id.0)))
        .collect();
    let nodes = make_basic_merkle_tree(hashes)?;
    let root = nodes[1].unwrap();
    Ok(DoubleMerkleTree {
        hashes: nodes,
        parent_id,
        block_id: AlpenglowBlockId(root),
    })
}
*/

pub fn make_merkle_proof(
    mut index: usize, // leaf index
    mut size: usize,  // number of leaves in the tree
    tree: &[Option<Hash>],
) -> impl Iterator<Item = Result<&MerkleProofEntry, Error>> {
    let non_leaf_num = get_non_leaf_num(size);
    size += non_leaf_num;
    index += non_leaf_num;

    std::iter::from_fn(move || {
        if index == 1 {
            None
        } else if index >= size {
            Some(Err(Error::InvalidMerkleProof))
        } else {
            let node = match tree.get(index ^ 1) {
                Some(Some(hash)) => hash,
                _ => tree[index].as_ref().expect("node must have a hash"),
            };
            index >>= 1;
            let entry_bytes = &node.as_ref()[..SIZE_OF_MERKLE_PROOF_ENTRY];
            let entry = <&MerkleProofEntry>::try_from(entry_bytes).unwrap();
            Some(Ok(entry))
        }
    })
}

// Obtains parent's hash by joining two sibling nodes in merkle tree.
fn join_nodes<S: AsRef<[u8]>, T: AsRef<[u8]>>(node: S, other: T) -> Hash {
    let node = &node.as_ref()[..SIZE_OF_MERKLE_PROOF_ENTRY];
    let other = &other.as_ref()[..SIZE_OF_MERKLE_PROOF_ENTRY];
    hashv(&[MERKLE_HASH_PREFIX_NODE, node, other])
}

// Recovers root of the merkle tree from a leaf node
// at the given index and the respective proof.
pub fn get_merkle_root<'a, I>(index: usize, node: Hash, proof: I) -> Result<Hash, Error>
where
    I: IntoIterator<Item = &'a MerkleProofEntry>,
{
    let (index, root) = proof
        .into_iter()
        .fold((index, node), |(index, node), other| {
            let parent = if index % 2 == 0 {
                join_nodes(node, other)
            } else {
                join_nodes(other, node)
            };
            (index >> 1, parent)
        });
    (index == 0)
        .then_some(root)
        .ok_or(Error::InvalidMerkleProof)
}

// Given number of items, returns the number of non-leaf nodes in the Merkle tree.
pub fn get_non_leaf_num(leaf_num: usize) -> usize {
    let mut non_leaf_num = 1;
    while non_leaf_num < leaf_num {
        non_leaf_num *= 2;
    }
    non_leaf_num
}

// Maps number of (code + data) shreds to merkle_proof.len().
#[cfg(test)]
pub(crate) const fn get_proof_size(num_shreds: usize) -> u8 {
    let bits = usize::BITS - num_shreds.leading_zeros();
    let proof_size = if num_shreds.is_power_of_two() {
        bits.saturating_sub(1)
    } else {
        bits
    };
    // this can never overflow because bits < 64
    proof_size as u8
}

#[cfg(test)]
mod tests {
    use {
        crate::shred::merkle_tree::*, assert_matches::assert_matches, rand::Rng,
        std::iter::repeat_with,
    };

    #[test]
    fn test_merkle_proof_entry_from_hash() {
        let mut rng = rand::thread_rng();
        let bytes: [u8; 32] = rng.gen();
        let hash = Hash::from(bytes);
        let entry = &hash.as_ref()[..SIZE_OF_MERKLE_PROOF_ENTRY];
        let entry = MerkleProofEntry::try_from(entry).unwrap();
        assert_eq!(entry, &bytes[..SIZE_OF_MERKLE_PROOF_ENTRY]);
    }

    #[test]
    fn test_make_merkle_proof_error() {
        let mut rng = rand::thread_rng();
        let nodes = repeat_with(|| rng.gen::<[u8; 32]>()).map(Hash::from);
        let nodes: Vec<_> = nodes.take(5).collect();
        let size = nodes.len();
        let tree = make_merkle_tree(nodes.into_iter().map(Ok)).unwrap();
        for index in size..size + 3 {
            assert_matches!(
                make_merkle_proof(index, size, &tree.hashes).next(),
                Some(Err(Error::InvalidMerkleProof))
            );
        }
    }

    fn run_merkle_tree_round_trip<R: Rng>(rng: &mut R, size: usize) {
        let nodes = repeat_with(|| rng.gen::<[u8; 32]>()).map(Hash::from);
        let nodes: Vec<_> = nodes.take(size).collect();
        let tree = make_merkle_tree(nodes.iter().cloned().map(Ok)).unwrap();
        let root = tree.hashes.last().unwrap().unwrap();
        for index in 0..size {
            for (k, &node) in nodes.iter().enumerate() {
                let proof = make_merkle_proof(index, size, &tree.hashes).map(Result::unwrap);
                if k == index {
                    assert_eq!(root, get_merkle_root(k, node, proof).unwrap());
                } else {
                    assert_ne!(root, get_merkle_root(k, node, proof).unwrap());
                }
            }
        }
    }

    #[test]
    fn test_merkle_tree_round_trip_small() {
        let mut rng = rand::thread_rng();
        for size in 1..=110 {
            run_merkle_tree_round_trip(&mut rng, size);
        }
    }

    #[test]
    fn test_merkle_tree_round_trip_big() {
        let mut rng = rand::thread_rng();
        for size in 110..=143 {
            run_merkle_tree_round_trip(&mut rng, size);
        }
    }

    #[test]
    fn test_get_proof_size() {
        assert_eq!(get_proof_size(0), 0);
        assert_eq!(get_proof_size(1), 0);
        assert_eq!(get_proof_size(2), 1);
        assert_eq!(get_proof_size(3), 2);
        assert_eq!(get_proof_size(4), 2);
        assert_eq!(get_proof_size(5), 3);
        assert_eq!(get_proof_size(63), 6);
        assert_eq!(get_proof_size(64), 6);
        assert_eq!(get_proof_size(65), 7);
        assert_eq!(get_proof_size(usize::MAX - 1), 64);
        assert_eq!(get_proof_size(usize::MAX), 64);
        for proof_size in 1u8..9 {
            let max_num_shreds = 1usize << u32::from(proof_size);
            let min_num_shreds = (max_num_shreds >> 1) + 1;
            for num_shreds in min_num_shreds..=max_num_shreds {
                assert_eq!(get_proof_size(num_shreds), proof_size);
            }
        }
    }
}
