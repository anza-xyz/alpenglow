// This is a temporay work around to correctly report last votes of peers in Alpenglow
// before we have the certs in banks.
use {
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{collections::HashMap, sync::RwLock, time::Instant},
};

#[derive(Default)]
pub struct AlpenglowLastVoted {
    last_voted_map: RwLock<HashMap<Pubkey, (Slot, Instant)>>,
}

impl AlpenglowLastVoted {
    pub fn update_last_voted(&self, verified_votes_by_pubkey: &HashMap<Pubkey, Vec<Slot>>) {
        let now = Instant::now();
        let mut map = self.last_voted_map.write().unwrap();
        for (pubkey, slots) in verified_votes_by_pubkey {
            let largest_slot = slots.iter().max().unwrap();
            map.entry(*pubkey)
                .and_modify(|entry| *entry = (entry.0.max(*largest_slot), now))
                .or_insert((*largest_slot, now));
        }
        map.retain(|_, (_, time)| now.duration_since(*time).as_secs() <= 3600);
    }

    pub fn get_last_voted(&self, pubkey: &Pubkey) -> Option<Slot> {
        self.last_voted_map
            .read()
            .unwrap()
            .get(pubkey)
            .map(|(slot, _)| *slot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpenglow_last_voted() {
        let alpenglow_last_voted = AlpenglowLastVoted::default();
        let pubkey1 = Pubkey::new_unique();
        let pubkey2 = Pubkey::new_unique();
        alpenglow_last_voted
            .update_last_voted(&HashMap::from([(pubkey1, vec![1]), (pubkey2, vec![2])]));
        assert_eq!(alpenglow_last_voted.get_last_voted(&pubkey1), Some(1));
        assert_eq!(alpenglow_last_voted.get_last_voted(&pubkey2), Some(2));
        assert_eq!(
            alpenglow_last_voted.get_last_voted(&Pubkey::new_unique()),
            None
        );
    }
}
