/// Path to the alpenglow-vote shared object
pub const ALPENGLOW_VOTE_SO_PATH: &str = env!("ALPENGLOW_VOTE_SO_PATH");

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::ALPENGLOW_VOTE_SO_PATH;

    #[test]
    pub fn ensure_alpenglow_vote_so_path_exists() {
        assert!(Path::new(ALPENGLOW_VOTE_SO_PATH).exists());
    }
}
