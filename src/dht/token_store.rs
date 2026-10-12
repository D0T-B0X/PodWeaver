use std::sync::Arc;
use std::sync::Mutex;
use std::net::IpAddr;
use std::time::Instant;
use std::time::Duration;

use sha2::Digest;
use sha2::Sha256;

const ROTATION_INTERVAL: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Copy)]
pub struct TokenStore {
    pub curr_secret: [u8; 32],
    pub prev_secret: [u8; 32],
    pub last_rotation: Instant,
}

impl TokenStore {
    pub fn new() -> Arc<Mutex<Self>> {
        let sec_a = rand::random::<[u8; 32]>();
        let sec_b = rand::random::<[u8; 32]>();

        Arc::new(Mutex::new(Self { 
            curr_secret: sec_a, 
            prev_secret: sec_b, 
            last_rotation: Instant::now() 
        }))
    }

    pub fn validate_token(&mut self, token: &[u8], ip: IpAddr) -> bool {
        self.verify_rotation();

        let curr_token = Self::compute_hash(&self.curr_secret, ip);
        let prev_token = Self::compute_hash(&self.prev_secret, ip);

        token == curr_token || token == prev_token
    }

    pub fn new_token(&mut self, ip: IpAddr) -> Vec<u8> {
        self.verify_rotation();
        Self::compute_hash(&self.curr_secret, ip)
    }

    fn compute_hash(secret: &[u8; 32], ip: IpAddr) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(secret);

        match ip {
            IpAddr::V4(v4) => hasher.update(v4.octets()),
            IpAddr::V6(v6) => hasher.update(v6.octets()),
        }

        hasher.finalize()[..8].to_vec()
    }

    fn verify_rotation(&mut self) {
        if self.last_rotation.elapsed() >= ROTATION_INTERVAL {
            self.prev_secret = self.curr_secret;
            self.curr_secret = rand::random::<[u8; 32]>();
            self.last_rotation = Instant::now();
        }
    }
}