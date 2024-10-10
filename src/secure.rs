use crate::PeerId;
use serde::{Deserialize, Serialize};

pub trait HandshakeProtocol: Send + Sync + 'static {
    fn create_request(&self, from: PeerId, to: PeerId, now: u64) -> Vec<u8>;
    fn verify_request(&self, data: Vec<u8>, expected_from: PeerId, expected_to: PeerId, now: u64) -> Result<(), String>;
    fn create_response(&self, from: PeerId, to: PeerId, now: u64) -> Vec<u8>;
    fn verify_response(&self, data: Vec<u8>, expected_from: PeerId, expected_to: PeerId, now: u64) -> Result<(), String>;
}

const HASH_SEED: &str = "atm0s-small-p2p";
const HANDSHAKE_TIMEOUT: u64 = 30_000;

#[derive(Debug, Serialize, Deserialize)]
struct HandshakeMessage {
    payload: Vec<u8>,
    signature: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct HandshakeData {
    from: PeerId,
    to: PeerId,
    timestamp: u64,
    is_initiator: bool,
}

/// Simple secure_key protect with hash
/// Idea is we serialize HandshakeData to bytes with bincode then concat it with secure_key and a seed
/// Then compare received hash for ensuring two nodes have same secure_key
/// at_ts timestamp is used for avoiding relay attach, if it older than HANDSHAKE_TIMEOUT then we reject
pub struct SharedKeyHandshake {
    secure_key: String,
}

impl From<&str> for SharedKeyHandshake {
    fn from(value: &str) -> Self {
        Self { secure_key: value.to_owned() }
    }
}

impl SharedKeyHandshake {
    fn generate_handshake(&self, from: PeerId, to: PeerId, is_client: bool, now: u64) -> Vec<u8> {
        let handshake_data = HandshakeData {
            from,
            to,
            timestamp: now,
            is_initiator: is_client,
        };

        let data = bincode::serialize(&handshake_data).unwrap();
        let mut hash_input = data.clone();
        hash_input.extend_from_slice(self.secure_key.as_bytes());
        hash_input.extend_from_slice(HASH_SEED.as_bytes());

        let hash = blake3::hash(&hash_input).as_bytes().to_vec();

        let handshake = HandshakeMessage { payload: data, signature: hash };
        bincode::serialize(&handshake).unwrap()
    }

    fn validate_handshake(&self, data: Vec<u8>, expected_from: PeerId, expected_to: PeerId, expected_is_client: bool, current_ts: u64) -> Result<(), String> {
        let handshake: HandshakeMessage = bincode::deserialize(&data).map_err(|_| "Invalid handshake format".to_string())?;

        let handshake_data: HandshakeData = bincode::deserialize(&handshake.payload).map_err(|_| "Invalid handshake data format".to_string())?;

        // Verify timestamp
        if current_ts > handshake_data.timestamp + HANDSHAKE_TIMEOUT {
            return Err(format!("Handshake timeout {} vs {}", current_ts, handshake_data.timestamp));
        }

        // Verify peer IDs
        if handshake_data.from != expected_from || handshake_data.to != expected_to {
            return Err("Invalid peer IDs".to_string());
        }

        // Verify client/server role
        if handshake_data.is_initiator != expected_is_client {
            return Err("Invalid client/server role".to_string());
        }

        // Verify hash
        let mut hash_input = handshake.payload;
        hash_input.extend_from_slice(self.secure_key.as_bytes());
        hash_input.extend_from_slice(HASH_SEED.as_bytes());
        let expected_hash = blake3::hash(&hash_input).as_bytes().to_vec();

        if handshake.signature != expected_hash {
            return Err("Invalid handshake hash".to_string());
        }

        Ok(())
    }
}

impl HandshakeProtocol for SharedKeyHandshake {
    fn create_request(&self, from: PeerId, to: PeerId, now: u64) -> Vec<u8> {
        self.generate_handshake(from, to, true, now)
    }

    fn verify_request(&self, data: Vec<u8>, expected_from: PeerId, expected_to: PeerId, now: u64) -> Result<(), String> {
        self.validate_handshake(data, expected_from, expected_to, true, now)
    }

    fn create_response(&self, from: PeerId, to: PeerId, now: u64) -> Vec<u8> {
        self.generate_handshake(from, to, false, now)
    }

    fn verify_response(&self, data: Vec<u8>, expected_from: PeerId, expected_to: PeerId, now: u64) -> Result<(), String> {
        self.validate_handshake(data, expected_from, expected_to, false, now)
    }
}

#[cfg(test)]
mod tests {
    use crate::now_ms;

    use super::*;

    #[test]
    fn test_handshake_flow() {
        let secure = SharedKeyHandshake::from("test_key");
        let peer1 = PeerId::from(1);
        let peer2 = PeerId::from(2);

        // Test request handshake
        let request = secure.create_request(peer1, peer2, now_ms());
        assert!(secure.verify_request(request, peer1, peer2, now_ms()).is_ok());

        // Test response handshake
        let response = secure.create_response(peer2, peer1, now_ms());
        assert!(secure.verify_response(response, peer2, peer1, now_ms()).is_ok());
    }

    #[test]
    fn test_invalid_handshake() {
        let secure1 = SharedKeyHandshake::from("key1");
        let secure2 = SharedKeyHandshake::from("key2");
        let peer1 = PeerId::from(1);
        let peer2 = PeerId::from(2);

        let request = secure1.create_request(peer1, peer2, now_ms());
        assert!(secure2.verify_request(request, peer1, peer2, now_ms()).is_err());
    }

    #[test]
    fn test_handshake_timeout() {
        let secure = SharedKeyHandshake::from("test_key");
        let peer1 = PeerId::from(1);
        let peer2 = PeerId::from(2);

        // when date of peer2 is faster than peer1
        let request = secure.create_request(peer2, peer1, 1000);
        assert!(secure.verify_request(request, peer2, peer1, 980).is_ok());

        // when peer2 is too slow
        let request = secure.create_request(peer2, peer1, 1000);
        assert!(secure.verify_request(request, peer2, peer1, 1000 + HANDSHAKE_TIMEOUT + 1).is_err());
    }
}
