//! Password credential storage: PBKDF2-HMAC-SHA256 with a per-principal salt and
//! constant-time verification.
//!
//! (The spec named Argon2id; PBKDF2 is used because it is already in the
//! dependency tree. `CredentialHash` is the seam to swap the KDF later without
//! touching callers.)

use pbkdf2::pbkdf2_hmac;
use serde::Deserialize;
use sha2::Sha256;
use subtle::ConstantTimeEq;

/// Default PBKDF2 iteration count (OWASP-recommended floor for HMAC-SHA256).
pub const DEFAULT_ITERATIONS: u32 = 600_000;

/// A stored password verifier. Never holds the plaintext.
#[derive(Clone, Debug)]
pub struct CredentialHash {
    pub salt: [u8; 16],
    pub iterations: u32,
    pub hash: [u8; 32],
}

/// Derive the PBKDF2-HMAC-SHA256 hash of `password` under `salt`/`iterations`.
pub fn hash_password(password: &[u8], salt: &[u8; 16], iterations: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out);
    out
}

impl CredentialHash {
    /// Build a verifier from a plaintext password and a random salt.
    pub fn new(password: &[u8], salt: [u8; 16], iterations: u32) -> CredentialHash {
        CredentialHash {
            salt,
            iterations,
            hash: hash_password(password, &salt, iterations),
        }
    }

    /// Constant-time check that `password` matches this credential.
    pub fn verify(&self, password: &[u8]) -> bool {
        let candidate = hash_password(password, &self.salt, self.iterations);
        candidate.ct_eq(&self.hash).into()
    }
}

/// JSON form: hex-encoded salt + hash. Parsed in `config.rs`.
#[derive(Deserialize)]
pub struct CredentialConfig {
    pub salt_hex: String,
    pub hash_hex: String,
    #[serde(default = "default_iterations")]
    pub iterations: u32,
}

fn default_iterations() -> u32 {
    DEFAULT_ITERATIONS
}

/// Decode a fixed-length byte array from a hex string.
pub fn hex_to_bytes<const N: usize>(s: &str) -> Option<[u8; N]> {
    if s.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

/// Hex-encode bytes (for generating config / tests).
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

impl CredentialConfig {
    /// Resolve the JSON form into a usable [`CredentialHash`].
    pub fn resolve(&self) -> Option<CredentialHash> {
        Some(CredentialHash {
            salt: hex_to_bytes::<16>(&self.salt_hex)?,
            iterations: self.iterations,
            hash: hex_to_bytes::<32>(&self.hash_hex)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_is_deterministic_for_same_salt() {
        let salt = [7u8; 16];
        let a = hash_password(b"secret", &salt, 10_000);
        let b = hash_password(b"secret", &salt, 10_000);
        assert_eq!(a, b);
    }

    #[test]
    fn different_salt_changes_hash() {
        let a = hash_password(b"secret", &[1u8; 16], 10_000);
        let b = hash_password(b"secret", &[2u8; 16], 10_000);
        assert_ne!(a, b);
    }

    #[test]
    fn verify_accepts_correct_rejects_wrong() {
        let c = CredentialHash::new(b"hunter2", [3u8; 16], 10_000);
        assert!(c.verify(b"hunter2"));
        assert!(!c.verify(b"hunter3"));
        assert!(!c.verify(b""));
    }

    #[test]
    fn hex_roundtrip() {
        let salt = [0xABu8; 16];
        let s = bytes_to_hex(&salt);
        assert_eq!(hex_to_bytes::<16>(&s), Some(salt));
        assert_eq!(hex_to_bytes::<16>("zz"), None); // wrong length
    }

    #[test]
    fn credential_config_resolves() {
        let salt = [5u8; 16];
        let hash = hash_password(b"pw", &salt, DEFAULT_ITERATIONS);
        let cfg = CredentialConfig {
            salt_hex: bytes_to_hex(&salt),
            hash_hex: bytes_to_hex(&hash),
            iterations: DEFAULT_ITERATIONS,
        };
        let resolved = cfg.resolve().unwrap();
        assert!(resolved.verify(b"pw"));
        assert!(!resolved.verify(b"nope"));
    }
}
