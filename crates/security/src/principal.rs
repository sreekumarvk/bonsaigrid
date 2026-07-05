//! An authenticated identity and its authorization decision.

use crate::permission::{name_matches, Action, Permission, ResourceType};

/// An authenticated principal with its compiled permission grants. Built once at
/// authentication and bound to the connection; `authorize` runs per request and
/// allocates nothing.
#[derive(Clone, Debug)]
pub struct Principal {
    pub name: String,
    pub grants: Vec<Permission>,
    /// Admin bypasses the grant list and authorizes every operation (including
    /// otherwise-unmapped/administrative ones).
    pub is_admin: bool,
}

impl Principal {
    /// A principal with no grants and no admin — denied everything.
    pub fn empty(name: impl Into<String>) -> Principal {
        Principal {
            name: name.into(),
            grants: Vec::new(),
            is_admin: false,
        }
    }

    /// The permissive `anonymous` principal used when no security config is
    /// present — authorizes everything, preserving today's no-auth behavior.
    pub fn anonymous_full() -> Principal {
        Principal {
            name: "anonymous".into(),
            grants: Vec::new(),
            is_admin: true,
        }
    }

    /// Authorize `action` on a resource of `rt` named `name`. Admin always
    /// passes; otherwise any grant whose type matches, whose glob matches the
    /// name, and whose action set contains `action` authorizes. Allocation-free.
    pub fn authorize(&self, rt: ResourceType, name: &str, action: Action) -> bool {
        if self.is_admin {
            return true;
        }
        self.grants.iter().any(|g| {
            g.resource_type == rt && g.actions.contains(action) && name_matches(&g.name, name)
        })
    }
}

/// Extract the subject Common Name (CN) from a DER-encoded X.509 certificate.
///
/// The CN attribute is OID 2.5.4.3, DER-encoded as `06 03 55 04 03`, immediately
/// followed by the value's TLV (a directory string). A cert carries the issuer DN
/// then the subject DN, so the LAST CN in the DER is the subject's — the client's
/// identity. This is a deliberately small, dependency-free scan (short-form lengths,
/// which cover every realistic CN); a mutually-authenticated cert has already been
/// cryptographically verified by the TLS handshake, so this only reads the name.
pub fn cn_from_cert_der(der: &[u8]) -> Option<String> {
    const CN_OID: [u8; 5] = [0x06, 0x03, 0x55, 0x04, 0x03];
    let mut found = None;
    let mut i = 0;
    while i + CN_OID.len() + 2 <= der.len() {
        if der[i..i + CN_OID.len()] == CN_OID {
            let tag_pos = i + CN_OID.len(); // value tag (e.g. 0x0C UTF8String, 0x13 Printable)
            let len = der[tag_pos + 1] as usize;
            let start = tag_pos + 2;
            if len < 0x80 && start + len <= der.len() {
                if let Ok(s) = std::str::from_utf8(&der[start..start + len]) {
                    found = Some(s.to_string()); // keep scanning → subject wins over issuer
                }
            }
        }
        i += 1;
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permission::ActionSet;

    #[test]
    fn cn_extractor_reads_subject_cn() {
        // A minimal DER fragment: CN OID + UTF8String "alice".
        let mut der = vec![0x30, 0x11]; // filler SEQUENCE header
        der.extend_from_slice(&[0x06, 0x03, 0x55, 0x04, 0x03]); // CN OID
        der.extend_from_slice(&[0x0C, 0x05]); // UTF8String, len 5
        der.extend_from_slice(b"alice");
        der.extend_from_slice(&[0x00, 0xFF]); // trailing bytes
        assert_eq!(cn_from_cert_der(&der).as_deref(), Some("alice"));
        assert_eq!(cn_from_cert_der(b"no cert here"), None);
    }

    #[test]
    fn cn_extractor_prefers_subject_over_issuer() {
        // issuer CN "ca" appears before subject CN "bob"; the subject must win.
        let mut der = Vec::new();
        for cn in ["ca", "bob"] {
            der.extend_from_slice(&[0x06, 0x03, 0x55, 0x04, 0x03, 0x13, cn.len() as u8]);
            der.extend_from_slice(cn.as_bytes());
        }
        assert_eq!(cn_from_cert_der(&der).as_deref(), Some("bob"));
    }

    fn read_only_on_orders() -> Principal {
        Principal {
            name: "app".into(),
            grants: vec![Permission {
                resource_type: ResourceType::Map,
                name: "orders*".into(),
                actions: ActionSet::of(Action::Read),
            }],
            is_admin: false,
        }
    }

    #[test]
    fn read_only_principal() {
        let p = read_only_on_orders();
        assert!(p.authorize(ResourceType::Map, "orders9", Action::Read));
        assert!(!p.authorize(ResourceType::Map, "orders9", Action::Put));
        assert!(!p.authorize(ResourceType::Map, "cart9", Action::Read)); // name mismatch
        assert!(!p.authorize(ResourceType::Queue, "orders9", Action::Read)); // type mismatch
    }

    #[test]
    fn admin_authorizes_everything() {
        let p = Principal {
            name: "ops".into(),
            grants: vec![],
            is_admin: true,
        };
        assert!(p.authorize(ResourceType::Map, "any", Action::Put));
        assert!(p.authorize(ResourceType::Cluster, "x", Action::Admin));
    }

    #[test]
    fn anonymous_full_authorizes_everything() {
        let p = Principal::anonymous_full();
        assert!(p.authorize(ResourceType::Map, "any", Action::Remove));
    }

    #[test]
    fn empty_principal_denies_everything() {
        let p = Principal::empty("nobody");
        assert!(!p.authorize(ResourceType::Map, "any", Action::Read));
    }
}
