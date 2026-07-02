//! TLS transport security: mode, PEM loading, rustls config builders, and the
//! kernel-TLS (kTLS) key handoff.
//!
//! The handshake runs in userspace (rustls); once it completes we export the
//! negotiated session keys and install them on the socket via
//! `setsockopt(TLS_TX/TLS_RX)`, so the io_uring data path then moves plaintext
//! and the kernel does per-record crypto — preserving the zero-alloc hot path.
//!
//! We deliberately restrict kTLS to TLS 1.3 with AES-GCM, the widely supported
//! kernel-offload suites, and hand-roll the `setsockopt` (no `ktls` crate) to
//! avoid pulling in tokio/async on this synchronous io_uring reactor.

use rustls::crypto::ring;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{
    ClientConfig, ConnectionTrafficSecrets, ExtractedSecrets, RootCertStore, ServerConfig,
};
use std::io;
use std::os::fd::RawFd;
use std::sync::Arc;

/// Per-node TLS posture. `permissive` accepts both TLS and plaintext (for a
/// zero-downtime rollout); `required` is TLS-only; `disabled` is plaintext-only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsMode {
    Disabled,
    Permissive,
    Required,
}

impl TlsMode {
    /// Parse `BONSAI_TLS_MODE` (case-insensitive); unknown/empty → `Disabled`.
    pub fn parse(s: &str) -> TlsMode {
        match s.to_ascii_lowercase().as_str() {
            "permissive" => TlsMode::Permissive,
            "required" => TlsMode::Required,
            _ => TlsMode::Disabled,
        }
    }
    pub fn tls_enabled(self) -> bool {
        self != TlsMode::Disabled
    }
}

// ---- PEM loading ------------------------------------------------------------

/// Parse a PEM certificate chain.
pub fn load_certs(pem: &[u8]) -> io::Result<Vec<CertificateDer<'static>>> {
    let mut rd = std::io::Cursor::new(pem);
    rustls_pemfile::certs(&mut rd).collect::<Result<Vec<_>, _>>()
}

/// Parse the first PEM private key (PKCS#8, PKCS#1, or SEC1).
pub fn load_private_key(pem: &[u8]) -> io::Result<PrivateKeyDer<'static>> {
    let mut rd = std::io::Cursor::new(pem);
    rustls_pemfile::private_key(&mut rd)?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no private key in PEM"))
}

/// Build a root store from a PEM CA bundle (used to verify peers).
pub fn load_ca(pem: &[u8]) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(pem)? {
        roots
            .add(cert)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    }
    Ok(roots)
}

// ---- rustls config ----------------------------------------------------------

/// Server config (TLS 1.3, secret extraction on for kTLS). `client_ca` present
/// enables mutual TLS (the peer must present a cert trusted by that CA).
pub fn server_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    client_ca: Option<RootCertStore>,
) -> io::Result<Arc<ServerConfig>> {
    let provider = Arc::new(ring::default_provider());
    let builder = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(to_io)?;
    let mut config = match client_ca {
        Some(roots) => {
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(to_io)?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    }
    .with_single_cert(certs, key)
    .map_err(to_io)?;
    config.enable_secret_extraction = true;
    // No post-handshake tickets: keeps the record stream clean so kTLS RX can be
    // installed immediately after the handshake with nothing buffered.
    config.send_tls13_tickets = 0;
    Ok(Arc::new(config))
}

/// Client config (TLS 1.3, secret extraction on). `client_auth` present presents
/// a client certificate (for member mTLS).
pub fn client_config(
    roots: RootCertStore,
    client_auth: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
) -> io::Result<Arc<ClientConfig>> {
    let provider = Arc::new(ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(to_io)?
        .with_root_certificates(roots);
    let mut config = match client_auth {
        Some((certs, key)) => builder.with_client_auth_cert(certs, key).map_err(to_io)?,
        None => builder.with_no_client_auth(),
    };
    config.enable_secret_extraction = true;
    Ok(Arc::new(config))
}

fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
}

// ---- handshake driver (server or client side) ------------------------------

/// The internal member-mesh server name. Member certificates must carry this as
/// a SAN; the dialing side verifies the peer's server cert against it (mutual
/// auth otherwise rests on the shared CA + client certificate).
pub const MEMBER_SERVER_NAME: &str = "member.bonsaigrid";

enum Side {
    Server(rustls::ServerConnection),
    Client(rustls::ClientConnection),
}

/// Drives a TLS handshake (either side) over an external byte transport (an
/// io_uring loop), then hands off to kTLS. rustls is fed **one complete TLS
/// record at a time** so the transition to kTLS happens only at a clean record
/// boundary — no application record is left half-consumed in userspace where the
/// kernel couldn't see it.
pub struct Handshake {
    side: Side,
    /// Incoming TLS bytes not yet framed into a complete record.
    buf: Vec<u8>,
}

impl Handshake {
    /// Server side (we accepted the connection); the peer speaks first.
    pub fn server(config: Arc<ServerConfig>) -> io::Result<Handshake> {
        Ok(Handshake {
            side: Side::Server(rustls::ServerConnection::new(config).map_err(to_io)?),
            buf: Vec::new(),
        })
    }

    /// Client side (we dialed out); we send the ClientHello first — call
    /// [`pump`](Self::pump) with an empty slice right after construction to emit it.
    pub fn client(config: Arc<ClientConfig>, server_name: &str) -> io::Result<Handshake> {
        let name =
            rustls::pki_types::ServerName::try_from(server_name.to_string()).map_err(to_io)?;
        Ok(Handshake {
            side: Side::Client(rustls::ClientConnection::new(config, name).map_err(to_io)?),
            buf: Vec::new(),
        })
    }

    /// Feed newly-received bytes. Outgoing TLS bytes (handshake/alerts) are
    /// appended to `send`; any decrypted application plaintext is appended to
    /// `plain`. Returns `true` once the handshake is complete AND all buffered
    /// TLS data has been consumed at a record boundary — the safe point to
    /// install kTLS via [`into_ktls`](Self::into_ktls).
    pub fn pump(
        &mut self,
        incoming: &[u8],
        send: &mut Vec<u8>,
        plain: &mut Vec<u8>,
    ) -> io::Result<bool> {
        match &mut self.side {
            Side::Server(c) => pump_conn(c, &mut self.buf, incoming, send, plain),
            Side::Client(c) => pump_conn(c, &mut self.buf, incoming, send, plain),
        }
    }

    /// Extract the negotiated session keys and install kTLS on `fd` immediately.
    /// Only safe when no handshake output is still queued to send (the peer would
    /// otherwise receive the last plaintext handshake record double-encrypted).
    /// Use [`into_pending`](Self::into_pending) when a final flight (e.g. the
    /// client's Finished) still has to go out first.
    pub fn into_ktls(self, fd: RawFd) -> io::Result<()> {
        self.into_pending()?.install(fd)
    }

    /// Extract the session keys without touching the socket yet. Install kTLS via
    /// [`PendingKtls::install`] only after the last handshake flight has been
    /// written to the wire.
    pub fn into_pending(self) -> io::Result<PendingKtls> {
        let secrets = match self.side {
            Side::Server(c) => c.dangerous_extract_secrets(),
            Side::Client(c) => c.dangerous_extract_secrets(),
        }
        .map_err(to_io)?;
        Ok(PendingKtls { secrets })
    }
}

/// Extracted session keys awaiting kTLS install once the final plaintext
/// handshake flight has been flushed.
pub struct PendingKtls {
    secrets: ExtractedSecrets,
}

impl PendingKtls {
    /// Install kTLS on `fd` (TLS ULP + TX/RX crypto info).
    pub fn install(self, fd: RawFd) -> io::Result<()> {
        enable_ktls(fd, self.secrets)
    }
}

/// Shared handshake pump over either connection side.
fn pump_conn<S: rustls::SideData>(
    conn: &mut rustls::ConnectionCommon<S>,
    buf: &mut Vec<u8>,
    incoming: &[u8],
    send: &mut Vec<u8>,
    plain: &mut Vec<u8>,
) -> io::Result<bool> {
    use std::io::Read;
    buf.extend_from_slice(incoming);

    while let Some(rec_len) = complete_record_len(buf) {
        let mut rec: &[u8] = &buf[..rec_len];
        conn.read_tls(&mut rec)?;
        buf.drain(0..rec_len);
        let state = conn.process_new_packets().map_err(to_io)?;
        let n = state.plaintext_bytes_to_read();
        if n > 0 {
            let start = plain.len();
            plain.resize(start + n, 0);
            conn.reader().read_exact(&mut plain[start..])?;
        }
    }

    while conn.wants_write() {
        conn.write_tls(send)?;
    }

    Ok(!conn.is_handshaking() && buf.is_empty())
}

/// Reactor-facing bundle: the node's TLS mode plus the server config to run
/// handshakes with. Cheap to clone (the config is shared).
#[derive(Clone)]
pub struct TlsAcceptor {
    mode: TlsMode,
    config: Arc<ServerConfig>,
}

impl TlsAcceptor {
    pub fn new(mode: TlsMode, config: Arc<ServerConfig>) -> TlsAcceptor {
        TlsAcceptor { mode, config }
    }
    pub fn mode(&self) -> TlsMode {
        self.mode
    }
    /// Start a fresh server-side handshake for a new connection.
    pub fn handshake(&self) -> io::Result<Handshake> {
        Handshake::server(self.config.clone())
    }
}

/// Member-mesh mutual-TLS bundle: this node presents its cert as both a server
/// (to inbound peers) and a client (dialing out), verifying the peer against the
/// shared CA in both directions. Cheap to clone.
#[derive(Clone)]
pub struct MemberTls {
    mode: TlsMode,
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
}

impl MemberTls {
    /// Build from this node's PEM cert chain + key and the peer-verifying CA.
    pub fn new(
        mode: TlsMode,
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
        ca: RootCertStore,
    ) -> io::Result<MemberTls> {
        // Reuse the same identity for both roles.
        let (c2, k2) = (certs.clone(), clone_key(&key));
        let server = server_config(certs, key, Some(ca.clone()))?; // require client certs (mTLS)
        let client = client_config(ca, Some((c2, k2)))?; // present our cert when dialing
        Ok(MemberTls {
            mode,
            server,
            client,
        })
    }
    pub fn mode(&self) -> TlsMode {
        self.mode
    }
    /// Handshake for an inbound (accepted) peer connection — we are the server.
    pub fn accept(&self) -> io::Result<Handshake> {
        Handshake::server(self.server.clone())
    }
    /// Handshake for an outbound (dialed) peer connection — we are the client.
    pub fn dial(&self) -> io::Result<Handshake> {
        Handshake::client(self.client.clone(), MEMBER_SERVER_NAME)
    }
}

/// Deep-copy a `PrivateKeyDer` (it isn't `Clone`).
fn clone_key(key: &PrivateKeyDer<'static>) -> PrivateKeyDer<'static> {
    PrivateKeyDer::try_from(key.secret_der().to_vec())
        .expect("re-parse of a valid private key cannot fail")
}

/// Length (header + payload) of the first complete TLS record in `buf`, or
/// `None` if a full record isn't present yet. A TLS record is
/// `type(1) | version(2) | length(2, big-endian) | payload(length)`.
fn complete_record_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < 5 {
        return None;
    }
    let payload = u16::from_be_bytes([buf[3], buf[4]]) as usize;
    let total = 5 + payload;
    if buf.len() < total {
        None
    } else {
        Some(total)
    }
}

// ---- kTLS setsockopt --------------------------------------------------------

// From <linux/tls.h> / <netinet/tcp.h>.
const SOL_TCP: libc::c_int = 6;
const TCP_ULP: libc::c_int = 31;
const SOL_TLS: libc::c_int = 282;
const TLS_TX: libc::c_int = 1;
const TLS_RX: libc::c_int = 2;
const TLS_1_3_VERSION: u16 = 0x0304;
const TLS_CIPHER_AES_GCM_128: u16 = 51;
const TLS_CIPHER_AES_GCM_256: u16 = 52;

#[repr(C)]
struct CryptoInfoAesGcm128 {
    version: u16,
    cipher_type: u16,
    iv: [u8; 8],
    key: [u8; 16],
    salt: [u8; 4],
    rec_seq: [u8; 8],
}

#[repr(C)]
struct CryptoInfoAesGcm256 {
    version: u16,
    cipher_type: u16,
    iv: [u8; 8],
    key: [u8; 32],
    salt: [u8; 4],
    rec_seq: [u8; 8],
}

/// Install kTLS on `fd` from the extracted TLS 1.3 session secrets: attach the
/// TLS ULP, then set the TX and RX crypto info. After this returns, ordinary
/// `send`/`recv` (io_uring or otherwise) on `fd` carry plaintext and the kernel
/// encrypts/decrypts each record.
pub fn enable_ktls(fd: RawFd, secrets: ExtractedSecrets) -> io::Result<()> {
    // Attach the "tls" upper-layer protocol to the TCP socket.
    let ulp = b"tls\0";
    let rc = unsafe {
        libc::setsockopt(
            fd,
            SOL_TCP,
            TCP_ULP,
            ulp.as_ptr() as *const libc::c_void,
            3, // length of "tls"
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    set_direction(fd, TLS_TX, secrets.tx.0, &secrets.tx.1)?;
    set_direction(fd, TLS_RX, secrets.rx.0, &secrets.rx.1)?;
    Ok(())
}

fn set_direction(
    fd: RawFd,
    dir: libc::c_int,
    seq: u64,
    secret: &ConnectionTrafficSecrets,
) -> io::Result<()> {
    // The kernel splits the 12-byte TLS 1.3 nonce into salt (first 4) + iv (last
    // 8); rec_seq is the record sequence number, big-endian.
    let rec_seq = seq.to_be_bytes();
    match secret {
        ConnectionTrafficSecrets::Aes128Gcm { key, iv } => {
            let iv = iv.as_ref();
            let key = key.as_ref();
            let mut info = CryptoInfoAesGcm128 {
                version: TLS_1_3_VERSION,
                cipher_type: TLS_CIPHER_AES_GCM_128,
                iv: [0; 8],
                key: [0; 16],
                salt: [0; 4],
                rec_seq,
            };
            info.salt.copy_from_slice(&iv[0..4]);
            info.iv.copy_from_slice(&iv[4..12]);
            info.key.copy_from_slice(&key[0..16]);
            setsockopt_tls(fd, dir, &info)
        }
        ConnectionTrafficSecrets::Aes256Gcm { key, iv } => {
            let iv = iv.as_ref();
            let key = key.as_ref();
            let mut info = CryptoInfoAesGcm256 {
                version: TLS_1_3_VERSION,
                cipher_type: TLS_CIPHER_AES_GCM_256,
                iv: [0; 8],
                key: [0; 32],
                salt: [0; 4],
                rec_seq,
            };
            info.salt.copy_from_slice(&iv[0..4]);
            info.iv.copy_from_slice(&iv[4..12]);
            info.key.copy_from_slice(&key[0..32]);
            setsockopt_tls(fd, dir, &info)
        }
        _ => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "kTLS supports only AES-GCM suites",
        )),
    }
}

fn setsockopt_tls<T>(fd: RawFd, dir: libc::c_int, info: &T) -> io::Result<()> {
    let rc = unsafe {
        libc::setsockopt(
            fd,
            SOL_TLS,
            dir,
            info as *const T as *const libc::c_void,
            std::mem::size_of::<T>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_parse() {
        assert_eq!(TlsMode::parse("disabled"), TlsMode::Disabled);
        assert_eq!(TlsMode::parse("Permissive"), TlsMode::Permissive);
        assert_eq!(TlsMode::parse("REQUIRED"), TlsMode::Required);
        assert_eq!(TlsMode::parse("bogus"), TlsMode::Disabled);
        assert!(TlsMode::Required.tls_enabled());
        assert!(!TlsMode::Disabled.tls_enabled());
    }

    #[test]
    fn pem_roundtrip() {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_pem = ck.cert.pem();
        let key_pem = ck.key_pair.serialize_pem();
        let certs = load_certs(cert_pem.as_bytes()).unwrap();
        assert_eq!(certs.len(), 1);
        load_private_key(key_pem.as_bytes()).unwrap();
        load_ca(cert_pem.as_bytes()).unwrap();
    }
}
