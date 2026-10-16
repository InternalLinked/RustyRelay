use std::{net::SocketAddr, sync::OnceLock, time::Instant};

use bytes::{BufMut, Bytes, BytesMut};
use rand::{RngExt, TryRng, rngs::SysRng};
use snow::{HandshakeState, TransportState, params::NoiseParams};
use tokio::{io, runtime::Runtime, sync::oneshot};
use version_macro::packed_version;

use crate::networking::packethandler::AnyPacket;

pub const RELAY_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const RELAY_VERSION_BYTES: u32 = packed_version!();


static NOISE_PARAMS: OnceLock<NoiseParams> = OnceLock::new();
static MONO_EPOCH: OnceLock<Instant> = OnceLock::new();

pub fn noise_params() -> &'static NoiseParams {
    NOISE_PARAMS.get_or_init(|| {
        "Noise_XX_25519_AESGCM_SHA256"
            .parse()
            .expect("invalid Noise pattern string")
    })
}

pub fn mono_epoch() -> &'static Instant {
    MONO_EPOCH.get_or_init(Instant::now)
}

pub fn now_ms() -> u64 {
    mono_epoch().elapsed().as_millis() as u64
}


static SERVER_RUNTIME: OnceLock<Runtime> = OnceLock::new();
static CLIENT_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn server_runtime() -> &'static Runtime {
    SERVER_RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

pub fn client_runtime() -> &'static Runtime {
    CLIENT_RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

pub fn packed_version_to_str(packed: u32) -> String {
    let major = (packed >> 20) & 0x3FF;
    let minor = (packed >> 10) & 0x3FF;
    let patch = packed & 0x3FF;

    format!("{major}.{minor}.{patch}")
}

pub enum EncryptionState {
    Unsecure,
    Handshake(snow::HandshakeState),
    Encrypted(snow::TransportState),
}
pub enum ServerCallback {
    /* Kill: Cuts the connection of client that returned the Kill enum */
    Kill,
    /* Pass: Instruct the server to do nothing */
    Pass,
    Error(std::io::Error),
}

pub enum ClientCallback {
    /* Kill: Cuts the connection of client that returned the Kill enum */
    Kill,
    /* Pass: Instruct the server to do nothing */
    Pass,
    Error(std::io::Error),
}

pub enum ClientContextInstruction {
    Close, 
    Packet {anypacket: AnyPacket },
    Awaitable {anypacket: AnyPacket, sender: oneshot::Sender<AnyPacket>},
    ResetLobbyState,
    RawPacket {raw_packet: Bytes}
}

pub fn random_code() -> u64 {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    let mut bytes = [0u8; 8];
    for b in bytes.iter_mut() {
        let idx = rng.random_range(0..CHARS.len());
        *b = CHARS[idx];
    }
    u64::from_le_bytes(bytes)
}
pub fn generate_auth_token() -> [u8; 16] {
    let mut token = [0u8; 16];
    SysRng
        .try_fill_bytes(&mut token)
        .expect("failed to get random bytes");
    token
}
