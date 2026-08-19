use std::{net::SocketAddr, sync::OnceLock};

use bytes::{BufMut, BytesMut};
use rand::{RngExt, TryRng, rngs::SysRng};
use snow::params::NoiseParams;
use tokio::{io::{self, AsyncReadExt}, net::TcpStream};
use version_macro::packed_version;

use crate::networking::packethandler::{ServerPacketHandler, packet::MAX_TCP_LENGTH};

pub const RELAY_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const RELAY_VERSION_BYTES: u32 = packed_version!();
static NOISE_PARAMS: OnceLock<NoiseParams> = OnceLock::new();

pub fn noise_params() -> &'static NoiseParams {
    NOISE_PARAMS.get_or_init(|| {
        "Noise_XX_25519_ChaChaPoly_BLAKE2s"
            .parse()
            .expect("invalid Noise pattern string")
    })
}

pub mod fastarray;

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

pub enum LobbyState {
    Alone,
    Host,
    Client,
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

pub fn random_string() -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::rng();
    (0..6)
        .map(|_| {
            let idx = rng.random_range(0..CHARS.len());
            CHARS[idx] as char
        })
        .collect()
}

pub fn generate_auth_token() -> [u8; 16] {
    let mut token = [0u8; 16];
    SysRng
        .try_fill_bytes(&mut token)
        .expect("failed to get random bytes");
    token
}

pub struct ServerInstance {
    pub(super) stream: TcpStream,
    pub(crate) encryption: EncryptionState,
}

pub struct ServerHandle {
    read_buffer: BytesMut,
    pub(super) write_buffer: BytesMut,
    pub(super) server_instance: ServerInstance,
}

impl ServerHandle {
    pub fn get_server_instance(&mut self) -> &mut ServerInstance {
        &mut self.server_instance
    }

    pub fn get_buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.read_buffer
    }

    pub fn get_buffer(&self) -> &BytesMut {
        &self.read_buffer
    }
}
