use std::{net::SocketAddr, sync::OnceLock};

use bytes::{BufMut, Bytes, BytesMut};
use rand::{RngExt, TryRng, rngs::SysRng};
use snow::{HandshakeState, TransportState, params::NoiseParams};
use tokio::{io::{self, AsyncReadExt}, net::TcpStream, runtime::Runtime, sync::oneshot};
use version_macro::packed_version;

use crate::networking::packethandler::{AnyPacket, TCPServerPacketHandler, packet::MAX_TCP_LENGTH, packets::handshake_packets::HandshakeHello};

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

use std::fmt;

#[derive(Debug)]
pub enum EncryptionError {
    DecryptionFailed,
    InvalidState,
    BufferTooSmall,
}

impl fmt::Display for EncryptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncryptionError::DecryptionFailed => write!(f, "failed to decrypt message"),
            EncryptionError::InvalidState => write!(f, "invalid encryption state"),
            EncryptionError::BufferTooSmall => write!(f, "encryption buffer is too small"),
        }
    }
}

pub enum EncryptionTransport {
    Handshake(HandshakeState),
    Transport(TransportState)
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

pub enum ClientContextInstruction {
    Close, 
    Packet(AnyPacket),
    Awaitable {anypacket: AnyPacket, sender: oneshot::Sender<AnyPacket>},
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


pub struct EncryptionHandler {
    pub encryption_state: EncryptionState,
    encryption_buffer: BytesMut
}

impl EncryptionHandler {

    pub fn new() -> Self {

        let buffer = BytesMut::zeroed(8096);
        
        EncryptionHandler { encryption_state: EncryptionState::Unsecure, encryption_buffer: buffer }

    }


    pub fn decrypt(&mut self, blob: &Bytes) -> Result<Bytes, EncryptionError> {
        match &mut self.encryption_state {
            EncryptionState::Encrypted(encrypt) => {
                let len = encrypt.read_message(
                        &blob[..blob.len()],
                        &mut self.encryption_buffer[..],
                    ).map_err(|_| EncryptionError::DecryptionFailed)?;

                Ok(self.encryption_buffer.split_to(len).freeze())
            }
            _ => Err(EncryptionError::InvalidState),
        }
    }

    pub fn encrypt(&mut self, msg: &Bytes) -> Result<Bytes, EncryptionError> {
        match &mut self.encryption_state {
            EncryptionState::Encrypted(encrypt) => {
                
                let len = encrypt.read_message(&msg[..msg.len()], &mut self.encryption_buffer).unwrap();

                Ok(self.encryption_buffer.split_to(len).freeze())
            }
            _ => Err(EncryptionError::InvalidState),
        }
    }
    
}