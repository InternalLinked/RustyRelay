use std::{error::Error, fmt, net::SocketAddr};

use crate::networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket::{self, LobbyClosePacket, LobbyCreatePacket}, packets::{error_packet::SendableError, lobby::{close::LobbyClose, create::LobbyCreate, join::LobbyJoin, leave::LobbyLeave}}}};

pub mod tcp;


#[derive(Debug)]
pub enum RelayHandleError {
    ServerRefusedConnection,
    HandshakeFailure,
    PipeError,
    InvalidAddress,
}

impl fmt::Display for RelayHandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelayHandleError::ServerRefusedConnection => {
                write!(f, "server refused the connection")
            }
            RelayHandleError::HandshakeFailure => {
                write!(f, "handshake with the server failed")
            }
            RelayHandleError::PipeError => {
                write!(f, "the oneshot pipe was broken")
            }
            RelayHandleError::InvalidAddress => {
                write!(f, "given address is invalid")
            }
        }
    }
}

impl Error for RelayHandleError {}


pub struct RelayClient {

    tcp_client: TCPClientHandle,

}

impl RelayClient {

    pub fn connect(address: &str) -> Result<Self, RelayHandleError> {

        let address: SocketAddr = "127.0.0.1:25565".parse().map_err(|_| RelayHandleError::InvalidAddress)?;

        let tcp_client = TCPClientHandle::new(address)?;

        Ok(Self {tcp_client})
        
    }

    pub fn create_lobby(&mut self) -> Result<String, SendableError>{

        let packet = LobbyCreate::new();

        match self.tcp_client.blocking_send_awaitable(LobbyCreatePacket(packet)).map_err(|_| SendableError::ConnectionClose)? {
            AnyPacket::LobbyCreateSuccessPacket(success) => {
                let bytes = success.read_lobby_code().to_be_bytes();

                return Ok(String::from_utf8(bytes.to_vec()).unwrap());

            },
            AnyPacket::RelayErrorPacket(code) => return Err(SendableError::try_from(code.read_code()).unwrap()),
            _ => panic!("Unexpected packet"),
        }

    }

    pub fn join_lobby(&mut self, code: String) -> Result<(), SendableError> {

        /* Converting string into u64 for sending over network */
        let code = code.as_bytes();

        let mut buf = [0u8; 8];

        let len = code.len().min(8);
        buf[..len].copy_from_slice(&code[..len]);

        let code = u64::from_be_bytes(buf);

        let packet = LobbyJoin::new(code);


        match self.tcp_client.blocking_send_awaitable(AnyPacket::LobbyJoinPacket(packet)).unwrap() {

            AnyPacket::LobbyJoinSuccessPacket(_) => {

                Ok(())
            }
            AnyPacket::RelayErrorPacket(code) =>  return Err(SendableError::try_from(code.read_code()).unwrap()),
            _ => panic!("Unexpected packet"),
        } 

    }


    pub fn leave_lobby(&mut self) -> Result<(), SendableError> {

        let packet = LobbyLeave::new();

        match self.tcp_client.blocking_send_awaitable(AnyPacket::LobbyLeavePacket(packet)).unwrap() {

            AnyPacket::LobbyLeaveSuccessPacket(_) => {
                
                Ok(())
            }
            AnyPacket::RelayErrorPacket(code) =>  return Err(SendableError::try_from(code.read_code()).unwrap()),
            _ => panic!("Unexpected packet"),
        } 

    }

    pub fn close_lobby(&mut self) -> Result<(), SendableError> {


        let packet = LobbyClose::new();

        match self.tcp_client.blocking_send_awaitable(AnyPacket::LobbyClosePacket(packet)).unwrap() {

            AnyPacket::LobbyCloseSuccessPacket(_) => {

                Ok(())
            }
            AnyPacket::RelayErrorPacket(code) =>  return Err(SendableError::try_from(code.read_code()).unwrap()),
            _ => panic!("Unexpected packet"),
        } 

    }

    pub fn close_client(&mut self) {
        let _ = self.tcp_client.blocking_close();
    }

}