use std::{error::Error, fmt, net::SocketAddr};

use bytes::Bytes;

use crate::networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket::{self, LobbyBroadcastPacket, LobbyClosePacket, LobbyCreatePacket}, packets::{error_packet::SendableError, lobby::{broadcast::LobbyBroadcast, close::LobbyClose, create::LobbyCreate, join::LobbyJoin, leave::LobbyLeave}}}, server::{StreamHandle, lobby_handler::LobbyState}};

pub mod tcp;


#[derive(Debug)]
pub enum RelayHandleError {
    ServerRefusedConnection,
    ConnectionClosed,
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
            RelayHandleError::ConnectionClosed => {
                write!(f, "given address is invalid")
            }
        }
    }
}

impl Error for RelayHandleError {}


pub struct RelayClient {

    tcp_client: TCPClientHandle,
    lobby_state: LobbyState,

}

impl RelayClient {

    pub fn connect(address: &str) -> Result<Self, RelayHandleError> {

        let address: SocketAddr = "127.0.0.1:25565".parse().map_err(|_| RelayHandleError::InvalidAddress)?;

        let tcp_client = TCPClientHandle::new(address)?;

        Ok(Self {tcp_client, lobby_state: LobbyState::Alone})
        
    }

    pub fn create_lobby(&mut self) -> Result<String, SendableError>{

        /* Before sending a packet we first check locally if the client is acutally already host/member */
        if self.lobby_state != LobbyState::Alone {
        
            return Err(SendableError::AlreadyHostOrMember);
        }

        let packet = LobbyCreate::new();

        match self.tcp_client.blocking_send_awaitable(LobbyCreatePacket(packet)).map_err(|_| SendableError::ConnectionClose)? {
            AnyPacket::LobbyCreateSuccessPacket(success) => {
                let bytes = success.read_lobby_code().to_be_bytes();
                self.lobby_state = LobbyState::Host { lobby_id: success.read_lobby_id() as usize, code: success.read_lobby_code() };
                return Ok(String::from_utf8(bytes.to_vec()).unwrap());

            },
            AnyPacket::RelayErrorPacket(code) => return Err(SendableError::try_from(code.read_code()).unwrap()),
            _ => panic!("Unexpected packet"),
        }

    }

    pub fn join_lobby(&mut self, code: String) -> Result<(), SendableError> {

        /* We do the same as in create_lobby*/
        if self.lobby_state != LobbyState::Alone {
        
            return Err(SendableError::AlreadyHostOrMember);
        }

        /* Converting string into u64 for sending over network */
        let code = code.as_bytes();

        let mut buf = [0u8; 8];

        let len = code.len().min(8);
        buf[..len].copy_from_slice(&code[..len]);

        let code = u64::from_be_bytes(buf);

        let packet = LobbyJoin::new(code);


        match self.tcp_client.blocking_send_awaitable(AnyPacket::LobbyJoinPacket(packet)).unwrap() {

            AnyPacket::LobbyJoinSuccessPacket(success) => {

                self.lobby_state = LobbyState::Client { lobby_id: success.read_lobby_id() as usize, seat_id: success.read_seat_id() as usize };

                Ok(())
            }
            AnyPacket::RelayErrorPacket(code) =>  return Err(SendableError::try_from(code.read_code()).unwrap()),
            _ => panic!("Unexpected packet"),
        } 

    }


    pub fn leave_lobby(&mut self) -> Result<(), SendableError> {

        /* We do the same as in create_lobby*/
        if self.lobby_state != LobbyState::Alone {
        
            return Err(SendableError::AlreadyHostOrMember);
        }

        let packet = LobbyLeave::new();

        match self.tcp_client.blocking_send_awaitable(AnyPacket::LobbyLeavePacket(packet)).unwrap() {

            AnyPacket::LobbyLeaveSuccessPacket(_) => {
                self.lobby_state = LobbyState::Alone;
                Ok(())
            }
            AnyPacket::RelayErrorPacket(code) =>  return Err(SendableError::try_from(code.read_code()).unwrap()),
            _ => panic!("Unexpected packet"),
        } 

    }

    pub fn broadcast_lobby(&mut self, packet: Bytes) -> Result<(), SendableError>{

        /* Check if the client is alone*/
        if self.lobby_state == LobbyState::Alone {
        
            return Err(SendableError::NoMember);
        }

        let packet = {

            match self.lobby_state {
                LobbyState::Client { lobby_id: _, seat_id } => {
                    /* We do send or current seat id so that the host can check which client sent 
                    the package. If the seat id is worng the current client gets kicked entirely from the server */
                    LobbyBroadcast::new(seat_id as u64, packet)
                }
                LobbyState::Host { lobby_id: _, code: _ } => {
                    /* If the broadcast is send by the Host the auther (seat_id) is completely irrelevant */
                    LobbyBroadcast::new(0, packet)
                }

                _ => panic!("Thats impossible. Never happens"),

            }

        };

        self.tcp_client.blocking_send(LobbyBroadcastPacket(packet)).map_err(|_| SendableError::ConnectionClose)?;
        Ok(())

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