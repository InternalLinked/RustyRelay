use std::{error::Error, fmt, net::SocketAddr};

use bytes::Bytes;
use tokio::sync::mpsc::{self, Receiver};

use crate::networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket::{self, LobbyBroadcastPacket, LobbyClosePacket, LobbyCreatePacket, LobbyKickPacket}, packets::{error_packet::SendableError, lobby::{broadcast::LobbyBroadcast, close::LobbyClose, create::LobbyCreate, join::LobbyJoin, kick::LobbyKick, leave::LobbyLeave}}}, server::{StreamHandle, lobby_handler::LobbyState}};

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
                write!(f, "the connection to the server was closed")
            }
        }
    }
}

impl Error for RelayHandleError {}



pub enum RelayClientCallback {
    Broadcast{author_id: usize, buffer: Bytes},
    LobbyJoined{author_id: usize},
    LobbyLeft{author_id: usize},
    LobbyKicked,
    LobbySeatChanged{seat_id: usize}

}

pub struct RelayClient {

    tcp_client: TCPClientHandle,
    lobby_state: LobbyState,
    broadcast_reader: Receiver<RelayClientCallback>
}


impl RelayClient {

    pub fn connect(address: &str) -> Result<Self, RelayHandleError> {

        let address: SocketAddr = address.parse().map_err(|_| RelayHandleError::InvalidAddress)?;

        let (sender, broadcast_reader) = mpsc::channel::<RelayClientCallback>(96);

        let tcp_client = TCPClientHandle::new(address, sender)?;
        
        Ok(Self {tcp_client, lobby_state: LobbyState::Alone, broadcast_reader})
        
    }

    pub fn is_host(&self) -> bool {
        if let LobbyState::Host { lobby_id: _, code: _ } = self.lobby_state {
            return true;
        }
        false
    }

    pub fn get_latency(&self) -> u64 {
        self.tcp_client.get_latency()
    }

    pub fn create_lobby(&mut self) -> Result<String, SendableError>{

        /* Before sending a packet we first check locally if the client is actually already host/member */
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


        match self.tcp_client.blocking_send_awaitable(AnyPacket::LobbyJoinPacket(packet)).map_err(|_| SendableError::ConnectionClose)? {

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
        if self.lobby_state == LobbyState::Alone {
        
            return Err(SendableError::NoMember);
        }

        let packet = LobbyLeave::new();

        match self.tcp_client.blocking_send_awaitable(AnyPacket::LobbyLeavePacket(packet)).map_err(|_| SendableError::ConnectionClose)? {

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

    pub fn kick_client(&mut self, seat_id: usize) -> Result<(), SendableError> {

        let LobbyState::Host { lobby_id, code } = self.lobby_state else {
            return Err(SendableError::NotAuthorized);
        };

        let packet = LobbyKick::new(seat_id as u64);

        match self.tcp_client.blocking_send_awaitable(LobbyKickPacket(packet)).map_err(|_| SendableError::ConnectionClose)? {
            AnyPacket::LobbyKickSuccessPacket(_) => {
                return Ok(());
            },
            AnyPacket::RelayErrorPacket(code) =>  return Err(SendableError::try_from(code.read_code()).unwrap()),
            _ => panic!("Unexpected packet")
        }

    }

    pub fn close_lobby(&mut self) -> Result<(), SendableError> {

        /* Only the host can close the lobby, so we check locally before sending */
        if !self.is_host() {
            return Err(SendableError::NotAuthorized);
        }

        let packet = LobbyClose::new();

        match self.tcp_client.blocking_send_awaitable(AnyPacket::LobbyClosePacket(packet)).map_err(|_| SendableError::ConnectionClose)? {

            AnyPacket::LobbyCloseSuccessPacket(_) => {
                self.lobby_state = LobbyState::Alone;
                Ok(())
            }
            AnyPacket::RelayErrorPacket(code) =>  return Err(SendableError::try_from(code.read_code()).unwrap()),
            _ => panic!("Unexpected packet"),
        } 

    }

    pub fn poll_event(&mut self) -> Option<RelayClientCallback> {

        /* We check if we got kicked. If so we set the lobby_state to LobbyState::Alone.
        This comes with the disadvantage that the clients state only gets updated when we poll events as this is the only way currently.
        The advantage is, that we do not have to open another thread for just listening for the kick event or use another mutext that we have to pass to the handler  */

        let event = self.broadcast_reader.blocking_recv();
        if let Some(e) = &event {
            match e {
                RelayClientCallback::LobbyKicked => {
                    self.lobby_state = LobbyState::Alone;
                }
                RelayClientCallback::LobbySeatChanged { seat_id: new_seat_id } => {
                    /* Only a client has a seat. When we are Alone or Host the event is irrelevant (e.g. seat 0 after leaving) */
                    if let LobbyState::Client { lobby_id: _, seat_id } = &mut self.lobby_state {
                        *seat_id = *new_seat_id;
                    }
                }
                _ => ()
            }
        }

        event
    }

    pub fn close_client(&mut self) {
        let _ = self.tcp_client.blocking_close();
    }

}

impl Drop for RelayClient {
    fn drop(&mut self) {
        /* The run loop holds its own sender clone, so the channel never closes on its own.
        Without an explicit Close the connection task would live forever after the client is dropped */
        self.tcp_client.close_detached();
    }
}