use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket::RelayErrorPacket, packet::Packet}, server::{StreamHandle, lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::io::{self};

#[packet_macro::packet(2, true, false, false, 128, 64, 30)]
pub struct RelayError {
    code: u8,
}

impl RelayError {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
         
        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        ClientCallback::Pass
    }
}


/// Errors that can be reported back to a caller as the result of a lobby operation.
///
/// These are returned inside the `Result` layer of each [`LobbyManagerHandle`] method's
/// response (see those methods for how this combines with the outer `Option`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendableError {
    /// Reserved for signaling a kick-related failure or notification. Not currently
    /// produced by [`LobbyManager`] itself — see [`NotAuthorized`](SendableError::NotAuthorized)
    /// for the "requester isn't the host" case.
    Kick,
    AlreadyHostOrMember,
    /// The target lobby already has the maximum number of clients allowed
    /// (`max_clients_per_lobby`), so [`Lobby::add_client`] refused to add another one.
    LobbyFull,
    /// The [`LobbyManager`] already has the maximum number of lobbies allowed
    /// (`max_lobbies`), so no new lobby could be created.
    MaxLobbiesExceded,
    /// The `lobby_id`/join code supplied does not correspond to any currently active
    /// lobby (either it never existed or has since been closed).
    LobbyNotFound,
    /// The requester attempted an action (e.g. kicking a client or closing a lobby)
    /// that only the lobby's host is permitted to perform.
    NotAuthorized,
    /// The lobby was found, but the given seat was not occupied by any client.
    SeatNotFound,
}