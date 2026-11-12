use crate::{networking::{client::{RelayClientCallback, tcp::{TCPClientCore, TCPClientHandle}}, packethandler::{AnyPacket::{self}, packet::Packet, packets::error_packet::{RelayError, SendableError}}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(10, true, true, 120, 10, 2000)]
pub struct LobbyBroadcast {
    author: u64
}

impl LobbyBroadcast {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        match client_core.lobby_state {
            LobbyState::Alone => {
                if self.is_awaitable_client() {
                    let mut error = RelayError::new(SendableError::NoMember as u8);
                    error.set_awaitable_client();
                    error.set_nonce(self.get_nonce());
                    client_core.get_client_handle().send(AnyPacket::RelayErrorPacket(error)).await;
                }
            },
            LobbyState::Member { lobby_id, seat_id } => {

                /* Checking if the seat id of the instance matches with the id sent by the Client. If not the Client is probably 
                trying to impersonate another client. If that is the case the connection gets killed */
                if seat_id != self.read_author() {

                    /* The entire reson for doing this so "clunky" is, that we can just pass the recieved bytes to the host without copying or alloacting once */
                    client_core.send_client_error(self, SendableError::NotAuthorized).await;

                    return ServerCallback::Kill;
                }
                
                if let Err(e) = lobby_manager.send_host(lobby_id, self.get_self().clone()).await {
                    client_core.send_client_error(self, e).await;
                    return ServerCallback::Pass;
                }

            }
            LobbyState::Host { lobby_id, code: _ } => {
            
                lobby_manager.broadcast(lobby_id, self.get_self().clone()).await;
            }
        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{

        match client_core.lobby_state {
            LobbyState::Member { lobby_id, seat_id } => {
                let _ = client_core.relay_callback_writer.send(RelayClientCallback::Broadcast { author_id: 0, buffer: self.get_dynamic().clone() }).await;
            },
            LobbyState::Host { lobby_id, code } => {
                let _ = client_core.relay_callback_writer.send(RelayClientCallback::Broadcast { author_id: self.read_author(), buffer: self.get_dynamic().clone() }).await;
            },
            _ => ()

        }

        ClientCallback::Pass
    }
}
