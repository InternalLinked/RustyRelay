use crate::{networking::{client::{RelayClientCallback, tcp::{TCPClientCore, TCPClientHandle}}, packethandler::{AnyPacket::{self}, packet::Packet, packets::error_packet::{RelayError, SendableError}}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(17, true, true, 128, 64, 30)]
pub struct LobbyExplicit {
    seat_id: u64
}

impl LobbyExplicit {
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
            LobbyState::Member { lobby_id:_, seat_id: _ } => {


                /* If the client is a member it is not authorized to send a message explicitly*/
                client_core.send_client_error(self, SendableError::NotAuthorized).await;

                return ServerCallback::Pass;

            }
            LobbyState::Host { lobby_id, code: _ } => {
                lobby_manager.send_explicit(lobby_id, self.seat_id, self.get_self().clone()).await;

                let mut packet = LobbyExplicitSuccess::new();
                if self.is_awaitable_client() {
                    packet.set_awaitable_client();
                    packet.set_nonce(self.get_nonce());
                }

                client_core.get_client_handle().send(AnyPacket::LobbyExplicitSuccessPacket(packet)).await;
            }
        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        match client_core.lobby_state {
            LobbyState::Member { lobby_id, seat_id } => {
                let _ = client_core.relay_callback_writer.send(RelayClientCallback::Broadcast { author_id: 0, buffer: self.get_dynamic().clone() }).await;
            },
            _ => ()

        }

        ClientCallback::Pass
    }
}



#[packet_macro::packet(18, true, false, 128, 64, 30)]
pub struct LobbyExplicitSuccess {
}

impl LobbyExplicitSuccess {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{

        ClientCallback::Pass
    }
}