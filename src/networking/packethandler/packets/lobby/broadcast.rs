use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket::{self, AckPacket}, packet::Packet, packets::error_packet::{RelayError, SendableError}}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(11, true, true, false, 128, 64, 30)]
pub struct LobbyBroadcast {

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
            LobbyState::Client { lobby_id, seat_id: _ } => {
                if let Err(e) = lobby_manager.send_host(lobby_id, self.get_self().clone()).await {
                    client_core.send_client_error(self, e).await;
                    return ServerCallback::Pass;
                }

            }
            LobbyState::Host { lobby_id, code: _ } => {
                if let Err(e) = lobby_manager.broadcast(lobby_id, self.get_self().clone()).await {
                    client_core.send_client_error(self, e).await;
                }
            }
        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{


        let s = str::from_utf8(self.get_dynamic()).unwrap();

        println!("self {}", s);

        ClientCallback::Pass
    }
}
