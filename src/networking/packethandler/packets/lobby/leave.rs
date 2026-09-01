use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket::{AckPacket, RelayErrorPacket}, packet::Packet, packets::error_packet::RelayError}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self};

#[packet_macro::packet(7, true, false, false, 128, 64, 30)]
pub struct LobbyLeave {
    request: u8,
}

impl LobbyLeave {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        match client_core.lobby_state {
            LobbyState::Client { lobby_id, seat_id } => {
                if let Err(e) = lobby_manager.leave(lobby_id, seat_id).await {
                    client_core.send_client_error(self, e).await;
                };
                
                return ServerCallback::Pass;
            }

            _ => ()
        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        ClientCallback::Pass
    }
}
