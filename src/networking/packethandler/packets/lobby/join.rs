use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket::{AckPacket, LobbyJoinSuccessPacket, RelayErrorPacket}, packet::Packet, packets::error_packet::{RelayError, SendableError}}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self};

#[packet_macro::packet(5, true, false, false, 128, 64, 30)]
pub struct LobbyJoin {
    code: u64,
}

impl LobbyJoin {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        if client_core.lobby_state != LobbyState::Alone {
            
            client_core.send_client_error(self, SendableError::AlreadyHostOrMember).await;

            return ServerCallback::Pass;
        }

        match lobby_manager.join(self.read_code(), client_core.get_client_handle().clone()).await {
            Ok((lobby_id, seat_id)) => {

                client_core.lobby_state = LobbyState::Client { lobby_id, seat_id };

                let mut packet = LobbyJoinSuccess::new(lobby_id as u64, seat_id as u64);
                if self.is_awaitable_client() {
                    packet.set_awaitable_client();
                    packet.set_nonce(self.get_nonce());
                }

                client_core.get_client_handle().send(LobbyJoinSuccessPacket(packet)).await;

            },
            Err(e) => {
                client_core.send_client_error(self, e).await;

            }
        }


        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        ClientCallback::Pass
    }
}

#[packet_macro::packet(6, true, false, false, 128, 64, 30)]
pub struct LobbyJoinSuccess {
    lobby_id: u64,
    seat_id: u64
}

impl LobbyJoinSuccess {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{

        client_core.lobby_state = LobbyState::Client { lobby_id: self.read_lobby_id() as usize, seat_id: self.read_seat_id() as usize };
        
        ClientCallback::Pass
    }
}
