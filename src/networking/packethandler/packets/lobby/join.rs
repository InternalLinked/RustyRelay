use crate::{networking::{client::{RelayClientCallback, tcp::{TCPClientCore, TCPClientHandle}}, packethandler::{AnyPacket::{LobbyJoinSuccessPacket, RelayErrorPacket}, packet::Packet, packets::error_packet::{RelayError, SendableError}}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self};

#[packet_macro::packet(4, true, false, 3, 2000, 30000)]
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

                client_core.lobby_state = LobbyState::Member { lobby_id, seat_id };
                
                /* Notfiy the host that a client joined */
                let mut joined_event = LobbyJoinedEvent::new(seat_id);
                let joined_event = joined_event.encode(&mut client_core.write_buffer).unwrap();
                lobby_manager.relay_broadcast(lobby_id, joined_event).await;

                let mut packet = LobbyJoinSuccess::new(lobby_id, seat_id);
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

#[packet_macro::packet(5, true, false, 1, 1000, 60000)]
pub struct LobbyJoinSuccess {
    lobby_id: u64,
    seat_id: u64
}

impl LobbyJoinSuccess {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{

        client_core.lobby_state = LobbyState::Member { lobby_id: self.read_lobby_id(), seat_id: self.read_seat_id() };
        let _ = client_core.relay_callback_writer.send(RelayClientCallback::LobbySeatChanged { seat_id: self.read_seat_id()}).await;
        
        ClientCallback::Pass
    }
}

#[packet_macro::packet(13, true, false, 1, 1000, 60000)]
pub struct LobbyJoinedEvent {
    seat_id: u64
}

impl LobbyJoinedEvent {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{

        match client_core.lobby_state {
            LobbyState::Host { lobby_id: _, code: _ } => {

                let _ = client_core.relay_callback_writer.send(RelayClientCallback::LobbyJoined { author_id: self.read_seat_id() }).await;

            },
            LobbyState::Member { lobby_id, seat_id }=> {

                if self.read_seat_id() == seat_id {
                    return ClientCallback::Pass;
                }

                let _ = client_core.relay_callback_writer.send(RelayClientCallback::LobbyJoined { author_id: self.read_seat_id() }).await;
            },

            _ => ()

        }

        
        ClientCallback::Pass
    }
}
