use crate::{networking::{client::{RelayClientCallback, tcp::{TCPClientCore, TCPClientHandle}}, packethandler::{AnyPacket::self, packet::Packet, packets::{error_packet::{RelayError, SendableError}, lobby::{close::LobbyCloseSuccess, join::LobbyJoinedEvent}}}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self};

#[packet_macro::packet(7, true, false, false, 128, 64, 30)]
pub struct LobbyLeave {
}

impl LobbyLeave {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        match client_core.lobby_state {
            LobbyState::Member { lobby_id, seat_id } => {
                if let Err(e) = lobby_manager.leave(lobby_id, seat_id).await {
                    client_core.send_client_error(self, e).await;
                    return ServerCallback::Pass;
                };
                
                client_core.lobby_state = LobbyState::Alone;

                /* Notfiy the host that a client left */
                let mut left_event = LobbyLeftEvent::new(seat_id);
                let left_event = left_event.encode(&mut client_core.write_buffer).unwrap();
                lobby_manager.relay_broadcast(lobby_id, left_event).await;

                let mut packet = LobbyLeaveSuccess::new();
                if self.is_awaitable_client() {
                    packet.set_awaitable_client();
                    packet.set_nonce(self.get_nonce());
                }

                client_core.get_client_handle().send(AnyPacket::LobbyLeaveSuccessPacket(packet)).await;
                
                return ServerCallback::Pass;
            }

            LobbyState::Alone => {
                if self.is_awaitable_client() {
                    let mut error = RelayError::new(SendableError::NoMember as u8);
                    error.set_awaitable_client();
                    error.set_nonce(self.get_nonce());
                    client_core.get_client_handle().send(AnyPacket::RelayErrorPacket(error)).await;
                }

                
            },

            LobbyState::Host { lobby_id, code } => {
                if let Err(e) = lobby_manager.close(lobby_id, client_core.get_client_handle().get_id()).await {
                    client_core.send_client_error(self, e).await;
                    return ServerCallback::Pass;
                };

                let mut left_event = LobbyLeftEvent::new(0);
                let left_event = left_event.encode(&mut client_core.write_buffer).unwrap();
                lobby_manager.relay_broadcast(lobby_id, left_event).await;

                let mut packet = LobbyCloseSuccess::new();
                if self.is_awaitable_client() {
                    packet.set_awaitable_client();
                    packet.set_nonce(self.get_nonce());
                }

                client_core.get_client_handle().send(AnyPacket::LobbyCloseSuccessPacket(packet)).await;
            }
            
        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        ClientCallback::Pass
    }
}

#[packet_macro::packet(8, true, false, false, 128, 64, 30)]
pub struct LobbyLeaveSuccess {
}

impl LobbyLeaveSuccess {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        client_core.lobby_state = LobbyState::Alone;
        let _ = client_core.relay_callback_writer.send(RelayClientCallback::LobbySeatChanged { seat_id: 0}).await;

        ClientCallback::Pass
    }
}


#[packet_macro::packet(15, true, false, false, 128, 64, 30)]
pub struct LobbyLeftEvent {
    seat_id: u64
}

impl LobbyLeftEvent {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{


        match client_core.lobby_state {
            LobbyState::Host { lobby_id: _, code: _ } => {

                let _ = client_core.relay_callback_writer.send(RelayClientCallback::LobbyLeft { author_id: self.read_seat_id() }).await;

            },
            LobbyState::Member { lobby_id, seat_id }=> {
                
                if self.read_seat_id() == seat_id {
                    return ClientCallback::Pass;
                }

                let _ = client_core.relay_callback_writer.send(RelayClientCallback::LobbyLeft { author_id: self.read_seat_id() }).await;
            },

            _ => ()

        }
        
        ClientCallback::Pass
    }
}