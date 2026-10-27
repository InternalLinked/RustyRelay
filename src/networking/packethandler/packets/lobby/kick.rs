use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket::{LobbyCloseSuccessPacket, LobbyKickSuccessPacket}, packet::Packet, packets::{error_packet::SendableError, lobby::{close::LobbyCloseSuccess, leave::LobbyLeftEvent}}}, server::{StreamHandle, lobby_handler::{HOST_SEAT, LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};


/* Is getting send by the host to kick a client from it's lobby */
#[packet_macro::packet(9, true, false, false, 128, 64, 30)]
pub struct LobbyKick {
    seat_id: u64,
}

impl LobbyKick {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        if let LobbyState::Host { lobby_id, code: _ } = client_core.lobby_state {
            let requester_id = client_core.get_client_handle().get_id();

            /* The host always sits on HOST_SEAT. If it kicks itself the lobby gets closed instead,
            otherwise the lobby would be left without a host */
            if self.read_seat_id() == HOST_SEAT {
                if let Err(e) = lobby_manager.close(lobby_id, requester_id).await {
                    client_core.send_client_error(self, e).await;
                    return ServerCallback::Pass;
                }

                client_core.lobby_state = LobbyState::Alone;

                let mut packet = LobbyCloseSuccess::new();
                if self.is_awaitable_client() {
                    packet.set_awaitable_client();
                    packet.set_nonce(self.get_nonce());
                }

                client_core.get_client_handle().send(LobbyCloseSuccessPacket(packet)).await;
                return ServerCallback::Pass;
            }

            match lobby_manager.kick(lobby_id, requester_id, self.read_seat_id()).await {
                Ok(mut lobby_client) => {
                    lobby_client.reset_lobby_state().await;

                    /* Notify the lobby that the client left. Has to happen before the awaitable check,
                    otherwise the lobby never hears about non-awaitable kicks */
                    let mut left_event = LobbyLeftEvent::new(self.read_seat_id());
                    let left_event = left_event.encode(&mut client_core.write_buffer).unwrap();
                    lobby_manager.relay_broadcast(lobby_id, left_event).await;

                    if !self.is_awaitable_client() {
                        return ServerCallback::Pass;
                    }

                    let mut response = LobbyKickSuccess::new();
                    response.set_nonce(self.get_nonce());
                    response.set_awaitable_client();


                    client_core.get_client_handle().send(LobbyKickSuccessPacket(response)).await;

                },
                Err(e) => {

                    client_core.send_client_error(self, e).await;

                }
            }
        }else{
            client_core.send_client_error(self, SendableError::NotAuthorized).await;
        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        ClientCallback::Pass
    }
}



/* Packet that the client gets when it got kicked */
#[packet_macro::packet(10, true, false, false, 128, 64, 30)]
pub struct LobbyClientKick {
}

impl LobbyClientKick {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        let _ = client_core.relay_callback_writer.send(crate::networking::client::RelayClientCallback::LobbyKicked).await;

        client_core.lobby_state = LobbyState::Alone;

        ClientCallback::Pass
    }
}

#[packet_macro::packet(16, true, false, false, 128, 64, 30)]
pub struct LobbyKickSuccess {
}

impl LobbyKickSuccess {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        

        ClientCallback::Pass
    }
}
