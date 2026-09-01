use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket::{self, AckPacket}, packet::Packet, packets::error_packet::SendableError}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
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
            match lobby_manager.kick(lobby_id, requester_id, self.read_seat_id() as usize).await {
                Ok(mut lobby_client) => {
                    lobby_client.reset_lobby_state().await;


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



/* Packet that the client get when it got kicked */
#[packet_macro::packet(10, true, false, false, 128, 64, 30)]
pub struct LobbyClientKick {
}

impl LobbyClientKick {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        client_core.lobby_state = LobbyState::Alone;

        ClientCallback::Pass
    }
}
