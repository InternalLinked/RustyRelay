use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket::RelayErrorPacket, packet::Packet, packets::error_packet::{RelayError, SendableError::{self, AlreadyHostOrMember}}}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::broadcast::error::SendError;
use std::io::{self};

#[packet_macro::packet(3, true, false, false, 128, 64, 30)]
pub struct CreateLobby {
    
}

impl CreateLobby {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        /* Handeling create lobby */
        /*Check if the client is already a host or a member of a lobby */
        if client_core.lobby_state != LobbyState::Alone {
            


            /* If so check if the client acutally wants a response back */
            if !self.is_awaitable_client() {
                return ServerCallback::Pass;
            }

            /* Sending a response back that the client is already a host or a member */

            let mut error_packet = RelayError::new(SendableError::AlreadyHostOrMember as u8);
            error_packet.set_awaitable_client();
            error_packet.set_nonce(self.get_nonce());

            client_core.get_client_handle().send(RelayErrorPacket(error_packet)).await;

            return ServerCallback::Pass;
        }

        /* If the client is alone we can create a lobby */
        match lobby_manager.create(client_core.get_client_handle().clone()).await {
            Ok((lobby_id, lobby_code)) => {

                client_core.lobby_state = LobbyState::Host { lobby_id }

            },
            Err(e) => {
                if !self.is_awaitable_client() {
                    return ServerCallback::Pass;
                }

                let mut error_packet = RelayError::new(e as u8);
                error_packet.set_awaitable_client();
                error_packet.set_nonce(self.get_nonce());
            }
        }



        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        ClientCallback::Pass
    }
}


#[packet_macro::packet(4, true, false, false, 128, 64, 30)]
pub struct LobbyCreateSuccess {
    
}

impl LobbyCreateSuccess {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {


        

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        ClientCallback::Pass
    }
}
