use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket::{LobbyCloseSuccessPacket, LobbyCreateSuccessPacket}, packet::Packet, packets::error_packet::SendableError}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(11, true, false, 128, 64, 30)]
pub struct LobbyClose {
    
}

impl LobbyClose {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        match client_core.lobby_state {
            
            LobbyState::Host { lobby_id, code: _ } => {
                if let Err(e) = lobby_manager.close(lobby_id, client_core.get_client_handle().get_id()).await {
                    client_core.send_client_error(self, e).await;
                    return ServerCallback::Pass;
                };

                
                client_core.lobby_state = LobbyState::Alone;
                
                let mut packet = LobbyCloseSuccess::new();
                if self.is_awaitable_client() {
                    packet.set_awaitable_client();
                    packet.set_nonce(self.get_nonce());
                }


                client_core.get_client_handle().send(LobbyCloseSuccessPacket(packet)).await;
            }

            _ => {

                client_core.send_client_error(self, SendableError::AlreadyHostOrMember).await;
            },


        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        


        ClientCallback::Pass
    }
}



#[packet_macro::packet(12, true, false, 128, 64, 30)]
pub struct LobbyCloseSuccess {
    
}

impl LobbyCloseSuccess {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        println!("Lobby closed");
        client_core.lobby_state = LobbyState::Alone;

        ClientCallback::Pass
    }
}
