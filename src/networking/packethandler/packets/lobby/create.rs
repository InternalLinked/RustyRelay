use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket::{LobbyCreateSuccessPacket, RelayErrorPacket}, packet::Packet, packets::error_packet::{RelayError, SendableError::{self, AlreadyHostOrMember}}}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::broadcast::error::SendError;
use std::io::{self};

#[packet_macro::packet(2, true, false, 3, 1000, 10000)]
pub struct LobbyCreate {
    
}

impl LobbyCreate {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        /* Handeling create lobby */
        /*Check if the client is already a host or a member of a lobby */
        if client_core.lobby_state != LobbyState::Alone {

            client_core.send_client_error(self, SendableError::AlreadyHostOrMember).await;

            return ServerCallback::Pass;
        }

        /* If the client is alone we can create a lobby */
        match lobby_manager.create(client_core.get_client_handle().clone()).await {
            Ok((lobby_id, code)) => {
                client_core.lobby_state = LobbyState::Host { lobby_id, code };

                let mut packet = LobbyCreateSuccess::new(lobby_id, code);
                if self.is_awaitable_client() {
                    packet.set_awaitable_client();
                    packet.set_nonce(self.get_nonce());
                }


                client_core.get_client_handle().send(LobbyCreateSuccessPacket(packet)).await;



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


#[packet_macro::packet(3, true, false, 1, 1000, 60000)]
pub struct LobbyCreateSuccess {
    lobby_id: u64,
    lobby_code: u64
}

impl LobbyCreateSuccess {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{

        client_core.lobby_state = LobbyState::Host { lobby_id: self.read_lobby_id(), code: self.read_lobby_code() };
        
        ClientCallback::Pass
    }
}
