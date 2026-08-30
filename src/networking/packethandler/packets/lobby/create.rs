use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::packet::Packet, server::{lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::{self};

#[packet_macro::packet(3, true, false, false, 128, 64, 30)]
pub struct CreateLobby {
    
}

impl CreateLobby {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
         
        //lobby_manager.create(host)

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        ClientCallback::Pass
    }
}
