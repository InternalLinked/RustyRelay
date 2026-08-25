use crate::{networking::{client::tcp::TCPClientHandle, packethandler::packet::Packet, server::{lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::TCPClientInstanceHandle}}, util::{ClientCallback, EncryptionState, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(2, true, false, false, 128, 64, 30)]
pub struct RelayError {
    code: u16,
}

impl RelayError {
    pub async fn on_trigger_server_tcp(&self, client_handle: &mut TCPClientInstanceHandle, encryption_state: &mut EncryptionState, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle) -> ClientCallback{
        
        ClientCallback::Pass
    }
}
