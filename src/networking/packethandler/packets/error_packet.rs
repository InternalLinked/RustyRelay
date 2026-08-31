use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::packet::Packet, server::{lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::io::{self};

#[packet_macro::packet(2, true, false, false, 128, 64, 30)]
pub struct RelayError {
    code: u8,
}

impl RelayError {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
         
        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        ClientCallback::Pass
    }
}


#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
pub enum SendableError {
    Kick,
    AlreadyHostOrMember,


}