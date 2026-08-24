use crate::{networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket, packet::Packet}, server::{StreamHandle, lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::TCPClientInstanceHandle}}, util::{ClientCallback, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(1, true, true, false, 128, 64, 30)]
pub struct Ack {
    request: u8,
}

impl Ack {
    pub async fn on_trigger_server_tcp(&self, client_handle: &mut TCPClientInstanceHandle, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        client_handle.counter += 1;
        
        let dynamic:Bytes = Bytes::copy_from_slice(b"test");
        let mut packet = Ack::new(0, dynamic);

        client_handle.send(AnyPacket::AckPacket(packet)).await;

        if client_handle.counter >= 1000000 {
            println!("Worked!");
        }
        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle) -> ClientCallback{

        println!("Got ack back from server");

        ClientCallback::Pass
    }
}
