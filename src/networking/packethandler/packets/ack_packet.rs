use crate::{networking::{packethandler::{AnyPacket, packet::Packet}, server::{StreamHandle, lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::TCPClientHandle}}, util::{ServerCallback, ServerInstance}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(1, true, true, false, 128, 64, 30)]
pub struct AckPacket {
    request: u8,
}

impl AckPacket {
    pub async fn on_trigger_server(&self, client_handle: &mut TCPClientHandle, lobby_manager: &mut LobbyManagerHandle<TCPClientHandle>) -> ServerCallback {
        client_handle.counter += 1;


        let dynamic:Bytes = Bytes::copy_from_slice(b"test");
        let mut packet = AckPacket::new(0, dynamic);

        client_handle.send(AnyPacket::AckPacket(packet)).await;

        if client_handle.counter >= 1000000 {
            println!("Worked!");
        }

        if 0 != self.read_request() {
            //println!("Message {}", str::from_utf8(self.get_dynamic().as_ref()).unwrap());
            let dynamic:Bytes = Bytes::copy_from_slice(b"test");
            let mut packet = AckPacket::new(0, dynamic);

            let mut buffer = BytesMut::new();

            packet.encode(&mut buffer);

        }
        ServerCallback::Pass
    }

    pub fn on_trigger_client(&self, _server_instance: &mut ServerInstance) {
        if 0 != self.read_request() {
        } else {
        }
    }
}
