use crate::{networking::{packethandler::packet::Packet, server::tcp::tcp_client_handle::ClientHandle}, util::{ServerCallback, ServerInstance}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(1, true, true, false, 128, 64, 30)]
pub struct AckPacket {
    request: u8,
}

impl AckPacket {
    pub async fn on_trigger_server(&self, client_handle: &mut ClientHandle) -> ServerCallback {
        client_handle.counter += 1;
        //println!("Count {} ", client_handle.counter);
        if client_handle.counter >= 1000000 {
            println!("Worked!");
        }

        if 0 != self.read_request() {
            //println!("Message {}", str::from_utf8(self.get_dynamic().as_ref()).unwrap());
            let packet = AckPacket::new(0, b"test");

        }
        ServerCallback::Pass
    }

    pub fn on_trigger_client(&self, _server_instance: &mut ServerInstance) {
        if 0 != self.read_request() {
        } else {
        }
    }
}
