pub mod packet;
pub mod packets;

use std::io::{self};

use crate::networking::packethandler::packet::DynamicPacket;
use crate::networking::server::tcp::tcp_client_handle::{ClientHandle, ClientInstance};
use crate::{util::ServerHandle};
use crate::networking::packethandler::packets::ack_packet::AckPacket;
use crate::util::{ServerCallback, ServerInstance};
use bytes::Bytes;
use packet_handler_macro::{
    dispatch_packet_client_impl, dispatch_packet_impl, dispatch_packet_server_impl,
};

macro_rules! register_packets {
    ($($packet:ident),* $(,)?) => {
        pub enum PacketResponse {
            None,
            Kill,
            $(
                $packet($packet),
            )*
        }

        macro_rules! dispatch_packet_server {
            ($client_handle:expr) => {
                //dispatch_packet_server_impl!($client_handle, $($packet),*)
            };
        }
        macro_rules! dispatch_packet_client {
            ($server_handle:expr) => {
                dispatch_packet_client_impl!($server_handle, $($packet),*)
            };
        }
        macro_rules! dispatch_packet {
            ($bytes:expr) => {
                dispatch_packet_impl!($bytes, $($packet),*)
            };
        }
    };
}

register_packets!(
    AckPacket,
);

pub struct ClientPacketHandler;

impl ClientPacketHandler {
    pub fn handle_packet(server_handle: &mut ServerHandle) -> io::Result<bool> {
        
        dispatch_packet_client!(server_handle)

    }
}

pub struct ServerPacketHandler;

impl ServerPacketHandler {
    pub fn handle_packet(client_handle: &mut ClientHandle) -> ServerCallback {
        //dispatch_packet_server!(client_handle)
        ServerCallback::Pass
    }
}

pub fn dispatch_packet(bytes: &mut Bytes) -> PacketResponse {
    dispatch_packet!(bytes)
}
