pub mod packet;
pub mod packets;

use std::io::{self};

use crate::networking::packethandler::packet::DynamicPacket;
use crate::networking::server::tcp::tcp_client_handle::{ClientHandle};
use crate::{util::ServerHandle};
use crate::networking::packethandler::packets::ack_packet::AckPacket;
use crate::util::{ServerCallback, ServerInstance};
use bytes::{Bytes, BytesMut};
use crate::networking::packethandler::packet::Packet;
use packet_handler_macro::{dispatch_packet_impl
};

macro_rules! register_packets {
    ($($packet:ident),* $(,)?) => {
        pub enum AnyPacket {
            None,
            Kill,
            $(
                $packet($packet),
            )*
        }
        
        impl AnyPacket {
            pub fn get_id(&self) -> u8 {
                match self {
                    $(
                    AnyPacket::$packet(p) => p.get_id(),
                    )*
                    _ => 0,

                }
            }

            pub async fn on_trigger_server(&self, client_handle: &mut ClientHandle) -> ServerCallback {
                match self {
                    $(
                    AnyPacket::$packet(p) => p.on_trigger_server(client_handle).await,
                    )*
                    _ => ServerCallback::Kill,

                }
            }

            pub fn encode(&mut self, buf: &mut BytesMut) -> io::Result<Bytes> {
                match self {
                    $(
                    AnyPacket::$packet(p) => p.encode(buf),
                    )*
                    _ => Err(std::io::ErrorKind::NotFound.into()),

                }
            }
            
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
        
        Ok(true)

    }
}

pub struct ServerPacketHandler;

impl ServerPacketHandler {
    pub async fn handle_packet(bytes: &mut BytesMut, client_handle: &mut ClientHandle) -> ServerCallback {
        
        loop {
            match dispatch_packet(bytes) {
                AnyPacket::Kill => return ServerCallback::Kill,
                AnyPacket::None => return ServerCallback::Pass,
                other => { other.on_trigger_server(client_handle).await;}

            }
        }
    }
}

pub fn dispatch_packet(bytes: &mut BytesMut) -> AnyPacket {
    dispatch_packet!(bytes)
}
