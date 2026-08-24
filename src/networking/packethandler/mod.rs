pub mod packet;
pub mod packets;

use std::io::{self};

use crate::networking::client::tcp::TCPClientHandle;
use crate::networking::packethandler::packet::DynamicPacket;
use crate::networking::packethandler::packets::ack::Ack;
use crate::networking::server::lobby_handler::LobbyManagerHandle;
use crate::networking::server::tcp::tcp_client_handle::{TCPClientInstanceHandle};
use crate::util::{ClientCallback, ServerCallback};
use bytes::{Bytes, BytesMut};
use crate::networking::packethandler::packet::Packet;
use packet_handler_macro::{dispatch_packet_impl
};

    
macro_rules! register_packets {
    ($($packet:ident),* $(,)?) => {
        paste::paste! {
            pub enum AnyPacket {
                None,
                Kill,
                $(
                    [<$packet Packet>]($packet),
                )*
            }

            impl AnyPacket {
                pub fn get_id(&self) -> u8 {
                    match self {
                        $(
                        AnyPacket::[<$packet Packet>](p) => p.get_id(),
                        )*
                        _ => 0,
                    }
                }

                pub async fn on_trigger_server_tcp(&self, client_handle: &mut TCPClientInstanceHandle, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
                    match self {
                        $(
                        AnyPacket::[<$packet Packet>](p) => p.on_trigger_server_tcp(client_handle, lobby_manager).await,
                        )*
                        _ => ServerCallback::Kill,
                    }
                }

                pub async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle) -> ClientCallback {
                    match self {
                        $(
                        AnyPacket::[<$packet Packet>](p) => p.on_trigger_client_tcp(client_handle).await,
                        )*
                        _ => ClientCallback::Kill,
                    }
                }

                pub fn encode(&mut self, buf: &mut BytesMut, nonce: u64) -> io::Result<(Bytes, Option<Bytes>)> {
                    match self {
                        $(
                        AnyPacket::[<$packet Packet>](p) => {
                            p.set_nonce(nonce);
                            p.encode(buf)
                        }, 
                        )*
                        _ => Err(std::io::ErrorKind::NotFound.into()),
                    }
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
    Ack,
);




pub struct TCPClientPacketHandler;

impl TCPClientPacketHandler {
    
pub async fn handle_packet(bytes: &mut BytesMut, client_handle: &mut TCPClientHandle) -> ClientCallback{
        
        loop {
            match dispatch_packet(bytes) {
                AnyPacket::Kill => return ClientCallback::Kill,
                AnyPacket::None => return ClientCallback::Pass,
                other => { other.on_trigger_client_tcp(client_handle).await;}

            }
        }
        

    }
}

pub struct TCPServerPacketHandler;

impl TCPServerPacketHandler {
    pub async fn handle_packet(bytes: &mut BytesMut, client_handle: &mut TCPClientInstanceHandle, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        loop {
            match dispatch_packet(bytes) {
                AnyPacket::Kill => return ServerCallback::Kill,
                AnyPacket::None => return ServerCallback::Pass,
                other => { other.on_trigger_server_tcp(client_handle, lobby_manager).await;}

            }
        }
    }
}

pub fn dispatch_packet(bytes: &mut BytesMut) -> AnyPacket {
    dispatch_packet!(bytes)
}
