pub mod packet;
pub mod packets;

use std::io::{self};

use crate::networking::client::tcp::{TCPClientCore, TCPClientHandle};
use crate::networking::packethandler::packet::DynamicPacket;
use crate::networking::packethandler::packets::ack::Ack;
use crate::networking::packethandler::packets::error_packet::RelayError;
use crate::networking::packethandler::packets::lobby::create::{LobbyCreate, LobbyCreateSuccess};
use crate::networking::packethandler::packets::lobby::join::{LobbyJoin, LobbyJoinSuccess};
use crate::networking::packethandler::packets::lobby::kick::{LobbyClientKick, LobbyKick};
use crate::networking::packethandler::packets::lobby::leave::{LobbyLeave, LobbyLeaveSuccess};
use crate::networking::server::lobby_handler::LobbyManagerHandle;
use crate::networking::server::tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle};
use crate::util::{ClientCallback, EncryptionState, ServerCallback};
use bytes::{Bytes, BytesMut};
use slab::Slab;
use tokio::sync::oneshot;
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

                pub async fn on_trigger_server_tcp(&mut self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
                    let owned = std::mem::replace(self, AnyPacket::None);
                   
                    match owned {
                        $(
                        AnyPacket::[<$packet Packet>](p) => {
                            let result = p.on_trigger_server_tcp(client_core, lobby_manager).await;
                            if p.is_awaitable_server() {
                                println!("Is awaitable_server");

                                if let Some(oneshot) = client_core.get_awaitable_cache().try_remove(p.get_nonce() as usize) {

                                    let _ = oneshot.send(AnyPacket::[<$packet Packet>](p));
                                }else {
                                    return ServerCallback::Kill;
                                }
                            }
                            return result;
                        },
                        )*
                        _ => ServerCallback::Kill,
                    }
                }

                pub async fn on_trigger_client_tcp(&mut self, client_core: &mut TCPClientCore, awaitable_cache: &mut Slab<oneshot::Sender<AnyPacket>>) -> ClientCallback {

                    let owned = std::mem::replace(self, AnyPacket::None);

                    match owned {
                        $(
                        AnyPacket::[<$packet Packet>](p) => {
                            let result = p.on_trigger_client_tcp(client_core).await;
                            if p.is_awaitable_client() {
                                if let Some(oneshot) = awaitable_cache.try_remove(p.get_nonce() as usize) {
                                    let _ = oneshot.send(AnyPacket::[<$packet Packet>](p));
                                    
                                }else {
                                    return ClientCallback::Kill;
                                }
                            }
                            return result;
                            
                        },
                        )*
                        _ => ClientCallback::Kill,
                    }
                }

                pub fn encode(&mut self, buf: &mut BytesMut) -> io::Result<Bytes> {
                    match self {
                        $(
                        AnyPacket::[<$packet Packet>](p) => p.encode(buf), 
                        )*
                        _ => Err(std::io::ErrorKind::NotFound.into()),
                    }
                }

                pub fn encode_awaitable_client(&mut self, buf: &mut BytesMut, nonce: u64) -> io::Result<Bytes> {
                    match self {
                        $(
                        AnyPacket::[<$packet Packet>](p) => {
                            p.set_awaitable_client();
                            p.set_nonce(nonce);
                            p.encode(buf)
                        }, 
                        )*
                        _ => Err(std::io::ErrorKind::NotFound.into()),
                    }
                }

                pub fn encode_awaitable_server(&mut self, buf: &mut BytesMut, nonce: u64) -> io::Result<Bytes> {
                    match self {
                        $(
                        AnyPacket::[<$packet Packet>](p) => {
                            p.set_awaitable_server();
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
    RelayError,
    LobbyCreate,
    LobbyCreateSuccess,
    LobbyJoin,
    LobbyJoinSuccess,
    LobbyLeave,
    LobbyLeaveSuccess,
    LobbyKick,
    LobbyClientKick
);




pub struct TCPClientPacketHandler;

impl TCPClientPacketHandler {
    
pub async fn handle_packet(bytes: &mut BytesMut, client_core: &mut TCPClientCore, awaitable_cache: &mut Slab<oneshot::Sender<AnyPacket>>) -> ClientCallback{
        loop {
            match dispatch_packet(bytes) {
                AnyPacket::Kill => return ClientCallback::Kill,
                AnyPacket::None => return ClientCallback::Pass,
                mut other => { 
                    match other.on_trigger_client_tcp(client_core, awaitable_cache).await {
                        ClientCallback::Pass => (),
                        other => return other,

                    }
                }

            }
        }
        

    }
}

pub struct TCPServerPacketHandler;

impl TCPServerPacketHandler {
    pub async fn handle_packet(bytes: &mut BytesMut, client_core: &mut TCPClientInstanceCore,  lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        loop {

            match dispatch_packet(bytes) {
                AnyPacket::Kill => return ServerCallback::Kill,
                AnyPacket::None => return ServerCallback::Pass,
                mut other => { other.on_trigger_server_tcp(client_core, lobby_manager).await;}

            }
        }
    }
}

pub fn dispatch_packet(bytes: &mut BytesMut) -> AnyPacket {
    dispatch_packet!(bytes)
}
