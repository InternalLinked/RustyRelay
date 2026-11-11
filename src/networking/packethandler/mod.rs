pub mod packet;
pub mod packets;

use std::io::{self};

use crate::networking::client::tcp::{TCPClientCore, TCPClientHandle};
use crate::networking::packethandler::packet::DynamicPacket;
use crate::networking::packethandler::packets::lobby::explicit::{LobbyExplicit, LobbyExplicitSuccess};
use crate::networking::packethandler::packets::ping::{Ping, Pong};
use crate::networking::packethandler::packets::error_packet::RelayError;
use crate::networking::packethandler::packets::lobby::broadcast::LobbyBroadcast;
use crate::networking::packethandler::packets::lobby::close::{LobbyClose, LobbyCloseSuccess};
use crate::networking::packethandler::packets::lobby::create::{LobbyCreate, LobbyCreateSuccess};
use crate::networking::packethandler::packets::lobby::join::{LobbyJoin, LobbyJoinSuccess, LobbyJoinedEvent};
use crate::networking::packethandler::packets::lobby::kick::{LobbyClientKick, LobbyKick, LobbyKickSuccess};
use crate::networking::packethandler::packets::lobby::leave::{LobbyLeave, LobbyLeaveSuccess, LobbyLeftEvent};
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
        /// Number of packet types registered via `register_packets!`.
        pub const REGISTERED_PACKET_COUNT: usize = [$(stringify!($packet)),*].len();

        /// IDs of all registered packets, in registration order.
        pub const REGISTERED_PACKET_IDS: [u8; REGISTERED_PACKET_COUNT] = [$($packet::ID),*];

        /// Names of all registered packets, in registration order.
        pub const REGISTERED_PACKET_NAMES: [&str; REGISTERED_PACKET_COUNT] = [$(stringify!($packet)),*];

        paste::paste! {
            pub enum AnyPacket {
                None,
                Kill,
                $(
                    [<$packet Packet>]($packet),
                )*
            }

            impl AnyPacket {
                /// Number of registered packet types (excludes the internal `None` / `Kill` variants).
                pub const fn registered_count() -> usize {
                    REGISTERED_PACKET_COUNT
                }

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

                pub async fn on_trigger_server_udp(&mut self) {
                    let owned = std::mem::replace(self, AnyPacket::None);
                   
                    match owned {
                        $(
                        AnyPacket::[<$packet Packet>](p) => {
                            p.on_trigger_server_udp().await;
                        },
                        )*
                        _ => ()
                    }
                }


                pub async fn on_trigger_client_udp(&mut self) {
                    let owned = std::mem::replace(self, AnyPacket::None);
                   
                    match owned {
                        $(
                        AnyPacket::[<$packet Packet>](p) => {
                            p.on_trigger_server_udp().await;
                        },
                        )*
                        _ => ()
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
    Ping,
    RelayError,
    LobbyCreate,
    LobbyCreateSuccess,
    LobbyJoin,
    LobbyJoinSuccess,
    LobbyLeave,
    LobbyLeaveSuccess,
    LobbyKick,
    LobbyClientKick,
    LobbyBroadcast,
    LobbyClose,
    LobbyCloseSuccess,
    LobbyLeftEvent,
    LobbyJoinedEvent,
    LobbyKickSuccess,
    Pong,
    LobbyExplicit,
    LobbyExplicitSuccess
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
                mut other => {
                    match other.on_trigger_server_tcp(client_core, lobby_manager).await {
                        ServerCallback::Pass => (),
                        other => return other,
                    }
                }

            }
        }
    }
}

pub struct UDPServerPacketHandler;

impl UDPServerPacketHandler {

    pub async fn handle_packet(bytes: &mut BytesMut) -> ServerCallback {

        loop {

            match dispatch_packet(bytes) {
                AnyPacket::Kill => return ServerCallback::Kill,
                AnyPacket::None => return ServerCallback::Pass,
                mut other => other.on_trigger_client_udp().await
                

            }
        }
    }

}

pub struct UDPClientPacketHandler;

impl UDPClientPacketHandler {

    pub async fn handle_packet(bytes: &mut BytesMut) -> ClientCallback {

        loop {

            match dispatch_packet(bytes) {
                AnyPacket::Kill => return ClientCallback::Kill,
                AnyPacket::None => return ClientCallback::Pass,
                mut other => other.on_trigger_client_udp().await
            
            }
        }
    }
}


pub fn dispatch_packet(bytes: &mut BytesMut) -> AnyPacket {
    dispatch_packet!(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_count_matches_ids() {
        assert_eq!(AnyPacket::registered_count(), REGISTERED_PACKET_IDS.len());
    }

    #[test]
    fn packet_ids_are_unique() {
        let mut seen: [Option<&str>; 256] = [None; 256];
        let mut duplicates = Vec::new();

        for (name, id) in REGISTERED_PACKET_NAMES.iter().zip(REGISTERED_PACKET_IDS.iter()) {
            match seen[*id as usize] {
                Some(other) => duplicates.push(format!("id {id}: {other} and {name}")),
                None => seen[*id as usize] = Some(name),
            }
        }

        assert!(duplicates.is_empty(), "Duplicate packet IDs: {duplicates:?}");
    }

    #[test]
    fn packet_id_zero_is_reserved() {
        // `AnyPacket::get_id` returns 0 for `None` / `Kill`, so no real packet may use it.
        for (name, id) in REGISTERED_PACKET_NAMES.iter().zip(REGISTERED_PACKET_IDS.iter()) {
            assert_ne!(*id, 0, "{name} uses reserved packet id 0");
        }
    }
}
