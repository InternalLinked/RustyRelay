pub mod packet;
pub mod packets;

use std::io::{self};
use std::time::Duration;

use crate::networking::client::tcp::{TCPClientCore, TCPClientHandle};
use crate::networking::packethandler::packet::DynamicPacket;
use crate::networking::packethandler::packets::error_packet::SendableError::RessourceRateLimited;
use crate::networking::packethandler::packets::lobby::explicit::{LobbyExplicit, LobbyExplicitSuccess};
use crate::networking::packethandler::packets::ping::{Ping, Pong};
use crate::networking::packethandler::packets::error_packet::RelayError;
use crate::networking::packethandler::packets::lobby::broadcast::LobbyBroadcast;
use crate::networking::packethandler::packets::lobby::close::{LobbyClose, LobbyCloseSuccess};
use crate::networking::packethandler::packets::lobby::create::{LobbyCreate, LobbyCreateSuccess};
use crate::networking::packethandler::packets::lobby::join::{LobbyJoin, LobbyJoinSuccess, LobbyJoinedEvent};
use crate::networking::packethandler::packets::lobby::kick::{LobbyClientKick, LobbyKick, LobbyKickSuccess};
use crate::networking::packethandler::packets::lobby::leave::{LobbyLeave, LobbyLeaveSuccess, LobbyLeftEvent};
use crate::networking::ratecache::RateCache;
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

                /// `None` for the internal `None` / `Kill` variants, which have no wire id.
                pub fn get_id(&self) -> Option<u8> {
                    match self {
                        $(
                        AnyPacket::[<$packet Packet>](p) => Some(p.get_id()),
                        )*
                        _ => None,
                    }
                }

                pub async fn on_trigger_server_tcp(&mut self, client_core: &mut TCPClientInstanceCore, rate_limiter: &mut RateCache, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
                    let owned = std::mem::replace(self, AnyPacket::None);
                   
                    match owned {
                        $(
                        AnyPacket::[<$packet Packet>](p) => {

                            // A reply to a request the server is waiting on skips the
                            // rate limiter. A reply with an unknown nonce is treated
                            // like any other packet and gets rate-limited.
                            let reply_waiter = if p.is_awaitable_server() {
                                client_core.get_awaitable_cache().try_remove(p.get_nonce() as usize)
                            } else {
                                None
                            };

                            if reply_waiter.is_none() {
                                let Some(node) = rate_limiter.get_node(p.get_id() as usize) else {
                                    client_core.send_client_error(&p, RessourceRateLimited).await;
                                    return ServerCallback::Pass;
                                };

                                if !node.try_pass(Duration::from_millis([<$packet>]::CACHE_THRESHOLD as u64)) {
                                    client_core.send_client_error(&p, RessourceRateLimited).await;
                                    return ServerCallback::Pass;
                                }

                                // Checked after `try_pass` counted this packet, so the packet
                                // that goes over the limit is the first one rejected.
                                if node.get_cache_hit() > [<$packet>]::MAX_CACHE {
                                    node.set_timeout(Duration::from_millis([<$packet>]::TIMEOUT as u64));
                                    client_core.send_client_error(&p, RessourceRateLimited).await;
                                    return ServerCallback::Pass;
                                }
                            }

                            let result = p.on_trigger_server_tcp(client_core, lobby_manager).await;
                            if let Some(oneshot) = reply_waiter {
                                let _ = oneshot.send(AnyPacket::[<$packet Packet>](p));
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
    pub async fn handle_packet(bytes: &mut BytesMut, rate_limiter: &mut RateCache, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {

        loop {

            match dispatch_packet(bytes) {
                AnyPacket::Kill => return ServerCallback::Kill,
                AnyPacket::None => return ServerCallback::Pass,
                mut other => {

                    match other.on_trigger_server_tcp(client_core, rate_limiter, lobby_manager).await {
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

    /// The rate limiter indexes `RateCache` by packet id, so ids must be exactly
    /// `0..REGISTERED_PACKET_COUNT` with no gaps. Reports every missing id
    /// (with the packets on either side of the gap) and every id that is out of range.
    #[test]
    fn packet_ids_are_contiguous_from_zero() {
        let name_of = |id: usize| -> Option<&str> {
            REGISTERED_PACKET_IDS
                .iter()
                .position(|&p| p as usize == id)
                .map(|i| REGISTERED_PACKET_NAMES[i])
        };

        let mut problems = Vec::new();

        for id in 0..REGISTERED_PACKET_COUNT {
            if name_of(id).is_none() {
                let below = (0..id).rev().find_map(|i| name_of(i).map(|n| format!("{n} ({i})")));
                let above = (id + 1..=u8::MAX as usize).find_map(|i| name_of(i).map(|n| format!("{n} ({i})")));
                problems.push(format!(
                    "missing id {id} (between {} and {})",
                    below.as_deref().unwrap_or("<start>"),
                    above.as_deref().unwrap_or("<end>"),
                ));
            }
        }

        for (name, &id) in REGISTERED_PACKET_NAMES.iter().zip(REGISTERED_PACKET_IDS.iter()) {
            if id as usize >= REGISTERED_PACKET_COUNT {
                problems.push(format!(
                    "{name} uses id {id}, but ids must be below {REGISTERED_PACKET_COUNT}"
                ));
            }
        }

        assert!(
            problems.is_empty(),
            "Packet ids must be 0..{REGISTERED_PACKET_COUNT} with no gaps:\n  {}",
            problems.join("\n  ")
        );
    }
}
