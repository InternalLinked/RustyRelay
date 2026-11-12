use crate::{networking::{client::tcp::{TCPClientCore, TCPClientHandle}, packethandler::{AnyPacket, packet::Packet}, server::{StreamHandle, lobby_handler::{LobbyManagerHandle, LobbyState}, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, ServerCallback, now_ms}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::networking::packethandler::DynamicPacket;
use std::{io::{self}, thread::{self, Thread}, time::{Duration, SystemTime, UNIX_EPOCH}};

#[packet_macro::packet(0, true, false, 128, 64, 30)]
pub struct Ping {
}

impl Ping {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {


        client_core.get_client_handle().send(AnyPacket::PongPacket(Pong::new())).await;


        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        client_core.get_client_handle().send(AnyPacket::PongPacket(Pong::new())).await;

        ClientCallback::Pass
    }
}


#[packet_macro::packet(16, true, false, 128, 64, 30)]
pub struct Pong {
}

impl Pong {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {


        let now = now_ms();
        let latency = now.saturating_sub(client_core.last_ping_timestamp);
        
        client_core.latency.store(latency, std::sync::atomic::Ordering::Relaxed);

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_core: &mut TCPClientCore) -> ClientCallback{
        
        let now = now_ms();
        let latency = now.saturating_sub(client_core.last_ping_timestamp);
        
        client_core.latency.store(latency, std::sync::atomic::Ordering::Relaxed);

        ClientCallback::Pass
    }
}