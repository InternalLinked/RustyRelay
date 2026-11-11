use std::{os::unix::net::SocketAddr, sync::{Arc, atomic::AtomicU64}};

use bytes::Bytes;
use tokio::sync::{mpsc::Sender, oneshot};

use crate::{networking::{packethandler::AnyPacket, server::{StreamHandle, lobby_handler::LobbyManagerHandle}}, util::ClientContextInstruction};

#[derive(Clone)]
pub struct UDPClientInstanceHandle {
    sender: Sender<ClientContextInstruction>,
    latency: Arc<AtomicU64>
}
