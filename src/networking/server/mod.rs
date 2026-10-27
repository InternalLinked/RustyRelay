use std::io;

use bytes::Bytes;

use crate::networking::packethandler::AnyPacket;

pub mod tcp;
pub mod lobby_handler;


pub trait StreamHandle: Send + 'static {
    fn send(&self, packet: AnyPacket) -> impl Future<Output = ()> + Send;

    fn send_raw(&self, packet: Bytes) -> impl Future<Output = ()> + Send;

    fn send_awaitable(&self, packet: AnyPacket) -> impl Future<Output = io::Result<AnyPacket>> + Send;

    fn close(&self) -> impl Future<Output = ()> + Send;

    fn get_id(&self) -> u64;
}