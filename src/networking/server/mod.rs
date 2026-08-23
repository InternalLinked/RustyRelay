use crate::networking::packethandler::AnyPacket;

pub mod tcp;
pub mod lobby_handler;


pub trait StreamHandle: Send + 'static {
    async fn send(&mut self, packet: AnyPacket);

    async fn close(&mut self);

    fn get_id(&self) -> usize;
}