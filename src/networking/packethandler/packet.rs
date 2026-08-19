use std::io;

use bytes::{Bytes, BytesMut};

use crate::{networking::server::tcp::tcp_client_handle::ClientInstance, util::{ServerCallback, ServerInstance}};

pub static MAX_TCP_LENGTH: usize = 4096;
pub static MAX_UDP_LENGTH: usize = 512;

pub trait AwaitablePacket: Packet + Awaitable {}
impl<T: Packet + Awaitable> AwaitablePacket for T {}

pub trait Packet: Send + Sync {
    fn get_id(&self) -> u8;

    fn encode(&mut self, buf: &mut BytesMut) -> io::Result<Bytes>;

    fn get_size(&self) -> usize;

    fn is_awaitable(&self) -> bool;

    /* This is the code that gets triggered if the server recieves the packet*/
    fn on_trigger_server(&self, _client_instance: &mut ClientInstance) -> ServerCallback {
        /*! Leave this here !*/
        todo!("Litte note that you forgot to implement on_trigger_server for a packet :)");
    }

    /* This is the code that gets triggered if the client recieves the packet*/
    fn on_trigger_client(&self, _client_instance: &mut ServerInstance) {
        /*! Leave this here !*/
        todo!("Litte note that you forgot to implement on_trigger_client for a packet :)");
    }

    fn get_nonce(&self) -> Option<u64>;

    fn set_nonce(&mut self, nonce: u64);

    fn get_dynamic_size(&self) -> u64;
}

/*
The reason why this trait is empty is because it just acts as
a boundary for passing a packet to send_awaitable */
pub trait Awaitable {}

pub trait DynamicPacket {}
