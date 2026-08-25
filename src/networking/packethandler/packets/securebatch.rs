use crate::{networking::{client::tcp::TCPClientHandle, packethandler::packet::Packet, server::{lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::TCPClientInstanceHandle}}, util::{ClientCallback, EncryptionState, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut, buf};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(5, true, true, false, 128, 64, 30)]
pub struct SecureBatch {
    
}

impl SecureBatch {
    pub async fn on_trigger_server_tcp(&self, client_handle: &mut TCPClientInstanceHandle, encryption_state: &mut EncryptionState, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        if let EncryptionState::Encrypted(encryption) = encryption_state {

            let enc_blob = self.get_dynamic();

            let mut buffer = BytesMut::with_capacity(65_535);
            
            buffer.resize(65_535, 0);

            let len = encryption.read_message(&enc_blob[..(self.get_dynamic_size() as usize)], &mut buffer[..]).unwrap();
            

            println!("Length: {len}");

        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle) -> ClientCallback{

        println!("Got ack back from server {}", self.get_nonce());
        
        ClientCallback::Pass
    }
}
