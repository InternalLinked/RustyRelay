use crate::{networking::{client::tcp::TCPClientHandle, packethandler::{TCPServerPacketHandler, packet::Packet}, server::{lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, EncryptionHandler, EncryptionState, ServerCallback}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut, buf};
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

#[packet_macro::packet(5, true, true, false, 128, 64, 30)]
pub struct SecureBatch {
    
}

impl SecureBatch {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        if let EncryptionState::Encrypted(_) = &mut client_core.get_encryption_handler().encryption_state {
            println!("Handle encrpyted on server");
            let enc_blob = self.get_dynamic();

            let mut batch = client_core.get_encryption_handler().decrypt(enc_blob).unwrap();

            //TCPServerPacketHandler::handle_packet(&mut batch, client_core, lobby_manager).await;

        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle, encryption_handler: &mut EncryptionHandler) -> ClientCallback{

        
        
        ClientCallback::Pass
    }
}
