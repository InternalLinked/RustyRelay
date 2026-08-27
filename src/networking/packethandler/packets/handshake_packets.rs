use crate::{networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket, packet::Packet, packets::error_packet::RelayError}, server::{StreamHandle, lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::{TCPClientInstanceCore, TCPClientInstanceHandle}}}, util::{ClientCallback, EncryptionHandler, EncryptionState, EncryptionTransport, RELAY_VERSION_BYTES, ServerCallback, noise_params}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use snow::Builder;
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

async fn handle_unsecure(packet: &HandshakeHello, client_core: &mut TCPClientInstanceCore) -> ServerCallback {

    
    if packet.version != RELAY_VERSION_BYTES {
        let response = RelayError::new(2);
        client_core.get_client_handle().send(AnyPacket::RelayErrorPacket(response)).await;
        return ServerCallback::Kill;
    }

    let client_public_key = packet.get_dynamic();
    let mut server_public_key = [0u8; 1024];
    let params = noise_params().clone();

    let responder_keys = Builder::new(params.clone()).generate_keypair().unwrap();

    let mut responder = Builder::new(params.clone())
        .local_private_key(&responder_keys.private)
        .unwrap()
        .build_responder()
        .unwrap();

    let _len = responder
        .read_message(client_public_key, &mut server_public_key)
        .unwrap();

    let len = responder
        .write_message(&[], &mut server_public_key)
        .unwrap();

    let mut packet = HandshakeAck::new(Bytes::copy_from_slice(&server_public_key[..len]));

    client_core.get_encryption_handler().encryption_state = EncryptionState::Handshake(responder);
    
    client_core.get_client_handle().send(AnyPacket::HandshakeAckPacket(packet)).await;
    ServerCallback::Pass

}

#[packet_macro::packet(3, true, true, false, 128, 64, 30)]
pub struct HandshakeHello {
    version: u32,
}

impl HandshakeHello {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
            
        match client_core.get_encryption_handler().encryption_state {

            EncryptionState::Unsecure => {
                return handle_unsecure(self, client_core).await;
            },
            _ => ()

        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle, encryption_handler: &mut EncryptionHandler) -> ClientCallback{

        
        ClientCallback::Pass
    }
}


#[packet_macro::packet(4, true, true, false, 128, 64, 30)]
pub struct HandshakeAck {
    
}

impl HandshakeAck {
    pub async fn on_trigger_server_tcp(&self, client_core: &mut TCPClientInstanceCore, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
     
        let old_state = std::mem::replace(&mut client_core.get_encryption_handler().encryption_state, EncryptionState::Unsecure);

        if let EncryptionState::Handshake(mut handshake) = old_state {
            let mut scratch = [0u8; 1024];
            let msg3_from_client = self.get_dynamic().as_ref();
            match handshake.read_message(msg3_from_client, &mut scratch) {
                Ok(_n) => {
                     client_core.get_encryption_handler().encryption_state = EncryptionState::Encrypted(handshake.into_transport_mode().unwrap());
                     println!("[Server] Encrypted.");
                }
                Err(e) => println!("{:?}", e),
            }
        }

        
        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle, encryption_handler: &mut EncryptionHandler) -> ClientCallback{

        let old_state = std::mem::replace(&mut encryption_handler.encryption_state, EncryptionState::Unsecure);

        if let EncryptionState::Handshake(mut handshake) = old_state { 

            let server_public_key = self.get_dynamic();
            let mut buf = [0u8; 1024];
            handshake.read_message(&server_public_key, &mut buf).unwrap(); 

            let mut msg3 = [0u8; 255];
            let len = handshake.write_message(&[], &mut msg3).unwrap();
            let initiator_transport = handshake.into_transport_mode().unwrap();

        

            let response = HandshakeAck::new(Bytes::copy_from_slice(&msg3[..len]));

            client_handle.send_unsecure(AnyPacket::HandshakeAckPacket(response)).await;

            encryption_handler.encryption_state = EncryptionState::Encrypted(initiator_transport);


            println!("[Client] Encrypted");
        }

        

        
        ClientCallback::Pass
    }
}