use crate::{networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket, packet::Packet, packets::error_packet::RelayError}, server::{StreamHandle, lobby_handler::LobbyManagerHandle, tcp::tcp_client_handle::TCPClientInstanceHandle}}, util::{ClientCallback, EncryptionState, EncryptionTransport, RELAY_VERSION_BYTES, ServerCallback, noise_params}};
use crate::networking::packethandler::packet::Awaitable;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use snow::Builder;
use crate::networking::packethandler::DynamicPacket;
use std::io::{self};

async fn handle_unsecure(packet: &HandshakeHello, client_handle: &mut TCPClientInstanceHandle, encryption_state: &mut EncryptionState) -> ServerCallback {

    if packet.version != RELAY_VERSION_BYTES {
        let response = RelayError::new(2);
        client_handle.send(AnyPacket::RelayErrorPacket(response)).await;
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

    *encryption_state = EncryptionState::Handshake(responder);

    packet.set_awaitable_client();
    packet.set_nonce(packet.get_nonce());

    
    client_handle.send(AnyPacket::HandshakeAckPacket(packet)).await;
    ServerCallback::Pass

}

#[packet_macro::packet(3, true, true, false, 128, 64, 30)]
pub struct HandshakeHello {
    version: u32,
}

impl HandshakeHello {
    pub async fn on_trigger_server_tcp(&self, client_handle: &mut TCPClientInstanceHandle, encryption_state: &mut EncryptionState, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        match encryption_state {

            EncryptionState::Unsecure => {
                return handle_unsecure(self, client_handle, encryption_state).await;
            },
            _ => ()

        }

        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle) -> ClientCallback{

        
        ClientCallback::Pass
    }
}


#[packet_macro::packet(4, true, true, false, 128, 64, 30)]
pub struct HandshakeAck {
    
}

impl HandshakeAck {
    pub async fn on_trigger_server_tcp(&self, client_handle: &mut TCPClientInstanceHandle, encryption_state: &mut EncryptionState, lobby_manager: &mut LobbyManagerHandle<TCPClientInstanceHandle>) -> ServerCallback {
        
        let old_state = std::mem::replace(encryption_state, EncryptionState::Unsecure);

        if let EncryptionState::Handshake(mut handshake) = old_state {
            let mut scratch = [0u8; 1024];
            let msg3_from_client = self.get_dynamic().as_ref();
            match handshake.read_message(msg3_from_client, &mut scratch) {
                Ok(_n) => {
                     *encryption_state = EncryptionState::Encrypted(handshake.into_transport_mode().unwrap());
                     println!("[Server] Encrypted.");
                }
                Err(e) => println!("{:?}", e),
            }
        }

        
        ServerCallback::Pass
    }

    pub async fn on_trigger_client_tcp(&self, client_handle: &mut TCPClientHandle) -> ClientCallback{
        
        
        ClientCallback::Pass
    }
}