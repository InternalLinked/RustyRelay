use std::thread;

use crate::{networking::packethandler::{AnyPacket, packets::{ack::Ack, handshake_packets::{HandshakeAck, HandshakeHello}}}, util::{RELAY_VERSION_BYTES, noise_params}};
use bytes::{Bytes, BytesMut};
use snow::{Builder, params::NoiseParams};
use tokio::{net::{TcpStream, windows::named_pipe::PipeMode::Byte}, runtime::Runtime};

use crate::networking::{client::tcp::TCPClientHandle, server::{StreamHandle, tcp::TCPServerCore}};


mod util;
mod networking;

fn main() {


    //let d = test::bla(12);


    TCPServerCore::run("127.0.0.1:25565".parse().unwrap());

    

    let mut handle = TCPClientHandle::new("127.0.0.1:25565".parse().unwrap()).unwrap();
    
    let params: NoiseParams = noise_params().clone();

    let initiator_keys = Builder::new(params.clone()).generate_keypair().unwrap();

    let mut initiator = Builder::new(params.clone())
        .local_private_key(&initiator_keys.private)
        .unwrap()
        .build_initiator()
        .unwrap();

    let mut encryption = [0u8; 1024];

    let len = initiator.write_message(&[], &mut encryption).unwrap();

    let authpkg = HandshakeHello::new(RELAY_VERSION_BYTES, Bytes::copy_from_slice(&encryption[..len]));




    

    match handle.blocking_send_awaitable(AnyPacket::HandshakeHelloPacket(authpkg)).unwrap() {
        AnyPacket::HandshakeAckPacket(packet) => {
            let server_public_key = packet.get_dynamic();
            let mut buf = [0u8; 1024];
            initiator.read_message(&server_public_key, &mut buf).unwrap(); 

            let mut msg3 = [0u8; 255];
            let len = initiator.write_message(&[], &mut msg3).unwrap();
            let initiator_transport = initiator.into_transport_mode().unwrap();

            let response = HandshakeAck::new(Bytes::copy_from_slice(&msg3[..len]));

            handle.blocking_send(AnyPacket::HandshakeAckPacket(response)).unwrap();
            

        },
        AnyPacket::RelayErrorPacket(packet) => {
            panic!("Error packet with errorcode: {}", packet.read_code());
        }
        _ => panic!("Unexpected"),
    }

    println!("Done");

    

    loop {
        
    }

}
