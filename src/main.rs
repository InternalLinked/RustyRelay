use std::thread;

use crate::networking::packethandler::{AnyPacket, packets::ack::Ack};
use bytes::Bytes;
use tokio::{net::TcpStream, runtime::Runtime};

use crate::networking::{client::tcp::TCPClientHandle, server::{StreamHandle, tcp::TCPServerCore}};


mod util;
mod networking;

fn main() {


    //let d = test::bla(12);


    TCPServerCore::run("127.0.0.1:25565".parse().unwrap());

    

    let mut handle = TCPClientHandle::new("127.0.0.1:25565".parse().unwrap()).unwrap();

    let dna = Bytes::copy_from_slice(b"test");


    let pkg = Ack::new(1, dna.clone());

    println!("sending");
    handle.blocking_send(AnyPacket::AckPacket(pkg));
        let dna = Bytes::copy_from_slice(b"test");

    println!("Done");

    

    loop {
        
    }

}
