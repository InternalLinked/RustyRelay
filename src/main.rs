use std::{thread, time::Duration};

use bytes::{Bytes, BytesMut};
use snow::{Builder, params::NoiseParams};
use tokio::{net::{TcpStream, windows::named_pipe::PipeMode::Byte}, runtime::Runtime};

use crate::networking::{client::tcp::TCPClientHandle, packethandler::packets::ack::Ack, server::{StreamHandle, tcp::TCPServerCore}};


mod util;
mod networking;

fn main() {


    //let d = test::bla(12);


    TCPServerCore::run("127.0.0.1:25565".parse().unwrap());

    

    let mut handle = TCPClientHandle::new("127.0.0.1:25565".parse().unwrap()).unwrap();
    
    let payload = Bytes::copy_from_slice(b"test");

    let pkg = Ack::new(12, payload);
    //thread::sleep(Duration::from_millis(1000));
    handle.blocking_send(networking::packethandler::AnyPacket::AckPacket(pkg)).unwrap();

    println!("Done");

    loop {
        
    }

}
