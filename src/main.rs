use std::{thread, time::{Duration, Instant}};

use crate::networking::packethandler::AnyPacket;
use crate::networking::packethandler::packets::lobby::create::CreateLobby;
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

    thread::sleep(Duration::from_millis(1000));
    println!("Sending");

    let packet = CreateLobby::new();

    match handle.blocking_send_awaitable(AnyPacket::CreateLobbyPacket(packet)).unwrap() {
        AnyPacket::LobbyCreateSuccessPacket(success) => {
            let bytes = success.read_lobby_code().to_le_bytes();
            let code = str::from_utf8(&bytes).unwrap();

            println!("Created Lobby successfull!\n LobbyID: {},\n Lobby_Code: {}", success.read_lobby_id(), code);
        },
        AnyPacket::RelayErrorPacket(code) => println!("got error {}", code.read_code()),
        _ => panic!("Unexpected packet"),
    }



    let mut handle = TCPClientHandle::new("127.0.0.1:25565".parse().unwrap()).unwrap();

    thread::sleep(Duration::from_millis(1000));
    println!("Sending");

    let packet = CreateLobby::new();

    match handle.blocking_send_awaitable(AnyPacket::CreateLobbyPacket(packet)).unwrap() {
        AnyPacket::LobbyCreateSuccessPacket(success) => {
            let bytes = success.read_lobby_code().to_le_bytes();
            let code = str::from_utf8(&bytes).unwrap();

            println!("Created Lobby successfull!\n LobbyID: {},\n Lobby_Code: {}", success.read_lobby_id(), code);
        },
        AnyPacket::RelayErrorPacket(code) => println!("got error {}", code.read_code()),
        _ => panic!("Unexpected packet"),
    }

    println!("Done");

    loop {
        
    }

}
