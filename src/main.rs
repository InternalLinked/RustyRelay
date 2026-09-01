use std::{thread, time::{Duration, Instant}};

use crate::networking::packethandler::packets::lobby::leave::LobbyLeave;
use bytes::{Bytes, BytesMut};
use snow::{Builder, params::NoiseParams};
use tokio::{net::{TcpStream, windows::named_pipe::PipeMode::Byte}, runtime::Runtime};

use crate::networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket, packets::{ack::Ack, lobby::{create::LobbyCreate, join::LobbyJoin}}}, server::{StreamHandle, tcp::TCPServerCore}};


mod util;
mod networking;

fn main() {


    //let d = test::bla(12);


    TCPServerCore::run("127.0.0.1:25565".parse().unwrap());

    

    let mut handle = TCPClientHandle::new("127.0.0.1:25565".parse().unwrap()).unwrap();

    thread::sleep(Duration::from_millis(1000));
    println!("Sending");

    let packet = LobbyCreate::new();

    let start = Instant::now();
    let code: u64 = match handle.blocking_send_awaitable(AnyPacket::LobbyCreatePacket(packet)).unwrap() {
        AnyPacket::LobbyCreateSuccessPacket(success) => {
            let bytes = success.read_lobby_code().to_le_bytes();
            let code = str::from_utf8(&bytes).unwrap();

            println!("Created Lobby successfull!\n LobbyID: {},\n Lobby_Code: {}", success.read_lobby_id(), code);
            success.read_lobby_code() as u64

        },
        AnyPacket::RelayErrorPacket(code) => panic!("got error {}", code.read_code()),
        _ => panic!("Unexpected packet"),
    };

    let duration = start.elapsed();
    println!("Time taken: {} ms", duration.as_millis());

    let mut handle = TCPClientHandle::new("127.0.0.1:25565".parse().unwrap()).unwrap();

    let packet = LobbyJoin::new(code);


    match handle.blocking_send_awaitable(AnyPacket::LobbyJoinPacket(packet)).unwrap() {

        AnyPacket::LobbyJoinSuccessPacket(packet) => {

            println!("Joined lobby:\nLobby_Id: {}, Seat_Id: {}", packet.read_lobby_id(), packet.read_seat_id());



        }
        AnyPacket::RelayErrorPacket(code) => panic!("got error {}", code.read_code()),
        _ => panic!("Unexpected packet"),
    } 


    let mut handle = TCPClientHandle::new("127.0.0.1:25565".parse().unwrap()).unwrap();

    let packet = LobbyJoin::new(code);


    match handle.blocking_send_awaitable(AnyPacket::LobbyJoinPacket(packet)).unwrap() {

        AnyPacket::LobbyJoinSuccessPacket(packet) => {

            println!("Joined lobby:\nLobby_Id: {}, Seat_Id: {}", packet.read_lobby_id(), packet.read_seat_id());



        }
        AnyPacket::RelayErrorPacket(code) => panic!("got error {}", code.read_code()),
        _ => panic!("Unexpected packet"),
    } 



    let packet = LobbyLeave::new();

    let _ = handle.blocking_send(AnyPacket::LobbyLeavePacket(packet));


    thread::sleep(Duration::from_secs(1));

    let packet = LobbyJoin::new(code);

    match handle.blocking_send_awaitable(AnyPacket::LobbyJoinPacket(packet)).unwrap() {

        AnyPacket::LobbyJoinSuccessPacket(packet) => {

            println!("Joined lobby:\nLobby_Id: {}, Seat_Id: {}", packet.read_lobby_id(), packet.read_seat_id());



        }
        AnyPacket::RelayErrorPacket(code) => panic!("got error {}", code.read_code()),
        _ => panic!("Unexpected packet"),
    } 

    handle.close();

    println!("Done");

    loop {
        
    }

}
