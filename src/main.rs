use std::{thread, time::{Duration, Instant}};

use crate::networking::packethandler::{AnyPacket::LobbyBroadcastPacket, packets::lobby::leave::LobbyLeave};
use crate::networking::packethandler::packets::lobby::kick::LobbyKick;
use crate::networking::packethandler::packets::lobby::broadcast::LobbyBroadcast;
use crate::networking::client::RelayClient;
use crate::networking::client::RelayClientCallback;
use bytes::{Bytes, BytesMut};
use snow::{Builder, params::NoiseParams};
use tokio::{net::{TcpStream, windows::named_pipe::PipeMode::Byte}, runtime::Runtime};

use crate::networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket, packets::{lobby::{create::LobbyCreate, join::LobbyJoin}}}, server::{StreamHandle, tcp::TCPServerCore}};


mod util;
mod networking;

fn main() {


    //let d = test::bla(12);


    TCPServerCore::run("127.0.0.1:25565".parse().unwrap());

    

    let mut host = RelayClient::connect("127.0.0.1:25565").unwrap();
    let code  = host.create_lobby().unwrap();

    println!("Lobby code: {code}");

    thread::spawn(move || {
        let mut x: u64 = 0;
        loop {
            match host.poll_event().unwrap() {

                RelayClientCallback::LobbyJoined { author_id } => {
                    println!("Host; Client joined: {author_id}");
                    if x >= 1 {
                        host.kick_client(author_id).unwrap();
                    }

                    x += 1;            
                }

                RelayClientCallback::LobbyLeft { author_id } => {
                    println!("Host; Client left: {author_id}");
                }

                _ => ()
            }
        }

    });

    
    let mut client1 = RelayClient::connect("127.0.0.1:25565").unwrap();

    client1.join_lobby(code.clone()).unwrap();

    thread::spawn(move || {

        loop {
            match client1.poll_event().unwrap() {

                RelayClientCallback::LobbyJoined { author_id } => {
                    println!("Client joined: {author_id}");         
                }

                RelayClientCallback::LobbyLeft { author_id } => {
                    println!("Client left: {author_id}");
                }

                _ => ()
            }
        }

    });

        
    let mut client2 = RelayClient::connect("127.0.0.1:25565").unwrap();

    client2.join_lobby(code).unwrap();

    thread::spawn(move || {

        loop {
            match client2.poll_event().unwrap() {

                RelayClientCallback::LobbyJoined { author_id } => {
                    println!("Client2 joined: {author_id}")              
                }

                RelayClientCallback::LobbyLeft { author_id } => {
                    println!("Client2 left: {author_id}");
                }

                _ => ()
            }
        }

    });

    loop {
        
    }

}
