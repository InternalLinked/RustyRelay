use std::{thread, time::{Duration, Instant}};

use crate::networking::packethandler::{AnyPacket::LobbyBroadcastPacket, packets::lobby::leave::LobbyLeave};
use crate::networking::packethandler::packets::lobby::kick::LobbyKick;
use crate::networking::packethandler::packets::lobby::broadcast::LobbyBroadcast;
use crate::networking::client::RelayClient;
use bytes::{Bytes, BytesMut};
use snow::{Builder, params::NoiseParams};
use tokio::{net::{TcpStream, windows::named_pipe::PipeMode::Byte}, runtime::Runtime};

use crate::networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket, packets::{ack::Ack, lobby::{create::LobbyCreate, join::LobbyJoin}}}, server::{StreamHandle, tcp::TCPServerCore}};


mod util;
mod networking;

fn main() {


    //let d = test::bla(12);


    TCPServerCore::run("127.0.0.1:25565".parse().unwrap());

    

    let mut host = RelayClient::connect("127.0.0.1:25565").unwrap();
    let code  = host.create_lobby().unwrap();

    println!("Lobby code: {code}");

    println!("Done");

    loop {
        
    }

}
