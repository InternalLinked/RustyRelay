use std::{thread, time::{Duration, Instant}};

use crate::networking::{packethandler::{AnyPacket::LobbyBroadcastPacket, packets::lobby::leave::LobbyLeave}, server::udp::UDPServerCore};
use bytes::{Bytes, BytesMut};
use snow::{Builder, params::NoiseParams};
use tokio::{net::{TcpStream, UdpSocket}, runtime::Runtime, sync::mpsc};

use crate::networking::{client::tcp::TCPClientHandle, packethandler::{AnyPacket, packets::{lobby::{create::LobbyCreate, join::LobbyJoin}}}, server::{StreamHandle, tcp::TCPServerCore}};

use std::net::UdpSocket as StdUdpSocket;


mod util;
mod networking;

fn main() {


    //let d = test::bla(12);

    let (sender, receiver) = mpsc::channel::<networking::server::udp::UDPCoreInstruction>(96);


    let listener = StdUdpSocket::bind("127.0.0.1:25565").unwrap();

    UDPServerCore::create(listener, receiver).unwrap();


    thread::park()

}
