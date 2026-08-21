use std::{any, net::SocketAddr};

use arc_swap::ArcSwap;
use bytes::BytesMut;
use tokio::{io::{self, AsyncReadExt, AsyncWriteExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::{networking::packethandler::{ PacketResponse, ServerPacketHandler, packet::{MAX_TCP_LENGTH, Packet}, packets::ack_packet::AckPacket}, util::{EncryptionState, LobbyState, ServerCallback}};


pub enum PacketRequest {
    LosePacket(Box<dyn Packet>),
    AwaitPacket(Box<dyn Packet>, oneshot::Sender<PacketResponse>),
}


pub struct ClientInstance {
    pub(crate) encryption: EncryptionState,
    pub(crate) lobby_state: LobbyState,
}



#[derive(Clone)]
pub struct ClientHandle {
    sender: Sender<Box<dyn Packet + Send>>,
    id: usize
}

impl ClientHandle {
    pub async fn new(stream: TcpStream, id: usize) -> Self {
        println!("Instantiate");
        ClientContext::instantiate(stream, id).await
    }

    pub async fn send(&mut self, packet: impl Packet + 'static) {
        if self.sender.send(Box::new(packet)).await.is_err() {
            todo!("Handle closed client");
        }
    }

    pub fn get_id(&self) -> usize {
        self.id
    }
}

struct ClientContext {
    read_half: OwnedReadHalf,
    write_half: OwnedWriteHalf,
    receiver: Receiver<Box<dyn Packet + Send>>,
    encryption_state: EncryptionState,
    client_handle: ClientHandle,
}

impl ClientContext {
    pub async fn instantiate(stream: TcpStream, id: usize) -> ClientHandle {
        let (sender, receiver) = mpsc::channel::<Box<dyn Packet + Send>>(32);
        let client_handle = ClientHandle { sender, id };

        let (read_half, write_half) = stream.into_split();

        let mut context = ClientContext {
            read_half,
            write_half,
            receiver,
            encryption_state: EncryptionState::Unsecure,
            client_handle: client_handle.clone()
        };

        tokio::spawn(async move {
            context.run().await;
        });
        client_handle
    }

    pub async fn run(&mut self) {

        println!("Running loop");
        tokio::select! {
            _callback = Self::handle_packet_event(&mut self.read_half, &mut self.client_handle) => {
                
            }
            _ = Self::handle_write_event(&mut self.write_half, &mut self.receiver) => {
                println!("Close");
                // channel closed (all ClientHandles dropped)
            }
        }
    }

    async fn handle_packet_event(read_half: &mut OwnedReadHalf, client_handle: &mut ClientHandle) -> ServerCallback {
        println!("Handling packet");
        let mut read_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        loop {
            read_buffer.reserve(MAX_TCP_LENGTH);

            match read_half.read_buf(&mut read_buffer).await {
                Ok(0) => return ServerCallback::Kill,
                Ok(_n) => {
                    // parse packets out of read_buffer here
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return ServerCallback::Error(e),
            }

            match ServerPacketHandler::handle_packet(&mut read_buffer, client_handle) {
                 ServerCallback::Pass => continue,
                 other => return other,
            }
        }
    }

    async fn handle_write_event(
        write_half: &mut OwnedWriteHalf,
        receiver: &mut Receiver<Box<dyn Packet + Send>>,
    ) {
        let mut write_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);

        loop {

            if let Some(mut packet) = receiver.recv().await {
                let encoded = packet.encode(&mut write_buffer).unwrap();
                if write_half.write_all(&encoded).await.is_err() {
                    break;
                }
            }

        }

    }
}