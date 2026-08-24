use std::net::SocketAddr;
use std::thread;

use bytes::{Buf, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::networking::packethandler::TCPClientPacketHandler;
use crate::util::{ClientCallback, client_runtime};
use crate::{networking::{packethandler::{ AnyPacket, TCPServerPacketHandler, packet::{MAX_TCP_LENGTH, Packet}, packets::ack::Ack}, server::{StreamHandle, lobby_handler::LobbyManagerHandle}}, util::{EncryptionState}};

enum ClientContextInstruction {
    Close, 
    AnyPacket(AnyPacket)
}

#[derive(Clone)]
pub struct TCPClientHandle {
    sender: Sender<ClientContextInstruction>
}

impl StreamHandle for TCPClientHandle {

    async fn send(&mut self, packet: AnyPacket) {
        if self.sender.send(ClientContextInstruction::AnyPacket(packet)).await.is_err() {
            todo!("Handle closed client");
        }
    }

    async fn close(&mut self) {
        if self.sender.send(ClientContextInstruction::Close).await.is_err() {
            todo!("Handle closed client");
        }
    }

    fn get_id(&self) -> usize {
        0
    }
}

impl TCPClientHandle {
    pub fn new(address: SocketAddr) -> io::Result<TCPClientHandle> {
        let (sender, receiver) = oneshot::channel::<io::Result<TCPClientHandle>>();



        client_runtime().spawn(async move {
        
        
            match TcpStream::connect(address).await {

                Ok(stream) => {
                    let handle = TCPClientContext::instantiate(stream).await;
                    let _ = sender.send(Ok(handle));
                },
                Err(err) => {
                    let _ = sender.send(Err(err));
                }


            }
            

        });
        

        receiver
            .blocking_recv()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
    }

    pub fn send(&mut self, packet: AnyPacket) -> io::Result<()> {
        self.sender.blocking_send(ClientContextInstruction::AnyPacket(packet))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn close(&mut self) -> io::Result<()>{
        self.sender.blocking_send(ClientContextInstruction::Close).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

}


struct TCPClientContext {
    read_half: OwnedReadHalf,
    write_half: OwnedWriteHalf,
    receiver: Receiver<ClientContextInstruction>,
    encryption_state: EncryptionState,
    client_handle: TCPClientHandle,
}

impl TCPClientContext {
    pub async fn instantiate(stream: TcpStream) -> TCPClientHandle {
        let (sender, receiver) = mpsc::channel::<ClientContextInstruction>(32);
        let client_handle = TCPClientHandle { sender };

        let (read_half, write_half) = stream.into_split();

        let mut context = TCPClientContext {
            read_half,
            write_half,
            receiver,
            encryption_state: EncryptionState::Unsecure,
            client_handle: client_handle.clone(),
        };

        tokio::spawn(async move {
            context.run().await;
        });
        client_handle
    }

    pub async fn run(&mut self) {

        tokio::select! {
            _callback = Self::handle_packet_event(&mut self.read_half, &mut self.client_handle) => {

            }
            _ = Self::handle_write_event(&mut self.write_half, &mut self.receiver) => {
                // channel closed (all ClientHandles dropped)
                println!("channel")
            }
        }
        println!("Close");
        
    }

    async fn handle_packet_event(read_half: &mut OwnedReadHalf, client_handle: &mut TCPClientHandle) -> ClientCallback {
        let mut read_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        loop {
            read_buffer.reserve(MAX_TCP_LENGTH);

            match read_half.read_buf(&mut read_buffer).await {
                Ok(0) => return ClientCallback::Kill,
                Ok(_n) => {
                    // parse packets out of read_buffer here
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return ClientCallback::Error(e),
            }

            match TCPClientPacketHandler::handle_packet(&mut read_buffer, client_handle).await {
                 ClientCallback::Pass => continue,
                 other => return other,
            }
        }
    }


    async fn handle_write_event(
        write_half: &mut OwnedWriteHalf,
        receiver: &mut Receiver<ClientContextInstruction>,
    ) {
        let mut write_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);

        loop {

            if let Some(instruction) = receiver.recv().await {
                match instruction {
                    ClientContextInstruction::AnyPacket(mut packet) => {
                        let (mut fixed, dynamic) = packet.encode(&mut write_buffer).unwrap();


                        match dynamic {
                            Some(payload) => {
                                let mut combined = fixed.chain(payload);
                                if write_half.write_all_buf(&mut combined).await.is_err() {
                                    break;
                                }
                            }
                            None => {
                                if write_half.write_all_buf(&mut fixed).await.is_err() {
                                    break;
                                }
                            }
                        }
                    },
                    ClientContextInstruction::Close => {
                        return;
                    }
                }

            }

        }

    }
}