use std::net::SocketAddr;
use std::thread;

use bytes::{Buf, BytesMut};
use slab::Slab;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::networking::packethandler::TCPClientPacketHandler;
use crate::util::{ClientCallback, ClientContextInstruction, client_runtime};
use crate::{networking::{packethandler::{ AnyPacket, TCPServerPacketHandler, packet::{MAX_TCP_LENGTH, Packet}, packets::ack::Ack}, server::{StreamHandle, lobby_handler::LobbyManagerHandle}}, util::{EncryptionState}};

#[derive(Clone)]
pub struct TCPClientHandle {
    sender: Sender<ClientContextInstruction>
}

impl StreamHandle for TCPClientHandle {

    async fn send(&mut self, packet: AnyPacket) {
        if self.sender.send(ClientContextInstruction::Packet(packet)).await.is_err() {
            todo!("Handle closed client");
        }
    }

    async fn send_awaitable(&mut self, anypacket: AnyPacket) -> io::Result<AnyPacket> {
        let (sender, r) = oneshot::channel::<AnyPacket>();


        if self.sender.send(ClientContextInstruction::Awaitable{anypacket, sender}).await.is_err() {
            todo!("Handle closed client");
        }
        r.await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))
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

    pub fn blocking_send(&mut self, packet: AnyPacket) -> io::Result<()> {
        self.sender.blocking_send(ClientContextInstruction::Packet(packet))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn blocking_send_awaitable(&mut self, anypacket: AnyPacket) -> io::Result<AnyPacket> {
        let (sender, r) = oneshot::channel::<AnyPacket>();


        if self.sender.blocking_send(ClientContextInstruction::Awaitable{anypacket, sender}).is_err() {
            todo!("Handle closed client");
        }
        r.blocking_recv().map_err(|e| io::Error::new(io::ErrorKind::Other, e))
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
    awaitable_cache: Slab<oneshot::Sender<AnyPacket>>
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
            awaitable_cache: Slab::new(),
        };

        tokio::spawn(async move {
            context.run().await;
        });
        client_handle
    }

    pub async fn run(&mut self) {
        /* Really ugly code here but rusts burrowchecker forces me to do this */

        let mut write_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        let mut read_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);


        loop {
             tokio::select! {
                callback = Self::read_buffer(&mut self.read_half, &mut read_buffer) => {

                    match callback {
                        Ok(0) => break,
                        Ok(_n) => {
                            // parse packets out of read_buffer here
                        }
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => {
                            println!("Client error {e}");
                            break;
                        },
                    }


                    
                    
                    TCPClientPacketHandler::handle_packet(&mut read_buffer, &mut self.client_handle, &mut self.awaitable_cache).await;


                }
                instruction = self.receiver.recv() => {

                    let Some(instruction) = instruction else {
                        break;
                    };

                    match instruction {
                        ClientContextInstruction::Packet(mut packet) => {
                            let (mut fixed, dynamic) = packet.encode(&mut write_buffer).unwrap();


                            match dynamic {
                                Some(payload) => {
                                    let mut combined = fixed.chain(payload);
                                    if self.write_half.write_all_buf(&mut combined).await.is_err() {
                                        break;
                                    }
                                }
                                None => {
                                    if self.write_half.write_all_buf(&mut fixed).await.is_err() {
                                        break;
                                    }
                                }
                            }
                        },

                        ClientContextInstruction::Awaitable {mut anypacket, sender} => {
                            let key = self.awaitable_cache.insert(sender);

                            let (mut fixed, dynamic) = anypacket.encode_awaitable_client(&mut write_buffer, key as u64).unwrap();
                            
                            match dynamic {
                                Some(payload) => {
                                    let mut combined = fixed.chain(payload);
                                    if self.write_half.write_all_buf(&mut combined).await.is_err() {
                                        break;
                                    }
                                }
                                None => {
                                    if self.write_half.write_all_buf(&mut fixed).await.is_err() {
                                        break;
                                    }
                                }
                            }

                        }

                        ClientContextInstruction::Close => {
                            return;
                        }
                    }
                }
            }
        }

        println!("[tcp_client] Close");
        
    }

    async fn read_buffer(read_half: &mut OwnedReadHalf, buffer: &mut BytesMut) -> Result<usize, std::io::Error> {
        buffer.reserve(MAX_TCP_LENGTH);
        read_half.read_buf(buffer).await
    }
    
}