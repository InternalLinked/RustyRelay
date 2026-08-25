use bytes::{Buf, Bytes, BytesMut};
use slab::Slab;
use snow::Builder;
use tokio::io::AsyncWriteExt;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::networking::packethandler::packets::error_packet::RelayError;
use crate::networking::packethandler::packets::handshake_packets::HandshakeAck;
use crate::util::{ClientContextInstruction, EncryptionTransport, RELAY_VERSION_BYTES, noise_params};
use crate::util::EncryptionState::{Handshake, Unsecure};
use crate::{networking::{packethandler::{ AnyPacket, TCPServerPacketHandler, packet::{MAX_TCP_LENGTH, Packet}, packets::ack::Ack}, server::{StreamHandle, lobby_handler::LobbyManagerHandle}}, util::{EncryptionState, LobbyState, ServerCallback}};

#[derive(Clone)]
pub struct TCPClientInstanceHandle {
    sender: Sender<ClientContextInstruction>,
    id: usize,
    pub counter: usize, 
}

impl TCPClientInstanceHandle {
    pub async fn new(stream: TcpStream, id: usize, lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>) -> Self {
        TCPClientContext::instantiate(stream, id, lobby_manager).await
    }

}

impl StreamHandle for TCPClientInstanceHandle {

    async fn send(&mut self, packet: AnyPacket) {
        if self.sender.send(ClientContextInstruction::Packet(packet)).await.is_err() {
            todo!("Handle closed client");
        }
    }

    async fn send_awaitable(&mut self, anypacket: AnyPacket) -> io::Result<AnyPacket> {
        let (sender, r) = oneshot::channel::<AnyPacket>();


        if self.sender.send(ClientContextInstruction::Awaitable{anypacket, sender}).await.is_err() {
            todo!("Handle closed server");
        }
        r.await.map_err(|e: oneshot::error::RecvError| io::Error::new(io::ErrorKind::Other, e))
    }

    async fn close(&mut self) {
        if self.sender.send(ClientContextInstruction::Close).await.is_err() {
            todo!("Handle closed server");
        }
    }

    fn get_id(&self) -> usize {
        self.id
    }
}

struct TCPClientContext {
    read_half: OwnedReadHalf,
    write_half: OwnedWriteHalf,
    receiver: Receiver<ClientContextInstruction>,
    client_handle: TCPClientInstanceHandle,
    lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>,
    awaitable_cache: Slab<oneshot::Sender<AnyPacket>>
}

impl TCPClientContext {
    pub async fn instantiate(stream: TcpStream, id: usize, lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>) -> TCPClientInstanceHandle {
        let (sender, receiver) = mpsc::channel::<ClientContextInstruction>(32);
        let client_handle = TCPClientInstanceHandle { sender, id, counter: 0 };

        let (read_half, write_half) = stream.into_split();

        let mut context = TCPClientContext {
            read_half,
            write_half,
            receiver,
            client_handle: client_handle.clone(),
            lobby_manager,
            awaitable_cache: Slab::new(),
        };

        tokio::spawn(async move {
            context.run().await;
            let t = EncryptionState::Unsecure;
            match t {
                Unsecure => print!("!"),
                _ => {}
            }
        });
        client_handle
    }

    pub async fn run(&mut self) {
        /* Really ugly code here but rusts burrowchecker forces me to do this */

        let mut encrypiton_state = EncryptionState::Unsecure;

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

                    TCPServerPacketHandler::handle_packet(&mut read_buffer, &mut self.client_handle, &mut encrypiton_state, &mut self.lobby_manager, &mut self.awaitable_cache).await;

                }
                instruction = self.receiver.recv() => {
                    println!("Got instruction");
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

                            println!("sending with key {}", key);

                            let (mut fixed, dynamic) = anypacket.encode_awaitable_server(&mut write_buffer, key as u64).unwrap();
                            
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