use std::io::Error;

use bytes::{Buf, Bytes, BytesMut};
use slab::Slab;
use snow::{Builder, error};
use tokio::io::AsyncWriteExt;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::networking::client::tcp::TCPClientHandle;
use crate::networking::encryption_layer::{ClientEncryptionLayer, EncryptionHandler, ServerEncryptionLayer};
use crate::networking::frame_reader::{FramedReader, FramedWriter};
use crate::networking::packethandler::packets::error_packet::RelayError;
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

    async fn send(&mut self, anypacket: AnyPacket) {
        if self.sender.send(ClientContextInstruction::Packet {anypacket}).await.is_err() {
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


pub struct TCPClientInstanceCore {

    client_handle: TCPClientInstanceHandle,
    awaitable_cache: Slab<oneshot::Sender<AnyPacket>>,

}

impl TCPClientInstanceCore {
    pub fn new(client_handle: TCPClientInstanceHandle) -> Self {

        Self {client_handle, awaitable_cache: Slab::new()}
    }

    pub fn get_client_handle(&mut self) -> &mut TCPClientInstanceHandle {
        &mut self.client_handle
    }

    pub fn get_awaitable_cache(&mut self) -> &mut Slab<oneshot::Sender<AnyPacket>> {
        &mut self.awaitable_cache
    }

}

struct TCPClientContext {
    receiver: Receiver<ClientContextInstruction>,
    lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>,
}


impl TCPClientContext {
    pub async fn instantiate(stream: TcpStream, id: usize, lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>) -> TCPClientInstanceHandle {
        let (sender, receiver) = mpsc::channel::<ClientContextInstruction>(32);
        let client_handle = TCPClientInstanceHandle { sender, id, counter: 0 };

        let (read_half, write_half, encryption_handler) = ServerEncryptionLayer::new(stream).await.unwrap();

        let mut context = TCPClientContext {
            receiver,
            lobby_manager,
        };


        let client_handle_clone  = client_handle.clone();

        tokio::spawn(async move {
            context.run(client_handle_clone, read_half, write_half, encryption_handler).await;
        });
        client_handle
    }

    pub async fn run(&mut self, client_handle: TCPClientInstanceHandle, mut read_half: FramedReader<OwnedReadHalf>, mut write_half: FramedWriter<OwnedWriteHalf>, mut encryption_handler: EncryptionHandler) {
        /* Really ugly code here but rusts burrowchecker forces me to do this */

        let mut write_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        let mut read_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        let mut core = TCPClientInstanceCore::new(client_handle);

        loop {
             tokio::select! {
                callback = read_half.read_frame() => {

                    let Ok(buffer) = callback else {
                        break;
                    };

                    let Ok(len) = encryption_handler.decrypt(&buffer, &mut read_buffer) else {
                        println!("[Server] Decryption error");
                        break;
                    };

                    match TCPServerPacketHandler::handle_packet(&mut read_buffer.split_to(len), &mut core, &mut self.lobby_manager).await {
                        ServerCallback::Kill => {
                            println!("[Server] Handler instructed to kill.");
                            break;
                        },
                        _ => ()
                    }

                }

                instruction = self.receiver.recv() => {
                    let Some(instruction) = instruction else {
                        break;
                    };
                    
                    match instruction {
                        ClientContextInstruction::Packet {mut anypacket} => {
                            
                            let encoded = anypacket.encode(&mut write_buffer).unwrap();
                            //write_buffer.resize(MAX_TCP_LENGTH, 0);
                            let Ok(len) = encryption_handler.encrypt(&encoded, &mut write_buffer) else {
                                println!("[Client] Encryption error.");
                                break;
                            };

                            let Ok(()) = write_half.write_frame(&mut write_buffer.split_to(len).freeze()).await else {
                                println!("[Client] Write error");
                                break;
                            };
                            

                        },
                        

                        ClientContextInstruction::Awaitable {mut anypacket, sender} => {
                            let key = core.awaitable_cache.insert(sender);

                            let encoded = anypacket.encode_awaitable_client(&mut write_buffer, key as u64).unwrap();
                            //write_buffer.resize(MAX_TCP_LENGTH, 0);
                            let Ok(len) = encryption_handler.encrypt(&encoded, &mut write_buffer) else {
                                println!("[Client] Encryption error.");
                                break;
                            };

                            let Ok(()) = write_half.write_frame(&mut write_buffer.split_to(len).freeze()).await else {
                                println!("[Client] Write error");
                                break;
                            };

                        }

                        ClientContextInstruction::Close => {
                            break;
                        }
                    }
                }

            }
        }

        println!("[Client] Close");
        
    }
}