use std::io::Error;

use bytes::{Buf, Bytes, BytesMut};
use slab::Slab;
use snow::{Builder, error};
use tokio::io::AsyncWriteExt;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::networking::client::tcp::TCPClientHandle;
use crate::networking::encryption_layer::{ClientEncryptionLayer, ServerEncryptionLayer};
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
    read_half: FramedReader<OwnedReadHalf>,
    write_half: FramedWriter<OwnedWriteHalf>,
    receiver: Receiver<ClientContextInstruction>,
    lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>,
}


impl TCPClientContext {
    pub async fn instantiate(stream: TcpStream, id: usize, lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>) -> TCPClientInstanceHandle {
        let (sender, receiver) = mpsc::channel::<ClientContextInstruction>(32);
        let client_handle = TCPClientInstanceHandle { sender, id, counter: 0 };

        ServerEncryptionLayer::new(stream).await.unwrap();

        /*let mut context = TCPClientContext {
            read_half,
            write_half,
            receiver,
            lobby_manager,
        };*/


        let client_handle_clone  = client_handle.clone();

        /*tokio::spawn(async move {
            context.run(client_handle_clone).await;
            let t = EncryptionState::Unsecure;
            match t {
                Unsecure => print!("!"),
                _ => {}
            }
        });*/
        client_handle
    }

    pub async fn run(&mut self, client_handle: TCPClientInstanceHandle) {
        /* Really ugly code here but rusts burrowchecker forces me to do this */

        let mut write_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        let mut read_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        
        let mut core = TCPClientInstanceCore::new(client_handle);

        /*loop {
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

                    TCPServerPacketHandler::handle_packet(&mut read_buffer, &mut core, &mut self.lobby_manager).await;

                }
                instruction = self.receiver.recv() => {
                    let Some(instruction) = instruction else {
                        break;
                    };
                    
                    match instruction {
                        ClientContextInstruction::Packet {mut anypacket, encrypt} => {
                            let mut buffer = anypacket.encode(&mut write_buffer).unwrap();

                            if encrypt && let EncryptionState::Encrypted(_) = &mut core.get_encryption_handler().encryption_state {

                                let mut enc_packet = SecureBatch::new(core.get_encryption_handler().encrypt(&buffer).unwrap());
                                
                                buffer = enc_packet.encode(&mut write_buffer).unwrap();
                                
                                
                            }
                            if self.write_half.write_all(&mut buffer).await.is_err() {
                                break;
                            }
                            

                        },
                        

                        ClientContextInstruction::Awaitable {mut anypacket, sender} => {
                            let key = core.get_awaitable_cache().insert(sender);

                            let mut buffer = anypacket.encode_awaitable_client(&mut write_buffer, key as u64).unwrap();
                            
                            if let EncryptionState::Encrypted(_) = &mut core.get_encryption_handler().encryption_state {

                                let mut enc_packet = SecureBatch::new(core.get_encryption_handler().encrypt(&buffer).unwrap());
                                
                                buffer = enc_packet.encode(&mut write_buffer).unwrap();
                                
                                
                            }
                                                        
                            if self.write_half.write_all(&mut buffer).await.is_err() {
                                break;
                            }

                        }

                        ClientContextInstruction::Close => {
                            return;
                        }
                    }
                }
            }
        }

        println!("[tcp_client] Close");*/
        
    }

    async fn read_buffer(read_half: &mut OwnedReadHalf, buffer: &mut BytesMut) -> Result<usize, std::io::Error> {
        buffer.reserve(MAX_TCP_LENGTH);
        read_half.read_buf(buffer).await
    }
}