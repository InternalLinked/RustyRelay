use std::error::Error;
use std::{fmt, net::SocketAddr};
use std::thread;

use bytes::{Buf, Bytes, BytesMut, buf};
use slab::Slab;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::networking::client::{RelayClientCallback, RelayHandleError};
use crate::networking::encryption_layer::{ClientEncryptionLayer, EncryptionHandler};
use crate::networking::frame_reader::{FramedReader, FramedWriter};
use crate::networking::packethandler::TCPClientPacketHandler;
use crate::networking::server::lobby_handler::LobbyState;
use crate::util::{ClientCallback, ClientContextInstruction, RELAY_VERSION_BYTES, client_runtime, noise_params};
use crate::{networking::{packethandler::{ AnyPacket, TCPServerPacketHandler, packet::{MAX_TCP_LENGTH, Packet}}, server::{StreamHandle, lobby_handler::LobbyManagerHandle}}, util::{EncryptionState}};


pub struct TCPClientCore {

    client_handle: TCPClientHandle,
    pub relay_callback_writer: Sender<RelayClientCallback>,
    pub lobby_state: LobbyState

}

impl TCPClientCore {
    pub fn new(client_handle: TCPClientHandle, relay_callback_writer: Sender<RelayClientCallback>) -> Self {

        Self {client_handle, lobby_state: LobbyState::Alone, relay_callback_writer}
    }

    pub fn get_client_handle(&mut self) -> &mut TCPClientHandle {
        &mut self.client_handle
    }

}


#[derive(Clone)]
pub struct TCPClientHandle {
    sender: Sender<ClientContextInstruction>
}

impl StreamHandle for TCPClientHandle {

    async fn send(&self, anypacket: AnyPacket) {
        if self.sender.send(ClientContextInstruction::Packet {anypacket}).await.is_err() {
            todo!("Handle closed client");
        }
    }

    async fn send_raw(&self, packet: Bytes) {
        if self.sender.blocking_send(ClientContextInstruction::RawPacket { raw_packet: packet }).is_err() {
            todo!("Handle closed client");
        }
    }

    async fn send_awaitable(&self, anypacket: AnyPacket) -> io::Result<AnyPacket> {
        let (sender, r) = oneshot::channel::<AnyPacket>();


        if self.sender.send(ClientContextInstruction::Awaitable{anypacket, sender}).await.is_err() {
            todo!("Handle closed client");
        }
        r.await.map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    async fn close(&self) {
        if self.sender.send(ClientContextInstruction::Close).await.is_err() {
            todo!("Handle closed client");
        }
    }

    fn get_id(&self) -> usize {
        0
    }
}



impl TCPClientHandle {
    pub fn new(address: SocketAddr, relay_callback_writer: Sender<RelayClientCallback>) -> Result<TCPClientHandle, RelayHandleError> {
        let (sender, receiver) = oneshot::channel::<Result<TCPClientHandle, RelayHandleError>>();

        client_runtime().spawn(async move {
        
        
            match TcpStream::connect(address).await {

                Ok(stream) => {
                    let handle = TCPClientContext::instantiate(stream, relay_callback_writer).await;
                    let _ = sender.send(handle);
                },
                Err(_) => {
                    let _ = sender.send(Err(RelayHandleError::ServerRefusedConnection));
                }


            }
            

        });
        

        receiver
            .blocking_recv()
            .map_err(|_| RelayHandleError::PipeError)?
    }

    pub fn blocking_send(&mut self, anypacket: AnyPacket) -> io::Result<()> {
        self.sender.blocking_send(ClientContextInstruction::Packet {anypacket})
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn blocking_send_awaitable(&mut self, anypacket: AnyPacket) -> io::Result<AnyPacket> {
        let (sender, r) = oneshot::channel::<AnyPacket>();


        if self.sender.blocking_send(ClientContextInstruction::Awaitable{anypacket, sender}).is_err() {
            todo!("Handle closed client");
        }
        r.blocking_recv().map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    pub fn blocking_close(&mut self) -> io::Result<()>{
        self.sender.blocking_send(ClientContextInstruction::Close).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

}


struct TCPClientContext {
    receiver: Receiver<ClientContextInstruction>,
    awaitable_cache: Slab<oneshot::Sender<AnyPacket>>,
}

impl TCPClientContext {
    pub async fn instantiate(stream: TcpStream, relay_callback_writer: Sender<RelayClientCallback>) -> Result<TCPClientHandle, RelayHandleError>{
        let (sender, receiver) = mpsc::channel::<ClientContextInstruction>(96);
        let client_handle = TCPClientHandle { sender };

        let (read_half, write_half, encryption_handler) = ClientEncryptionLayer::new(stream).await.map_err(|_| RelayHandleError::HandshakeFailure)?;

        let mut context = TCPClientContext {
            receiver,
            awaitable_cache: Slab::new(),
            
        };

        let client_handle_clone = client_handle.clone();

        tokio::spawn(async move {
            context.run(client_handle_clone, read_half, write_half, encryption_handler, relay_callback_writer).await;
        });

        Ok(client_handle)
    }

    pub async fn run(&mut self, client_handle: TCPClientHandle, mut read_half: FramedReader<OwnedReadHalf>, mut write_half: FramedWriter<OwnedWriteHalf>, mut encryption_handler: EncryptionHandler, relay_callback_writer: Sender<RelayClientCallback>) {
        /* Really ugly code here but rusts burrowchecker forces me to do this */

        let mut write_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        let mut read_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        let mut core = TCPClientCore::new(client_handle, relay_callback_writer);

        loop {
             tokio::select! {
                callback = read_half.read_frame() => {

                    let Ok(buffer) = callback else {
                        break;
                    };

                    let Ok(len) = encryption_handler.decrypt(&buffer, &mut read_buffer) else {
                        println!("[Client] Decryption error");
                        break;
                    };

                    match TCPClientPacketHandler::handle_packet(&mut read_buffer.split_to(len), &mut core, &mut self.awaitable_cache).await {
                        ClientCallback::Kill => {
                            println!("[Client] Handler instructed to kill.");
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
                            let key = self.awaitable_cache.insert(sender);

                            let encoded = anypacket.encode_awaitable_client(&mut write_buffer, key as u64).unwrap();
                            
                            let Ok(len) = encryption_handler.encrypt(&encoded, &mut write_buffer) else {
                                println!("[Client] Encryption error.");
                                break;
                            };

                            let Ok(()) = write_half.write_frame(&mut write_buffer.split_to(len).freeze()).await else {
                                println!("[Client] Write error");
                                break;
                            };

                        }

                        ClientContextInstruction::RawPacket {raw_packet: _} => (),

                        ClientContextInstruction::Close => {
                            break;
                        },

                        _ => ()
                        
                    }
                }

            }
        }

        println!("[Client] Close");
        
    }

}