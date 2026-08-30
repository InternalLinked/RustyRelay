use std::net::SocketAddr;
use std::thread;

use bytes::{Buf, Bytes, BytesMut, buf};
use slab::Slab;
use snow::Builder;
use snow::params::NoiseParams;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::networking::encryption_layer::{ClientEncryptionLayer, EncryptionHandler};
use crate::networking::frame_reader::{FramedReader, FramedWriter};
use crate::networking::packethandler::TCPClientPacketHandler;
use crate::util::{ClientCallback, ClientContextInstruction, RELAY_VERSION_BYTES, client_runtime, noise_params};
use crate::{networking::{packethandler::{ AnyPacket, TCPServerPacketHandler, packet::{MAX_TCP_LENGTH, Packet}, packets::ack::Ack}, server::{StreamHandle, lobby_handler::LobbyManagerHandle}}, util::{EncryptionState}};


pub struct TCPClientCore {

    client_handle: TCPClientHandle,

}

impl TCPClientCore {
    pub fn new(client_handle: TCPClientHandle) -> Self {

        Self {client_handle}
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

    async fn send(&mut self, anypacket: AnyPacket) {
        if self.sender.send(ClientContextInstruction::Packet {anypacket}).await.is_err() {
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

    pub fn close(&mut self) -> io::Result<()>{
        self.sender.blocking_send(ClientContextInstruction::Close).map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

}


struct TCPClientContext {
    receiver: Receiver<ClientContextInstruction>,
    awaitable_cache: Slab<oneshot::Sender<AnyPacket>>
}

impl TCPClientContext {
    pub async fn instantiate(stream: TcpStream) -> TCPClientHandle {
        let (sender, receiver) = mpsc::channel::<ClientContextInstruction>(32);
        let client_handle = TCPClientHandle { sender };

        

        let (read_half, write_half, encryption_handler) = ClientEncryptionLayer::new(stream).await.unwrap();

        let mut context = TCPClientContext {
            receiver,
            awaitable_cache: Slab::new(),
        };

        let client_handle_clone = client_handle.clone();

        tokio::spawn(async move {
            context.run(client_handle_clone, read_half, write_half, encryption_handler).await;
        });

        client_handle
    }

    pub async fn run(&mut self, client_handle: TCPClientHandle, mut read_half: FramedReader<OwnedReadHalf>, mut write_half: FramedWriter<OwnedWriteHalf>, mut encryption_handler: EncryptionHandler) {
        /* Really ugly code here but rusts burrowchecker forces me to do this */

        let mut write_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        let mut core = TCPClientCore::new(client_handle);

        loop {
             tokio::select! {
                callback = read_half.read_frame() => {

                    let Ok(buffer) = callback else {
                        break;
                    };

                    let Ok(mut buffer) = encryption_handler.decrypt(&buffer) else {
                        println!("[Client] Encryption handler");
                        break;
                    };

                    match TCPClientPacketHandler::handle_packet(&mut buffer, &mut core, &mut self.awaitable_cache).await {
                        ClientCallback::Kill => {
                            println!("[Client] Handler instructed to kill.");
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
                            write_buffer.resize(MAX_TCP_LENGTH, 0);
                            let Ok(buffer) = encryption_handler.encrypt(&encoded) else {
                                println!("[Client] Encryption error.");
                                break;
                            };

                            let Ok(()) = write_half.write_frame(buffer).await else {
                                println!("[Client] Write error");
                                break;
                            };
                            

                        },
                        

                        ClientContextInstruction::Awaitable {mut anypacket, sender} => {
                            let key = self.awaitable_cache.insert(sender);

                            let encoded = anypacket.encode_awaitable_client(&mut write_buffer, key as u64).unwrap();
                            
                            let Ok(buffer) = encryption_handler.encrypt(&encoded) else {
                                println!("[Client] Encryption error.");
                                break;
                            };

                            let Ok(()) = write_half.write_frame(buffer).await else {
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