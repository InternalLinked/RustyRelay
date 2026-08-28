use std::net::SocketAddr;
use std::thread;

use bytes::{Buf, Bytes, BytesMut};
use slab::Slab;
use snow::Builder;
use snow::params::NoiseParams;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::networking::frame_reader::FramedStream;
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
        if self.sender.send(ClientContextInstruction::Packet {anypacket, encrypt: true}).await.is_err() {
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
        self.sender.blocking_send(ClientContextInstruction::Packet {anypacket, encrypt: true})
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

        /*let mut write_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        let mut read_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);

        let mut encryption_handler = EncryptionHandler::new();

        let params: NoiseParams = noise_params().clone();

        let initiator_keys = Builder::new(params.clone()).generate_keypair().unwrap();

        let mut initiator = Builder::new(params.clone())
            .local_private_key(&initiator_keys.private)
            .unwrap()
            .build_initiator()
            .unwrap();

        let mut encryption = [0u8; 1024];

        let len = initiator.write_message(&[], &mut encryption).unwrap();

        let authpkg = HandshakeHello::new(RELAY_VERSION_BYTES, Bytes::copy_from_slice(&encryption[..len]));

        self.client_handle.send_unsecure(AnyPacket::HandshakeHelloPacket(authpkg)).await;

        encryption_handler.encryption_state = EncryptionState::Handshake(initiator);

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

                    TCPClientPacketHandler::handle_packet(&mut read_buffer, &mut self.client_handle, &mut encryption_handler, &mut self.awaitable_cache).await;


                }
                instruction = self.receiver.recv() => {

                    let Some(instruction) = instruction else {
                        break;
                    };

                    match instruction {
                        ClientContextInstruction::Packet {mut anypacket, encrypt} => {
                            let mut buffer = anypacket.encode(&mut write_buffer).unwrap();

                            if encrypt {
                                println!("Encrypting {}", anypacket.get_id());
                                if let EncryptionState::Encrypted(_) = &mut encryption_handler.encryption_state {

                                    println!("Encryptin!!!!");

                                    let mut enc_packet = SecureBatch::new(encryption_handler.encrypt(&buffer).unwrap());
                                    
                                    buffer = enc_packet.encode(&mut write_buffer).unwrap();

                                }
                            }

                            
                            if self.write_half.write_all(&mut buffer).await.is_err() {
                                break;
                            }
                            

                        },
                        

                        ClientContextInstruction::Awaitable {mut anypacket, sender} => {
                            let key = self.awaitable_cache.insert(sender);

                            let mut buffer = anypacket.encode_awaitable_client(&mut write_buffer, key as u64).unwrap();
                            
                            if let EncryptionState::Encrypted(_) = &mut encryption_handler.encryption_state {

                                let mut enc_packet = SecureBatch::new(encryption_handler.encrypt(&buffer).unwrap());
                                
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