use std::io::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};
use slab::Slab;
use snow::{Builder, error};
use tokio::io::AsyncWriteExt;
use tokio::time::interval;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::networking::client::tcp::TCPClientHandle;
use crate::networking::encryption_layer::{ClientEncryptionLayer, EncryptionHandler, ServerEncryptionLayer};
use crate::networking::frame_reader::{FramedReader, FramedWriter};
use crate::networking::packethandler::packets::error_packet::{RelayError, SendableError};
use crate::networking::packethandler::packets::lobby::kick::LobbyClientKick;
use crate::networking::packethandler::packets::lobby::leave::LobbyLeftEvent;
use crate::networking::packethandler::packets::ping::{Ping, Pong};
use crate::networking::ratecache::RateCache;
use crate::networking::server::lobby_handler::LobbyState;
use crate::util::{ClientContextInstruction, mono_epoch, now_ms};
use crate::util::EncryptionState::{Handshake, Unsecure};
use crate::{networking::{packethandler::{ AnyPacket, TCPServerPacketHandler, packet::{MAX_TCP_LENGTH, Packet}}, server::{StreamHandle, lobby_handler::LobbyManagerHandle}}, util::ServerCallback};

/* How long a freshly accepted peer gets to complete the Noise handshake */
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct TCPClientInstanceHandle {
    sender: Sender<ClientContextInstruction>,
    id: u64,
    latency: Arc<AtomicU64>
}

impl TCPClientInstanceHandle {
    /* Returns None if the encryption handshake fails or the peer does not complete it in time */
    pub async fn new(stream: TcpStream, id: u64, lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>) -> Option<Self> {
        TCPClientContext::instantiate(stream, id, lobby_manager).await
    }

    pub fn get_latency(&self) -> u64 {
        self.latency.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl StreamHandle for TCPClientInstanceHandle {

    async fn send(&self, anypacket: AnyPacket) {
        if self.sender.send(ClientContextInstruction::Packet {anypacket}).await.is_err() {
            todo!("Handle closed client");
        }
    }

    async fn send_raw(&self, packet: Bytes) {
        if self.sender.send(ClientContextInstruction::RawPacket { raw_packet: packet }).await.is_err() {
            todo!("Handle closed client");
        }
    }

    async fn send_awaitable(&self, anypacket: AnyPacket) -> io::Result<AnyPacket> {
        let (sender, r) = oneshot::channel::<AnyPacket>();


        if self.sender.send(ClientContextInstruction::Awaitable{anypacket, sender}).await.is_err() {
            todo!("Handle closed server");
        }
        r.await.map_err(|e: oneshot::error::RecvError| io::Error::new(io::ErrorKind::Other, e))
    }

    async fn close(&self) {
        if self.sender.send(ClientContextInstruction::Close).await.is_err() {
            todo!("Handle closed server");
        }
    }

    /* Is used when a client gets kicked or the lobby it is in gets closed. The client may already be
    gone, then there is no state left to reset, so a closed channel is ignored */
    async fn reset_lobby_state(&self) {
        let _ = self.sender.send(ClientContextInstruction::ResetLobbyState).await;
    }

    fn get_id(&self) -> u64 {
        self.id
    }
}


pub struct TCPClientInstanceCore {

    client_handle: TCPClientInstanceHandle,
    awaitable_cache: Slab<oneshot::Sender<AnyPacket>>,
    pub write_buffer: BytesMut,
    pub lobby_state: LobbyState,
    pub latency: Arc<AtomicU64>,
    pub last_ping_timestamp: u64,

}

impl TCPClientInstanceCore {
    pub fn new(client_handle: TCPClientInstanceHandle, latency: Arc<AtomicU64>) -> Self {

        Self {client_handle, awaitable_cache: Slab::new(), lobby_state: LobbyState::Alone, write_buffer: BytesMut::with_capacity(MAX_TCP_LENGTH), latency, last_ping_timestamp: 0}
    }

    pub fn get_client_handle(&mut self) -> &mut TCPClientInstanceHandle {
        &mut self.client_handle
    }

    pub fn get_awaitable_cache(&mut self) -> &mut Slab<oneshot::Sender<AnyPacket>> {
        &mut self.awaitable_cache
    }

    /* Takes packet and the error to send. The function automatically checks if the client expects a response
    if so it sends the errorcode back */
    pub async fn send_client_error(&mut self, packet: &impl Packet, error: SendableError) {
        if !packet.is_awaitable_client() {
            return
        }

        let mut error_packet = RelayError::new(error as u8);
        error_packet.set_awaitable_client();
        error_packet.set_nonce(packet.get_nonce());
        let _ = &self.get_client_handle().send(AnyPacket::RelayErrorPacket(error_packet)).await;
    }

}

struct TCPClientContext {
    receiver: Receiver<ClientContextInstruction>,
    lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>,
    latency: Arc<AtomicU64>
}


impl TCPClientContext {
    pub async fn instantiate(stream: TcpStream, id: u64, lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>) -> Option<TCPClientInstanceHandle> {
        /* Handshake first, so nothing gets allocated for peers that never finish it. The timeout stops
        a peer that connects and stays silent from holding the socket (and this task) forever */
        let handshake = tokio::time::timeout(HANDSHAKE_TIMEOUT, ServerEncryptionLayer::new(stream)).await;

        let (read_half, write_half, encryption_handler) = match handshake {
            Ok(Ok(layer)) => layer,
            Ok(Err(())) => {
                println!("[Server] Id: {id} handshake failed");
                return None;
            }
            Err(_) => {
                println!("[Server] Id: {id} handshake timed out");
                return None;
            }
        };

        let (sender, receiver) = mpsc::channel::<ClientContextInstruction>(32);

        let latency = Arc::new(AtomicU64::new(0));
        let clone_lat = latency.clone();

        let client_handle = TCPClientInstanceHandle { sender, latency: latency.clone(), id };

        let mut context = TCPClientContext {
            receiver,
            lobby_manager,
            latency
        };

        let client_handle_clone  = client_handle.clone();

        tokio::spawn(async move {
            context.run(client_handle_clone, read_half, write_half, encryption_handler, clone_lat).await;
        });
        Some(client_handle)
    }

    pub async fn run(&mut self, client_handle: TCPClientInstanceHandle, mut read_half: FramedReader<OwnedReadHalf>, mut write_half: FramedWriter<OwnedWriteHalf>, mut encryption_handler: EncryptionHandler, latency: Arc<AtomicU64>) {
        /* Really ugly code here but rusts burrowchecker forces me to do this */

        let mut read_buffer = BytesMut::with_capacity(MAX_TCP_LENGTH);
        let mut core = TCPClientInstanceCore::new(client_handle, latency);

        let mut ticker = interval(Duration::from_secs(5));

        let mut rate_limiter = RateCache::new();

        loop {
             tokio::select! {

                _ = ticker.tick() => {
                    
                    let now = now_ms();
                    
                    core.last_ping_timestamp = now;

                    core.client_handle.send(AnyPacket::PingPacket(Ping::new())).await;

                }


                callback = read_half.read_frame() => {

                    let Ok(buffer) = callback else {
                        break;
                    };

                    let Ok(len) = encryption_handler.decrypt(&buffer, &mut read_buffer) else {
                        println!("[Server] Decryption error");
                        break;
                    };

                    match TCPServerPacketHandler::handle_packet(&mut read_buffer.split_to(len), &mut rate_limiter, &mut core, &mut self.lobby_manager).await {
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
                            
                            let encoded = anypacket.encode(&mut core.write_buffer).unwrap();
                            //write_buffer.resize(MAX_TCP_LENGTH, 0);
                            let Ok(len) = encryption_handler.encrypt(&encoded, &mut core.write_buffer) else {
                                println!("[Client] Encryption error.");
                                break;
                            };

                            let Ok(()) = write_half.write_frame(&mut core.write_buffer.split_to(len).freeze()).await else {
                                println!("[Client] Write error");
                                break;
                            };
                            

                        },

                        ClientContextInstruction::RawPacket {raw_packet} => {

                            let Ok(len) = encryption_handler.encrypt(&raw_packet, &mut core.write_buffer) else {
                                println!("[Client] Encryption error.");
                                break;
                            };

                            let Ok(()) = write_half.write_frame(&mut core.write_buffer.split_to(len)).await else {
                                println!("[Client] Write error");
                                break;
                            };
                        }, 
                        

                        ClientContextInstruction::Awaitable {mut anypacket, sender} => {
                            let key = core.awaitable_cache.insert(sender);

                            let encoded = anypacket.encode_awaitable_client(&mut core.write_buffer, key as u64).unwrap();
                            //write_buffer.resize(MAX_TCP_LENGTH, 0);
                            let Ok(len) = encryption_handler.encrypt(&encoded, &mut core.write_buffer) else {
                                println!("[Client] Encryption error.");
                                break;
                            };

                            let Ok(()) = write_half.write_frame(&mut core.write_buffer.split_to(len).freeze()).await else {
                                println!("[Client] Write error");
                                break;
                            };

                        }

                        ClientContextInstruction::ResetLobbyState => {
                            core.lobby_state = LobbyState::Alone;

                            let packet = LobbyClientKick::new();
                            core.get_client_handle().send(AnyPacket::LobbyClientKickPacket(packet)).await;
                            
                        }

                        ClientContextInstruction::Close => {
                            break;
                        }
                    }
                }

            }

        }


        /* Cleaing up referenced lobby to the client */

        match core.lobby_state {
            
            LobbyState::Alone => (),
            LobbyState::Host { lobby_id, code: _ } => {
                let _ = self.lobby_manager.close(lobby_id, core.get_client_handle().get_id()).await;
            },
            LobbyState::Member { lobby_id, seat_id } => {
                let _ = self.lobby_manager.leave(lobby_id, seat_id).await;
                
                /* Notify lobby that client left */
                let mut left_event = LobbyLeftEvent::new(seat_id);
                let left_event = left_event.encode(&mut core.write_buffer).unwrap();
                let _ = self.lobby_manager.send_host(lobby_id, left_event).await;

            },
        }

        println!("[Server] Id: {} closed", core.client_handle.get_id());
        
    }
}