use bytes::{Buf, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio::{io::{self, AsyncReadExt}, net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf, ReadHalf, WriteHalf}}, select, sync::{mpsc::{self, Receiver, Sender}, oneshot}};

use crate::{networking::{packethandler::{ AnyPacket, TCPServerPacketHandler, packet::{MAX_TCP_LENGTH, Packet}, packets::ack_packet::AckPacket}, server::{StreamHandle, lobby_handler::LobbyManagerHandle}}, util::{EncryptionState, LobbyState, ServerCallback}};

enum ClientContextInstruction {
    Close, 
    AnyPacket(AnyPacket)
}

#[derive(Clone)]
pub struct TCPClientHandle {
    sender: Sender<ClientContextInstruction>,
    id: usize,
    pub counter: usize, 
}

impl TCPClientHandle {
    pub async fn new(stream: TcpStream, id: usize, lobby_manager: LobbyManagerHandle<TCPClientHandle>) -> Self {
        TCPClientContext::instantiate(stream, id, lobby_manager).await
    }
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
        self.id
    }
}

struct TCPClientContext {
    read_half: OwnedReadHalf,
    write_half: OwnedWriteHalf,
    receiver: Receiver<ClientContextInstruction>,
    encryption_state: EncryptionState,
    client_handle: TCPClientHandle,
    lobby_manager: LobbyManagerHandle<TCPClientHandle>,
}

impl TCPClientContext {
    pub async fn instantiate(stream: TcpStream, id: usize, lobby_manager: LobbyManagerHandle<TCPClientHandle>) -> TCPClientHandle {
        let (sender, receiver) = mpsc::channel::<ClientContextInstruction>(32);
        let client_handle = TCPClientHandle { sender, id, counter: 0 };

        let (read_half, write_half) = stream.into_split();

        let mut context = TCPClientContext {
            read_half,
            write_half,
            receiver,
            encryption_state: EncryptionState::Unsecure,
            client_handle: client_handle.clone(),
            lobby_manager
        };

        tokio::spawn(async move {
            context.run().await;
        });
        client_handle
    }

    pub async fn run(&mut self) {

        tokio::select! {
            _callback = Self::handle_packet_event(&mut self.read_half, &mut self.client_handle, &mut self.lobby_manager) => {
                
            }
            _ = Self::handle_write_event(&mut self.write_half, &mut self.receiver) => {
                println!("Close");
                // channel closed (all ClientHandles dropped)
            }
        }
        
    }

    async fn handle_packet_event(read_half: &mut OwnedReadHalf, client_handle: &mut TCPClientHandle, lobby_manager: &mut LobbyManagerHandle<TCPClientHandle>) -> ServerCallback {
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

            match TCPServerPacketHandler::handle_packet(&mut read_buffer, client_handle, lobby_manager).await {
                 ServerCallback::Pass => continue,
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