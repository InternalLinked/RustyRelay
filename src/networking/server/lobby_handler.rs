use slab::Slab;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use crate::networking::server::StreamHandle;

//This was AI genearted but under heavy observation by me and spicific instructions to save myself some time

// ============================================================
// Lobby<H>
// ============================================================

pub struct Lobby<H: StreamHandle> {
    host: H,
    clients: Slab<H>,
}

impl<H: StreamHandle> Lobby<H> {
    pub fn new(host: H) -> Self {
        Self {
            host,
            clients: Slab::new(),
        }
    }

    pub fn host_id(&self) -> usize {
        self.host.get_id()
    }

    pub fn is_host(&self, client_id: usize) -> bool {
        self.host.get_id() == client_id
    }

    pub fn add_client(&mut self, client: H) -> usize {
        self.clients.insert(client)
    }

    pub fn remove_client(&mut self, slot: usize) -> Option<H> {
        if self.clients.contains(slot) {
            Some(self.clients.remove(slot))
        } else {
            None
        }
    }

    pub fn contains_slot(&self, slot: usize) -> bool {
        self.clients.contains(slot)
    }

    pub fn drain_clients(&mut self) -> Vec<H> {
        todo!("TODO: Close lobby");
    }
}

// ============================================================
// TCPLobbyInstruction<H>
// ============================================================

pub enum LobbyInstruction<H: StreamHandle> {
    Create {
        host: H,
        reply: oneshot::Sender<usize>,
    },
    Join {
        lobby_slot: usize,
        client: H,
        reply: oneshot::Sender<Option<usize>>,
    },
    Leave {
        lobby_slot: usize,
        slot: usize,
    },
    Kick {
        lobby_slot: usize,
        requester_id: usize,
        target_slot: usize,
    },
    Close {
        lobby_slot: usize,
        requester_id: usize,
    },
}

// ============================================================
// TCPLobbyManager<H>
// ============================================================

pub struct LobbyManager<H: StreamHandle> {
    lobbies: Slab<Lobby<H>>,
    receiver: Receiver<LobbyInstruction<H>>,
}

impl<H: StreamHandle> LobbyManager<H> {
    pub fn new(receiver: Receiver<LobbyInstruction<H>>) -> Self {
        Self {
            lobbies: Slab::new(),
            receiver,
        }
    }

    pub async fn run(mut self) {
        while let Some(instruction) = self.receiver.recv().await {
            self.handle_instruction(instruction);
        }
    }

    fn handle_instruction(&mut self, instruction: LobbyInstruction<H>) {
        match instruction {
            LobbyInstruction::Create { host, reply } => {
                let lobby = Lobby::new(host);
                let lobby_slot = self.lobbies.insert(lobby);
                let _ = reply.send(lobby_slot);
            }

            LobbyInstruction::Join {
                lobby_slot,
                client,
                reply,
            } => {
                let slot = self
                    .lobbies
                    .get_mut(lobby_slot)
                    .map(|lobby| lobby.add_client(client));
                let _ = reply.send(slot);
            }

            LobbyInstruction::Leave { lobby_slot, slot } => {
                if let Some(lobby) = self.lobbies.get_mut(lobby_slot) {
                    lobby.remove_client(slot);
                }
            }

            LobbyInstruction::Kick {
                lobby_slot,
                requester_id,
                target_slot,
            } => {
                if let Some(lobby) = self.lobbies.get_mut(lobby_slot) {
                    if !lobby.is_host(requester_id) {
                        return;
                    }
                    lobby.remove_client(target_slot);
                }
            }

            LobbyInstruction::Close {
                lobby_slot,
                requester_id,
            } => {
                self.close_lobby(lobby_slot, requester_id);
            }
        }
    }

    fn close_lobby(&mut self, lobby_slot: usize, requester_id: usize) {
        let Some(lobby) = self.lobbies.get_mut(lobby_slot) else {
            return;
        };

        if !lobby.is_host(requester_id) {
            return;
        }

        lobby.drain_clients();
        self.lobbies.remove(lobby_slot);
    }
}

// ============================================================
// TCPLobbyManagerHandle<H>
// ============================================================

#[derive(Clone)]
pub struct LobbyManagerHandle<H: StreamHandle> {
    sender: Sender<LobbyInstruction<H>>,
}

impl<H: StreamHandle> LobbyManagerHandle<H> {
    pub fn new(buffer: usize) -> Self {
        let (sender, receiver) = mpsc::channel(buffer);

        let manager = LobbyManager::new(receiver);
        tokio::spawn(manager.run());

        Self { sender }
    }

    pub async fn create(&self, host: H) -> Option<usize> {
        let (reply, response) = oneshot::channel();

        self.sender
            .send(LobbyInstruction::Create { host, reply })
            .await
            .ok()?;

        response.await.ok()
    }

    pub async fn join(&self, lobby_slot: usize, client: H) -> Option<usize> {
        let (reply, response) = oneshot::channel();

        self.sender
            .send(LobbyInstruction::Join {
                lobby_slot,
                client,
                reply,
            })
            .await
            .ok()?;

        response.await.ok()?
    }

    pub async fn leave(&self, lobby_slot: usize, slot: usize) {
        let _ = self
            .sender
            .send(LobbyInstruction::Leave { lobby_slot, slot })
            .await;
    }

    pub async fn kick(&self, lobby_slot: usize, requester_id: usize, target_slot: usize) {
        let _ = self
            .sender
            .send(LobbyInstruction::Kick {
                lobby_slot,
                requester_id,
                target_slot,
            })
            .await;
    }

    pub async fn close(&self, lobby_slot: usize, requester_id: usize) {
        let _ = self
            .sender
            .send(LobbyInstruction::Close {
                lobby_slot,
                requester_id,
            })
            .await;
    }
}