use slab::Slab;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::networking::server::tcp::tcp_client_handle::TCPClientHandle;

// ============================================================
// Lobby
// ============================================================

pub struct Lobby {
    host: TCPClientHandle,
    clients: Slab<TCPClientHandle>,
}

impl Lobby {
    pub fn new(host: TCPClientHandle) -> Self {
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

    pub fn add_client(&mut self, client: TCPClientHandle) -> usize {
        self.clients.insert(client)
    }

    /// Removes a client by matching `TCPClientHandle::get_id()` rather than
    /// the Slab key.
    pub fn remove_client_by_get_id(&mut self, client_id: usize) -> Option<TCPClientHandle> {
        let key = self
            .clients
            .iter()
            .find(|(_, c)| c.get_id() == client_id)
            .map(|(key, _)| key)?;

        Some(self.clients.remove(key))
    }

    pub fn contains_client(&self, client_id: usize) -> bool {
        self.clients.iter().any(|(_, c)| c.get_id() == client_id)
    }

    /// Removes and returns every client currently in the lobby (host excluded).
    pub fn drain_clients(&mut self) -> Vec<TCPClientHandle> {
        todo!("removing clients");
    }
}

// ============================================================
// TCPLobbyInstruction
// ============================================================

pub enum TCPLobbyInstruction {
    Join {
        client: TCPClientHandle,
    },
    Leave {
        client_id: usize,
    },
    Kick {
        requester_id: usize,
        target_id: usize,
    },
    Close {
        requester_id: usize,
    },
}

// ============================================================
// TCPLobbyManager
// ============================================================

pub struct TCPLobbyManager {
    lobby: Option<Lobby>,
    receiver: Receiver<TCPLobbyInstruction>,
}

impl TCPLobbyManager {
    pub fn new(lobby: Lobby, receiver: Receiver<TCPLobbyInstruction>) -> Self {
        Self {
            lobby: Some(lobby),
            receiver,
        }
    }

    pub async fn run(mut self) {
        while let Some(instruction) = self.receiver.recv().await {
            self.handle_instruction(instruction);

            // Once the lobby has been closed, there's nothing left to manage.
            if self.lobby.is_none() {
                break;
            }
        }
    }

    fn handle_instruction(&mut self, instruction: TCPLobbyInstruction) {
        // No lobby left (already closed) — ignore anything further.
        let Some(lobby) = self.lobby.as_mut() else {
            return;
        };

        match instruction {
            TCPLobbyInstruction::Join { client } => {
                lobby.add_client(client);
            }

            TCPLobbyInstruction::Leave { client_id } => {
                if lobby.is_host(client_id) {
                    self.close_lobby(client_id);
                } else {
                    lobby.remove_client_by_get_id(client_id);
                }
            }

            TCPLobbyInstruction::Kick {
                requester_id,
                target_id,
            } => {
                if !lobby.is_host(requester_id) {
                    return;
                }
                if lobby.is_host(target_id) {
                    return;
                }
                lobby.remove_client_by_get_id(target_id);
            }

            TCPLobbyInstruction::Close { requester_id } => {
                self.close_lobby(requester_id);
            }
        }
    }

    /// Closes the lobby if `requester_id` is the host: removes every client
    /// from the slab, then removes the lobby itself from the manager.
    fn close_lobby(&mut self, requester_id: usize) {
        let Some(lobby) = self.lobby.as_mut() else {
            return;
        };

        if !lobby.is_host(requester_id) {
            return;
        }

        lobby.drain_clients();

        // Remove the lobby itself from the manager.
        self.lobby = None;
    }
}

// ============================================================
// TCPLobbyManagerHandle
// ============================================================

pub struct TCPLobbyManagerHandle {
    sender: Sender<TCPLobbyInstruction>,
}

impl TCPLobbyManagerHandle {
    pub fn new(lobby: Lobby, buffer: usize) -> Self {
        let (sender, receiver) = mpsc::channel(buffer);

        let manager = TCPLobbyManager::new(lobby, receiver);
        tokio::spawn(manager.run());

        Self { sender }
    }

    pub async fn join(&self, client: TCPClientHandle) {
        let _ = self
            .sender
            .send(TCPLobbyInstruction::Join { client })
            .await;
    }

    pub async fn leave(&self, client_id: usize) {
        let _ = self
            .sender
            .send(TCPLobbyInstruction::Leave { client_id })
            .await;
    }

    pub async fn kick(&self, requester_id: usize, target_id: usize) {
        let _ = self
            .sender
            .send(TCPLobbyInstruction::Kick {
                requester_id,
                target_id,
            })
            .await;
    }

    pub async fn close(&self, requester_id: usize) {
        let _ = self
            .sender
            .send(TCPLobbyInstruction::Close { requester_id })
            .await;
    }
}