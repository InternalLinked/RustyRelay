//! A lobby system for grouping [`StreamHandle`] connections (e.g. TCP clients) into
//! host/client lobbies, identified by both a slab index (`lobby_id`) and a randomly
//! generated join code.
//!
//! The system runs as a single background task ([`LobbyManager::run`]) that owns all
//! lobby state and processes [`LobbyInstruction`]s sent over a channel. Callers
//! interact with it through the cheaply cloneable [`LobbyManagerHandle`], which sends
//! instructions and awaits a reply via a `oneshot` channel per call.

use std::collections::HashMap;

use slab::Slab;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::oneshot;

use crate::networking::packethandler::packets::error_packet::SendableError;
use crate::networking::server::StreamHandle;
use crate::util::random_code;

//This was part of AI genearted but under heavy observation by me and spicific instructions to save myself some time

/// Tracks what role, if any, a given connection currently holds with respect to a lobby.
///
/// This is intended to be held by the caller (e.g. per-connection state), not by
/// [`LobbyManager`] itself, since the manager does not currently track per-client state.
#[derive(PartialEq)]
pub enum LobbyState {
    /// Not currently hosting or a member of any lobby.
    Alone,
    /// Hosting the lobby identified by `lobby_id`.
    Host { lobby_id: usize, code: u64 },
    /// A client (non-host member) of the lobby identified by `lobby_id`, occupying
    /// seat `seat_id`.
    Client { lobby_id: usize, seat_id: usize },
}

// ============================================================
// Lobby<H>
// ============================================================

/// A single lobby: one host connection plus a bounded set of client connections,
/// reachable by other players via `code`.
pub struct Lobby<H: StreamHandle> {
    /// The connection that created and owns this lobby.
    host: H,
    /// Randomly generated code other players can use to look up this lobby.
    code: u64,
    /// Connected (non-host) clients, keyed by seat id.
    clients: Slab<H>,
    /// Maximum number of clients (excluding the host) this lobby will accept.
    max_clients: usize,
}

impl<H: StreamHandle> Lobby<H> {
    /// Creates a new lobby owned by `host`, reachable via `code`, accepting at most
    /// `max_clients` non-host clients.
    pub fn new(host: H, code: u64, max_clients: usize) -> Self {
        Self {
            host,
            code,
            clients: Slab::new(),
            max_clients,
        }
    }

    /// Returns the connection id of this lobby's host.
    pub fn host_id(&self) -> usize {
        self.host.get_id()
    }

    /// Returns `true` if `client_id` is this lobby's host.
    pub fn is_host(&self, client_id: usize) -> bool {
        self.host.get_id() == client_id
    }

    /// Returns `true` if the lobby currently holds `max_clients` clients and cannot
    /// accept any more.
    pub fn is_full(&self) -> bool {
        self.clients.len() >= self.max_clients
    }

    /// Attempts to add `client` to this lobby's client slab, returning its assigned
    /// seat id.
    ///
    /// # Errors
    ///
    /// Returns [`SendableError::LobbyFull`] if the lobby is already at `max_clients`.
    pub fn add_client(&mut self, client: H) -> Result<usize, SendableError> {
        if self.is_full() {
            return Err(SendableError::LobbyFull);
        }
        Ok(self.clients.insert(client))
    }

    /// Removes and returns the client occupying `slot`, if any client is currently
    /// occupying it.
    pub fn remove_client(&mut self, slot: usize) -> Option<H> {
        if self.clients.contains(slot) {
            Some(self.clients.remove(slot))
        } else {
            None
        }
    }

    /// Returns `true` if `slot` currently has a client occupying it.
    pub fn contains_slot(&self, slot: usize) -> bool {
        self.clients.contains(slot)
    }

    /// Removes and returns every client currently in the lobby, e.g. so the caller
    /// can notify/disconnect them when the lobby is closed.
    ///
    /// # TODO
    ///
    /// Not yet implemented.
    pub fn drain_clients(&mut self) -> Vec<H> {
        todo!("TODO: Close lobby");
    }
}

// ============================================================
// TCPLobbyInstruction<H>
// ============================================================

/// A single request sent to a running [`LobbyManager`], paired with a `oneshot`
/// reply channel.
pub enum LobbyInstruction<H: StreamHandle> {
    /// Create a new lobby hosted by `host`.
    ///
    /// Replies with the new lobby's `(lobby_id, code)`, or
    /// [`SendableError::MaxLobbiesExceded`] if the manager is at capacity.
    Create {
        host: H,
        reply: oneshot::Sender<Result<(usize, u64), SendableError>>,
    },
    /// Add `client` to the lobby whose join code is `code`.
    ///
    /// The manager resolves `code` to a `lobby_id` via its `code_map` before adding
    /// the client.
    ///
    /// Replies with the new client's seat id, or [`SendableError::LobbyNotFound`] (if
    /// `code` isn't recognized) / [`SendableError::LobbyFull`] as appropriate.
    Join {
        code: u64,
        client: H,
        reply: oneshot::Sender<Result<usize, SendableError>>,
    },
    /// Remove the client at `seat_id` from the lobby identified by `lobby_id`.
    ///
    /// Replies with `Ok(())` on success (including if `seat_id` was already empty),
    /// or [`SendableError::LobbyNotFound`] if the lobby doesn't exist.
    Leave {
        lobby_id: usize,
        seat_id: usize,
        reply: oneshot::Sender<Result<(), SendableError>>,
    },
    /// Remove the client at `target_seat` from the lobby identified by `lobby_id`,
    /// on behalf of `requester_id`.
    ///
    /// Replies with [`SendableError::NotAuthorized`] if `requester_id` is not the
    /// lobby's host, or [`SendableError::LobbyNotFound`] if the lobby doesn't exist.
    Kick {
        lobby_id: usize,
        requester_id: usize,
        target_seat: usize,
        reply: oneshot::Sender<Result<(), SendableError>>,
    },
    /// Close (remove) the lobby identified by `lobby_id`, on behalf of `requester_id`.
    ///
    /// Replies with [`SendableError::NotAuthorized`] if `requester_id` is not the
    /// lobby's host, or [`SendableError::LobbyNotFound`] if the lobby doesn't exist.
    Close {
        lobby_id: usize,
        requester_id: usize,
        reply: oneshot::Sender<Result<(), SendableError>>,
    },
}

// ============================================================
// TCPLobbyManager<H>
// ============================================================

/// Owns all active lobbies and processes [`LobbyInstruction`]s sent by
/// [`LobbyManagerHandle`]s.
///
/// Constructed via [`LobbyManager::new`] and driven to completion with
/// [`LobbyManager::run`], typically spawned onto its own task (see
/// [`LobbyManagerHandle::new`]).
pub struct LobbyManager<H: StreamHandle> {
    /// All currently active lobbies, keyed by `lobby_id`.
    lobbies: Slab<Lobby<H>>,
    /// Maps a lobby's join code to its `lobby_id`, so [`LobbyInstruction::Join`] can
    /// resolve a code to the lobby it refers to.
    code_map: HashMap<u64, usize>,
    /// Channel this manager receives [`LobbyInstruction`]s from.
    receiver: Receiver<LobbyInstruction<H>>,
    /// Maximum number of lobbies this manager will allow to exist at once.
    max_lobbies: usize,
    /// Maximum number of non-host clients allowed per lobby created by this manager.
    max_clients_per_lobby: usize,
}

impl<H: StreamHandle> LobbyManager<H> {
    /// Creates a new manager that will read instructions from `receiver`, enforcing
    /// `max_lobbies` total lobbies and `max_clients_per_lobby` clients per lobby.
    pub fn new(
        receiver: Receiver<LobbyInstruction<H>>,
        max_lobbies: usize,
        max_clients_per_lobby: usize,
    ) -> Self {
        Self {
            lobbies: Slab::new(),
            code_map: HashMap::new(),
            receiver,
            max_lobbies,
            max_clients_per_lobby,
        }
    }

    /// Runs the manager's main loop, processing instructions until the sending side
    /// of `receiver` is dropped (e.g. every [`LobbyManagerHandle`] has been dropped).
    ///
    /// Intended to be spawned as its own task; see [`LobbyManagerHandle::new`].
    pub async fn run(mut self) {
        while let Some(instruction) = self.receiver.recv().await {
            self.handle_instruction(instruction);
        }
    }

    /// Dispatches a single [`LobbyInstruction`], mutating state as needed and sending
    /// the result back over its `reply` channel.
    ///
    /// Reply-send failures (i.e. the caller dropped its `oneshot::Receiver`) are
    /// silently ignored, since there's no one left to observe the outcome.
    fn handle_instruction(&mut self, instruction: LobbyInstruction<H>) {
        match instruction {
            LobbyInstruction::Create { host, reply } => {
                if self.lobbies.len() >= self.max_lobbies {
                    let _ = reply.send(Err(SendableError::MaxLobbiesExceded));
                    return;
                }

                /* Generate the join code */
                let code = random_code();

                let lobby = Lobby::new(host, code, self.max_clients_per_lobby);
                let lobby_id = self.lobbies.insert(lobby);

                /* Insert the Code with the slot id so we can resolve the lobby id by the code */
                self.code_map.insert(code, lobby_id);

                /* Send back the slot and code to the host */
                let _ = reply.send(Ok((lobby_id, code)));
            }

            LobbyInstruction::Join {
                code,
                client,
                reply,
            } => {
                /* Resolve the join code to a lobby id, then look up the lobby itself */
                let result = match self.code_map.get(&code) {
                    Some(&lobby_id) => match self.lobbies.get_mut(lobby_id) {
                        Some(lobby) => lobby.add_client(client),
                        None => Err(SendableError::LobbyNotFound),
                    },
                    None => Err(SendableError::LobbyNotFound),
                };
                let _ = reply.send(result);
            }

            LobbyInstruction::Leave {
                lobby_id,
                seat_id,
                reply,
            } => {
                let result = match self.lobbies.get_mut(lobby_id) {
                    Some(lobby) => {
                        lobby.remove_client(seat_id);
                        Ok(())
                    }
                    None => Err(SendableError::LobbyNotFound),
                };
                let _ = reply.send(result);
            }

            LobbyInstruction::Kick {
                lobby_id,
                requester_id,
                target_seat,
                reply,
            } => {
                let result = match self.lobbies.get_mut(lobby_id) {
                    Some(lobby) => {
                        if !lobby.is_host(requester_id) {
                            Err(SendableError::NotAuthorized)
                        } else {
                            lobby.remove_client(target_seat);
                            Ok(())
                        }
                    }
                    None => Err(SendableError::LobbyNotFound),
                };
                let _ = reply.send(result);
            }

            LobbyInstruction::Close {
                lobby_id,
                requester_id,
                reply,
            } => {
                let result = self.close_lobby(lobby_id, requester_id);
                let _ = reply.send(result);
            }
        }
    }

    /// Removes the lobby identified by `lobby_id`, provided `requester_id` is its host.
    ///
    /// Drains its clients (see [`Lobby::drain_clients`]) before removing it from the
    /// slab, and also removes its join code from `code_map` so stale codes can no
    /// longer resolve to it.
    ///
    /// # Errors
    ///
    /// Returns [`SendableError::LobbyNotFound`] if no such lobby exists, or
    /// [`SendableError::NotAuthorized`] if `requester_id` isn't the lobby's host.
    fn close_lobby(&mut self, lobby_id: usize, requester_id: usize) -> Result<(), SendableError> {
        let Some(lobby) = self.lobbies.get_mut(lobby_id) else {
            return Err(SendableError::LobbyNotFound);
        };

        if !lobby.is_host(requester_id) {
            return Err(SendableError::NotAuthorized);
        }

        lobby.drain_clients();
        self.code_map.remove(&lobby.code);
        self.lobbies.remove(lobby_id);
        Ok(())
    }
}

// ============================================================
// TCPLobbyManagerHandle<H>
// ============================================================

/// A cheaply cloneable handle used to interact with a [`LobbyManager`] running on a
/// background task.
///
/// Each method sends a [`LobbyInstruction`] over an internal `mpsc` channel and awaits
/// the manager's reply on a fresh `oneshot` channel. Every method returns
/// `Option<Result<T, SendableError>>`:
/// - The outer `Option` is `None` only if the underlying channel failed (e.g. the
///   manager's task has stopped running) — a transport-level failure, not a domain
///   error.
/// - The inner `Result` carries the actual outcome, including any [`SendableError`]
///   reported by the manager (e.g. [`SendableError::LobbyNotFound`],
///   [`SendableError::LobbyFull`]).
#[derive(Clone)]
pub struct LobbyManagerHandle<H: StreamHandle> {
    sender: Sender<LobbyInstruction<H>>,
}

impl<H: StreamHandle> LobbyManagerHandle<H> {
    /// Spawns a new [`LobbyManager`] on its own task and returns a handle to it.
    ///
    /// * `buffer` — capacity of the instruction channel.
    /// * `max_lobbies` — maximum number of lobbies the manager will allow at once.
    /// * `max_clients_per_lobby` — maximum non-host clients allowed per lobby.
    pub fn new(buffer: usize, max_lobbies: usize, max_clients_per_lobby: usize) -> Self {
        let (sender, receiver) = mpsc::channel(buffer);

        let manager = LobbyManager::new(receiver, max_lobbies, max_clients_per_lobby);
        tokio::spawn(manager.run());

        Self { sender }
    }

    /// Creates a new lobby hosted by `host`.
    ///
    /// # Errors
    ///
    /// Returns [`SendableError::MaxLobbiesExceded`] if the manager is at capacity.
    ///
    /// # Panics
    ///
    /// Panics if the [`LobbyManager`] task is not running (it is expected to live as
    /// long as the server).
    pub async fn create(&self, host: H) -> Result<(usize, u64), SendableError> {
        let (reply, response) = oneshot::channel();

        self.sender
            .send(LobbyInstruction::Create { host, reply })
            .await
            .expect("LobbyManager task is not running");

        response.await.expect("LobbyManager dropped reply channel")
    }

    /// Adds `client` to the lobby whose join code is `code`.
    ///
    /// # Errors
    ///
    /// Returns [`SendableError::LobbyNotFound`] if `code` isn't recognized, or
    /// [`SendableError::LobbyFull`] if the lobby is at capacity.
    ///
    /// # Panics
    ///
    /// Panics if the [`LobbyManager`] task is not running (it is expected to live as
    /// long as the server).
    pub async fn join(&self, code: u64, client: H) -> Result<usize, SendableError> {
        let (reply, response) = oneshot::channel();

        self.sender
            .send(LobbyInstruction::Join {
                code,
                client,
                reply,
            })
            .await
            .expect("LobbyManager task is not running");

        response.await.expect("LobbyManager dropped reply channel")
    }

    /// Removes the client at `seat_id` from the lobby identified by `lobby_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SendableError::LobbyNotFound`] if the lobby doesn't exist.
    ///
    /// # Panics
    ///
    /// Panics if the [`LobbyManager`] task is not running (it is expected to live as
    /// long as the server).
    pub async fn leave(&self, lobby_id: usize, seat_id: usize) -> Result<(), SendableError> {
        let (reply, response) = oneshot::channel();

        self.sender
            .send(LobbyInstruction::Leave {
                lobby_id,
                seat_id,
                reply,
            })
            .await
            .expect("LobbyManager task is not running");

        response.await.expect("LobbyManager dropped reply channel")
    }

    /// Removes the client at `target_seat` from the lobby identified by `lobby_id`,
    /// on behalf of `requester_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SendableError::NotAuthorized`] if `requester_id` isn't the host, or
    /// [`SendableError::LobbyNotFound`] if the lobby doesn't exist.
    ///
    /// # Panics
    ///
    /// Panics if the [`LobbyManager`] task is not running (it is expected to live as
    /// long as the server).
    pub async fn kick(
        &self,
        lobby_id: usize,
        requester_id: usize,
        target_seat: usize,
    ) -> Result<(), SendableError> {
        let (reply, response) = oneshot::channel();

        self.sender
            .send(LobbyInstruction::Kick {
                lobby_id,
                requester_id,
                target_seat,
                reply,
            })
            .await
            .expect("LobbyManager task is not running");

        response.await.expect("LobbyManager dropped reply channel")
    }

    /// Closes the lobby identified by `lobby_id`, on behalf of `requester_id`.
    ///
    /// # Errors
    ///
    /// Returns [`SendableError::NotAuthorized`] if `requester_id` isn't the host, or
    /// [`SendableError::LobbyNotFound`] if the lobby doesn't exist.
    ///
    /// # Panics
    ///
    /// Panics if the [`LobbyManager`] task is not running (it is expected to live as
    /// long as the server).
    pub async fn close(
        &self,
        lobby_id: usize,
        requester_id: usize,
    ) -> Result<(), SendableError> {
        let (reply, response) = oneshot::channel();

        self.sender
            .send(LobbyInstruction::Close {
                lobby_id,
                requester_id,
                reply,
            })
            .await
            .expect("LobbyManager task is not running");

        response.await.expect("LobbyManager dropped reply channel")
    }
}