
pub mod tcp_client_handle;

use std::{net::SocketAddr};

use dashmap::DashMap;
use tokio::{net::TcpListener, runtime::Runtime};

use crate::{networking::server::{lobby_handler::{LobbyManager, LobbyManagerHandle}, tcp::tcp_client_handle::TCPClientInstanceHandle}, util::server_runtime};

pub struct TCPServerCore {
    listener: TcpListener,
    lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>,
    next_id: u64,
}


impl TCPServerCore {


    pub fn run(address: SocketAddr) {

        server_runtime().spawn(async move {

            let listener = TcpListener::bind(address).await.unwrap();
            let mut core = TCPServerCore { listener, lobby_manager: LobbyManagerHandle::new(96, 255, 2), next_id: 0 };
            core.listen().await.unwrap();


        });
    }

    async fn listen(&mut self) -> std::io::Result<()> {


        loop {

            let (stream, address) = self.listener.accept().await?;
            println!("[Server] New connection from {}", address);
            /* The handshake runs on its own task, so a slow or silent peer can't stall the accept loop
            and a failed handshake only drops that one connection */
            let id = self.next_id;
            let lobby_manager = self.lobby_manager.clone();
            tokio::spawn(async move {
                TCPClientInstanceHandle::new(stream, id, lobby_manager).await;
            });
            self.next_id += 1;

        }

    }


}
 
