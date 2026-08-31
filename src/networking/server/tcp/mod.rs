pub mod tcp_client_handle;

use std::{net::SocketAddr};

use dashmap::DashMap;
use tokio::{net::TcpListener, runtime::Runtime};

use crate::{networking::server::{lobby_handler::{LobbyManager, LobbyManagerHandle}, tcp::tcp_client_handle::TCPClientInstanceHandle}, util::server_runtime};

pub struct TCPServerCore {
    listener: TcpListener,
    lobby_manager: LobbyManagerHandle<TCPClientInstanceHandle>,
    next_id: usize,
}

impl TCPServerCore {


    pub fn run(address: SocketAddr) {

        server_runtime().spawn(async move {

            let listener = TcpListener::bind(address).await.unwrap();
            let mut core = TCPServerCore { listener, lobby_manager: LobbyManagerHandle::new(32, 255, 2), next_id: 0 };
            core.listen().await.unwrap();


        });
    }

    async fn listen(&mut self) -> std::io::Result<()>  {


        loop {

            let (stream, address) = self.listener.accept().await?;
            println!("New connection from {}", address);
            TCPClientInstanceHandle::new(stream, self.next_id, self.lobby_manager.clone()).await;
            self.next_id += 1;

        }

    }


}
 
