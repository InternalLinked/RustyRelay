pub mod tcp_client_handle;
pub mod lobby_handler;

use std::{net::SocketAddr};

use dashmap::DashMap;
use tokio::{net::TcpListener, runtime::Runtime};

use crate::networking::server::tcp::tcp_client_handle::TCPClientHandle;

pub struct TCPServerCore {
    listener: TcpListener,
    clients: DashMap<usize, TCPClientHandle>,
    next_id: usize,
}

impl TCPServerCore {


    pub fn run(address: SocketAddr) {
        let rt = Runtime::new().unwrap();

        rt.block_on(async {

            let listener = TcpListener::bind(address).await.unwrap();
            let mut core = TCPServerCore { listener, clients: DashMap::new(), next_id: 0 };
            core.listen().await.unwrap();


        });
    }

    async fn listen(&mut self) -> std::io::Result<()>  {


        loop {

            let (stream, address) = self.listener.accept().await?;
            println!("New connection from {}", address);
            let client = TCPClientHandle::new(stream, self.next_id).await;
            self.clients.insert(self.next_id, client);
            self.next_id += 1;

        }

    }


}
 
