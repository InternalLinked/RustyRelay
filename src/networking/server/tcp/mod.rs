pub mod tcp_client_handle;

use std::{net::SocketAddr};

use slab::Slab;
use tokio::{net::TcpListener, runtime::Runtime};

use crate::networking::server::tcp::tcp_client_handle::ClientHandle;

pub struct TCPServerCore {
    listener: TcpListener,
    connections: Slab<usize>,
}

impl TCPServerCore {


    pub fn run(address: SocketAddr) {
        let rt = Runtime::new().unwrap();

        rt.block_on(async {

            let listener = TcpListener::bind(address).await.unwrap();
            let mut core = TCPServerCore { listener, connections: Slab::new() };
            core.listen().await.unwrap();


        });
    }

    async fn listen(&mut self) -> std::io::Result<()>  {


        loop {

            let (stream, address) = self.listener.accept().await?;
            println!("New connection from {}", address);

            tokio::spawn(async move {
                let client = ClientHandle::new(stream).await;

            });

        }

    }


}
 
