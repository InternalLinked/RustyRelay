use std::io;

use tokio::{net::{UdpSocket, unix::SocketAddr}, sync::mpsc::Receiver};

use crate::{networking::packethandler::packet::MAX_UDP_LENGTH, util::server_runtime};
mod udp_client_handle;
use std::net::UdpSocket as StdUdpSocket;

pub enum UDPCoreInstruction {

}

pub struct UDPServerCore {
    listener: UdpSocket,
    receiver: Receiver<UDPCoreInstruction>,
    buffer: [u8; MAX_UDP_LENGTH]
}

impl UDPServerCore {


    pub fn create(listener: StdUdpSocket, receiver: Receiver<UDPCoreInstruction>) -> io::Result<()> {

        listener.set_nonblocking(true).unwrap();
         
    
        server_runtime().spawn(async move {
              

            let buffer = [0u8; MAX_UDP_LENGTH];
            let listener = UdpSocket::from_std(listener).unwrap(); 
            let mut core = UDPServerCore { listener, receiver, buffer };
                core.listen().await.unwrap();

            });

        Ok(())
    }

    async fn listen(&mut self) -> std::io::Result<()>  {

        loop {


            if let Ok((length, address)) = self.listener.recv_from(&mut self.buffer).await {

                

            }

        }

    }

}