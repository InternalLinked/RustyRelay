use std::io;

use bytes::{Bytes, BytesMut};
use snow::{Builder, params::NoiseParams};
use tokio::net::TcpStream;

use crate::{networking::{frame_reader::FramedStream, packethandler::{packet::MAX_TCP_LENGTH}}, util::{EncryptionState, noise_params}};


#[derive(Debug)]
pub enum EncryptionError {
    DecryptionFailed,
    InvalidState,
    BufferTooSmall,
    SetupError,
}

impl std::fmt::Display for EncryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncryptionError::DecryptionFailed => write!(f, "failed to decrypt message"),
            EncryptionError::InvalidState => write!(f, "invalid encryption state"),
            EncryptionError::BufferTooSmall => write!(f, "encryption buffer is too small"),
            EncryptionError::SetupError => write!(f, "setting up encryption failed"),
        }
    }
}


pub struct EncryptionHandler {
    pub encryption_state: EncryptionState,
    encryption_buffer: BytesMut
}

impl EncryptionHandler {

    pub fn new() -> Self {

        let buffer = BytesMut::zeroed(8096);
        
        EncryptionHandler { encryption_state: EncryptionState::Unsecure, encryption_buffer: buffer }

    }


    pub fn decrypt(&mut self, blob: &Bytes) -> Result<BytesMut, EncryptionError> {
        match &mut self.encryption_state {
            EncryptionState::Encrypted(encrypt) => {
                let len = encrypt.read_message(
                        &blob[..blob.len()],
                        &mut self.encryption_buffer[..],
                    ).map_err(|_| EncryptionError::DecryptionFailed)?;

                Ok(self.encryption_buffer.split_to(len))
            }
            _ => Err(EncryptionError::InvalidState),
        }
    }

    pub fn encrypt(&mut self, msg: &Bytes) -> Result<Bytes, EncryptionError> {
        match &mut self.encryption_state {
            EncryptionState::Encrypted(encrypt) => {
                
                let len = encrypt.write_message(&msg[..msg.len()], &mut self.encryption_buffer).unwrap();

                Ok(self.encryption_buffer.split_to(len).freeze())
            }
            _ => Err(EncryptionError::InvalidState),
        }
    }
    
}


struct ClientEncryptionLayer {
    encryption_handler: EncryptionHandler,
    stream: FramedStream<TcpStream>
}

impl ClientEncryptionLayer {

    pub async fn new(stream: TcpStream) -> io::Result<Self> {


        let mut layer = Self {encryption_handler: EncryptionHandler::new(), stream: FramedStream::new(stream)};

        layer.do_ack().await;


        Ok(layer)

    }

    async fn do_ack(&mut self) -> io::Result<()> {

        let params: NoiseParams = noise_params().clone();

        let initiator_keys = Builder::new(params.clone()).generate_keypair().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let builder = Builder::new(params.clone()).local_private_key(&initiator_keys.private).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let mut initiator = builder.build_initiator().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let mut encryption = [0u8; 1024];

        let len = initiator.write_message(&[], &mut encryption).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        self.encryption_handler.encryption_state = EncryptionState::Handshake(initiator);

        

        self.stream.write_frame(&encryption[..len]).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(())
    }

    pub async fn handle_ack(&mut self) {



    }



}

struct ServerEncryptionLayer {
    encryption_handler: EncryptionHandler,
    stream: FramedStream<TcpStream>
}

impl ServerEncryptionLayer {

    pub async fn new(stream: TcpStream) -> io::Result<Self> {

        let mut layer = Self {encryption_handler: EncryptionHandler::new(), stream: FramedStream::new(stream)};


        layer.handle_handshake().await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(layer)
    }

    async fn handle_handshake(&mut self) -> io::Result<()> {




        Ok(())
    }

}