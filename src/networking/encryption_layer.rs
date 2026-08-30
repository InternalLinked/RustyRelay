use std::{error, fs::read, io::{self, Error}};

use bytes::{Bytes, BytesMut};
use snow::{Builder, HandshakeState, params::NoiseParams};
use tokio::net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}, windows::named_pipe::PipeMode::Byte};

use crate::{networking::{frame_reader::{FramedReader, FramedWriter}, packethandler::packet::MAX_TCP_LENGTH}, util::{EncryptionState, noise_params}};


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


pub struct ClientEncryptionLayer {
    encryption_handler: EncryptionHandler,
    encryption_buffer: BytesMut
}

impl ClientEncryptionLayer {

    pub async fn new(stream: TcpStream) -> Result<(FramedReader<OwnedReadHalf>, FramedWriter<OwnedWriteHalf>, EncryptionHandler), ()> {

        let (reader, writer) = stream.into_split();
        let mut reader = FramedReader::new(reader);
        let mut writer = FramedWriter::new(writer);

        let mut layer = Self {encryption_handler: EncryptionHandler::new(), encryption_buffer: BytesMut::zeroed(255)};

        layer.do_ack(&mut reader, &mut writer).await?;
        layer.handle_ack(&mut reader, &mut writer).await?;
        layer.handle_check(&mut reader, &mut writer).await?;

        Ok((reader, writer, layer.encryption_handler))

    }

    async fn do_ack(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {
        println!("[Client] doing handshake");
        let params: NoiseParams = noise_params().clone();

        let initiator_keys = Builder::new(params.clone()).generate_keypair().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)).map_err(|e| ())?;

        let builder = Builder::new(params.clone()).local_private_key(&initiator_keys.private).map_err(|e| ())?;
        let mut initiator = builder.build_initiator().map_err(|e| ())?;

        let mut encryption = BytesMut::zeroed(50);

        let len = initiator.write_message(&[], &mut encryption).map_err(|e| ())?;

        self.encryption_handler.encryption_state = EncryptionState::Handshake(initiator);

        writer.write_frame(encryption.split_to(len)).await.map_err(|e| ())?;

        Ok(())
    }

    pub async fn handle_ack(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {

        println!("[Client] Handle handshake");
        let old_state = std::mem::replace(&mut self.encryption_handler.encryption_state, EncryptionState::Unsecure);

        let EncryptionState::Handshake(mut handshake) = old_state else {

            return Err(());
        };

        let server_public_key = reader.read_frame().await.map_err(|e| ())?;

        let mut buf = [0u8; 1024];
        handshake.read_message(&server_public_key, &mut buf).unwrap(); 


        let len = handshake.write_message(&[], &mut self.encryption_buffer).map_err(|e| ())?;

        self.encryption_handler.encryption_state = EncryptionState::Encrypted(handshake.into_transport_mode().map_err(|e| ())?);

        writer.write_frame(self.encryption_buffer.split_to(len).freeze()).await.map_err(|e| ())?;

        println!("[Client] Handle");
        Ok(())

    }

    pub async fn handle_check(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {

        println!("[Client] Waiting for check");

        let buffer = reader.read_frame().await.map_err(|e| ())?;
        let buffer = self.encryption_handler.decrypt(&buffer).map_err(|e| ())?;
        if u32::from_be_bytes(buffer[..4].try_into().unwrap()) != 0x1337 {
            return Err(());
        }


        Ok(())
    }



}

pub struct ServerEncryptionLayer {
    encryption_handler: EncryptionHandler,
}

impl ServerEncryptionLayer {

    pub async fn new(stream: TcpStream) -> Result<(FramedReader<OwnedReadHalf>, FramedWriter<OwnedWriteHalf>, EncryptionHandler), ()> {

        let (reader, writer) = stream.into_split();
        let mut reader = FramedReader::new(reader);
        let mut writer = FramedWriter::new(writer);


        let mut layer = Self {encryption_handler: EncryptionHandler::new()};


        layer.handle_handshake(&mut reader, &mut writer).await.map_err(|e| e)?;
        layer.handle_ack(&mut reader, &mut writer).await.map_err(|e| e)?;

        Ok((reader, writer, layer.encryption_handler))
    }

    async fn handle_handshake(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {
        println!("[Server] trying to read frame");
        let client_public_key = reader.read_frame().await.map_err(|e| ())?;

        let mut server_public_key = BytesMut::zeroed(100);
        let params = noise_params().clone();

        let responder_keys = Builder::new(params.clone()).generate_keypair().map_err(|e| ())?;

        let mut responder = Builder::new(params.clone())
            .local_private_key(&responder_keys.private).map_err(|e| ())?
            .build_responder().map_err(|e| ())?;

        let _len = responder
        .read_message(&client_public_key, &mut server_public_key).map_err(|e| ())?;


        let len = responder
        .write_message(&[], &mut server_public_key).map_err(|e| ())?;

        self.encryption_handler.encryption_state = EncryptionState::Handshake(responder);

        writer.write_frame(server_public_key.split_to(len).freeze()).await.map_err(|e| ())?;
        println!("[Server] Send ack");

        Ok(())
    }

    async fn handle_ack(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {

        println!("[Server] Handling ack back");
        let old_state = std::mem::replace(&mut self.encryption_handler.encryption_state, EncryptionState::Unsecure);

        let EncryptionState::Handshake(mut handshake) = old_state else {
            return Err(());
        };


        let mut scratch = [0u8; 1024];

        let msg3 = reader.read_frame().await.map_err(|e| ())?;

        handshake.read_message(&msg3, &mut scratch).map_err(|e| ())?;

        println!("[Server] Successfully encrypted");

        let handshake = handshake.into_transport_mode().map_err(|e| ())?;

        self.encryption_handler.encryption_state = EncryptionState::Encrypted(handshake);

        let check: u32 = 0x1337;

        let buffer = self.encryption_handler.encrypt(&Bytes::copy_from_slice(&check.to_be_bytes())).map_err(|e| ())?;

        writer.write_frame(buffer).await.map_err(|e| ())?;

        Ok(())
    }

}