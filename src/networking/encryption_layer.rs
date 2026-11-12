use std::{error, fs::read, io::{self, Error}};

use bytes::{Bytes, BytesMut};
use snow::{Builder, HandshakeState, params::NoiseParams};
use tokio::net::{TcpStream, tcp::{OwnedReadHalf, OwnedWriteHalf}};
use crate::{networking::{frame_reader::{FramedReader, FramedWriter}, packethandler::packet::MAX_TCP_LENGTH}, util::{EncryptionState, noise_params}};


#[derive(Debug)]
pub enum EncryptionError {
    DecryptionFailed,
    EncryptionFailed,
    InvalidState,
    BufferTooSmall,
    SetupError,
}

impl std::fmt::Display for EncryptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncryptionError::DecryptionFailed => write!(f, "failed to decrypt message"),
            EncryptionError::EncryptionFailed => write!(f, "failed to encrypt message"),
            EncryptionError::InvalidState => write!(f, "invalid encryption state"),
            EncryptionError::BufferTooSmall => write!(f, "encryption buffer is too small"),
            EncryptionError::SetupError => write!(f, "setting up encryption failed"),
        }
    }
}

pub struct EncryptionHandler {
    pub encryption_state: EncryptionState,
}

impl EncryptionHandler {

    pub fn new() -> Self {
        EncryptionHandler { encryption_state: EncryptionState::Unsecure }
    }

    pub fn decrypt(&mut self, blob: &Bytes, out: &mut BytesMut) -> Result<usize, EncryptionError> {
        match &mut self.encryption_state {
            EncryptionState::Encrypted(encrypt) => {
                out.resize(blob.len(), 0);
                let len = encrypt
                    .read_message(&blob[..blob.len()], &mut out[..])
                    .map_err(|_| EncryptionError::DecryptionFailed)?;

                Ok(len)
            }
            _ => Err(EncryptionError::InvalidState),
        }
    }

    pub fn encrypt(&mut self, msg: &Bytes, out: &mut BytesMut) -> Result<usize, EncryptionError> {
        match &mut self.encryption_state {
            EncryptionState::Encrypted(encrypt) => {
                out.resize(msg.len() + 16, 0);
                /* Fails e.g. if the message is larger than a Noise message may be. That is reported as an
                error instead of a panic, so the caller can close the connection cleanly */
                let len = encrypt
                    .write_message(&msg[..msg.len()], &mut out[..])
                    .map_err(|_| EncryptionError::EncryptionFailed)?;

                Ok(len)
            }
            _ => Err(EncryptionError::InvalidState),
        }
    }

}


pub struct ClientEncryptionLayer {
    encryption_handler: EncryptionHandler,
    encryption_buffer: BytesMut,
}

impl ClientEncryptionLayer {

    pub async fn new(stream: TcpStream) -> Result<(FramedReader<OwnedReadHalf>, FramedWriter<OwnedWriteHalf>, EncryptionHandler), ()> {

        let (reader, writer) = stream.into_split();
        let mut reader = FramedReader::new(reader);
        let mut writer = FramedWriter::new(writer);

        let mut layer = Self {
            encryption_handler: EncryptionHandler::new(),
            encryption_buffer: BytesMut::zeroed(MAX_TCP_LENGTH + 16),
        };

        layer.do_ack(&mut reader, &mut writer).await?;
        layer.handle_ack(&mut reader, &mut writer).await?;
        layer.handle_check(&mut reader, &mut writer).await?;

        Ok((reader, writer, layer.encryption_handler))

    }

    async fn do_ack(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {
        let params: NoiseParams = noise_params().clone();

        let initiator_keys = Builder::new(params.clone()).generate_keypair().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)).map_err(|_| ())?;

        let builder = Builder::new(params.clone()).local_private_key(&initiator_keys.private).map_err(|_| ())?;
        let mut initiator = builder.build_initiator().map_err(|_| ())?;

        let len = initiator.write_message(&[], &mut self.encryption_buffer).map_err(|_| ())?;

        self.encryption_handler.encryption_state = EncryptionState::Handshake(initiator);

        writer.write_frame(&self.encryption_buffer[..len]).await.map_err(|_| ())?;

        Ok(())
    }

    pub async fn handle_ack(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {

        let old_state = std::mem::replace(&mut self.encryption_handler.encryption_state, EncryptionState::Unsecure);

        let EncryptionState::Handshake(mut handshake) = old_state else {
            return Err(());
        };

        let server_public_key = reader.read_frame().await.map_err(|_| ())?;

        let mut buf = [0u8; 1024];
        handshake.read_message(&server_public_key, &mut buf).unwrap();

        let len = handshake.write_message(&[], &mut self.encryption_buffer).map_err(|_| ())?;

        self.encryption_handler.encryption_state = EncryptionState::Encrypted(handshake.into_transport_mode().map_err(|_| ())?);

        writer.write_frame(&self.encryption_buffer[..len]).await.map_err(|_| ())?;

        Ok(())

    }

    pub async fn handle_check(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {


        let frame = reader.read_frame().await.map_err(|_| ())?;
        let len = self.encryption_handler.decrypt(&frame, &mut self.encryption_buffer).map_err(|_| ())?;

        if len < 4 || u32::from_be_bytes(self.encryption_buffer[..4].try_into().unwrap()) != 0x1337 {
            return Err(());
        }

        println!("[Client] Successfully encrypted");
        Ok(())
    }

}

pub struct ServerEncryptionLayer {
    encryption_handler: EncryptionHandler,
    encryption_buffer: BytesMut,
}

impl ServerEncryptionLayer {

    pub async fn new(stream: TcpStream) -> Result<(FramedReader<OwnedReadHalf>, FramedWriter<OwnedWriteHalf>, EncryptionHandler), ()> {

        let (reader, writer) = stream.into_split();
        let mut reader = FramedReader::new(reader);
        let mut writer = FramedWriter::new(writer);

        let mut layer = Self {
            encryption_handler: EncryptionHandler::new(),
            encryption_buffer: BytesMut::zeroed(MAX_TCP_LENGTH + 16),
        };

        layer.handle_handshake(&mut reader, &mut writer).await?;
        layer.handle_ack(&mut reader, &mut writer).await?;

        Ok((reader, writer, layer.encryption_handler))
    }

    async fn handle_handshake(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {
        let client_public_key = reader.read_frame().await.map_err(|_| ())?;

        let params = noise_params().clone();

        let responder_keys = Builder::new(params.clone()).generate_keypair().map_err(|_| ())?;

        let mut responder = Builder::new(params.clone())
            .local_private_key(&responder_keys.private).map_err(|_| ())?
            .build_responder().map_err(|_| ())?;

        let _len = responder
            .read_message(&client_public_key, &mut self.encryption_buffer).map_err(|_| ())?;

        let len = responder
            .write_message(&[], &mut self.encryption_buffer).map_err(|_| ())?;

        self.encryption_handler.encryption_state = EncryptionState::Handshake(responder);

        writer.write_frame(&self.encryption_buffer[..len]).await.map_err(|_| ())?;

        Ok(())
    }

    async fn handle_ack(&mut self, reader: &mut FramedReader<OwnedReadHalf>, writer: &mut FramedWriter<OwnedWriteHalf>) -> Result<(), ()> {

        let old_state = std::mem::replace(&mut self.encryption_handler.encryption_state, EncryptionState::Unsecure);

        let EncryptionState::Handshake(mut handshake) = old_state else {
            return Err(());
        };

        let mut scratch = [0u8; 1024];

        let msg3 = reader.read_frame().await.map_err(|_| ())?;

        handshake.read_message(&msg3, &mut scratch).map_err(|_| ())?;

        println!("[Server] Successfully encrypted");

        let handshake = handshake.into_transport_mode().map_err(|_| ())?;

        self.encryption_handler.encryption_state = EncryptionState::Encrypted(handshake);

        let check: u32 = 0x1337;

        let len = self.encryption_handler
            .encrypt(&Bytes::copy_from_slice(&check.to_be_bytes()), &mut self.encryption_buffer)
            .map_err(|_| ())?;

        writer.write_frame(&self.encryption_buffer[..len]).await.map_err(|_| ())?;

        Ok(())
    }

}