use std::io;

use bytes::{Bytes, BytesMut};
use tokio::{io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt}, net::TcpStream};

pub const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024; // 16 MiB
 
const READ_CHUNK: usize = 8 * 1024;

#[derive(Debug)]
pub enum FrameError {
    Io(io::Error),
    FrameTooLarge(u32),
    /// Peer closed the connection cleanly between frames.
    Eof,
    /// Peer closed the connection mid-frame (header or payload incomplete).
    ConnectionReset,
}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> Self {
        FrameError::Io(e)
    }
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Io(e) => write!(f, "io error: {e}"),
            FrameError::FrameTooLarge(n) => write!(f, "frame of {n} bytes exceeds max"),
            FrameError::Eof => write!(f, "connection closed"),
            FrameError::ConnectionReset => write!(f, "connection closed mid-frame"),
        }
    }
}

impl std::error::Error for FrameError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            FrameError::Io(e) => Some(e),
            _ => None,
        }
    }
}
 
/// Reads and writes `u32` length-prefixed frames over any `AsyncRead` /
/// `AsyncWrite` stream (e.g. `TcpStream`).
///
/// Wire format: `[4-byte big-endian length][length bytes of payload]`.
pub struct FramedStream<S> {
    inner: S,
    read_buf: BytesMut,
}
 
impl<S> FramedStream<S> {
    pub fn new(inner: S) -> Self {
        Self { inner, read_buf: BytesMut::with_capacity(READ_CHUNK) }
    }
}
 
impl<S: AsyncRead + Unpin> FramedStream<S> {
    /// Reads one full frame and returns it as a `Bytes`  a cheap,
    /// refcounted slice, not a fresh allocation.
    pub async fn read_frame(&mut self) -> Result<Bytes, FrameError> {
        loop {
            if let Some(frame) = self.try_parse_frame()? {
                return Ok(frame);
            }
 
            if self.read_buf.capacity() - self.read_buf.len() < READ_CHUNK {
                self.read_buf.reserve(READ_CHUNK);
            }
 
            let n = self.inner.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(if self.read_buf.is_empty() {
                    FrameError::Eof
                } else {
                    FrameError::ConnectionReset
                });
            }
        }
    }
 
    /// Tries to pull one complete frame out of `read_buf` without touching
    /// the socket. Returns `Ok(None)` if more bytes are needed.
    fn try_parse_frame(&mut self) -> Result<Option<Bytes>, FrameError> {
        if self.read_buf.len() < 4 {
            return Ok(None);
        }
        let len = u32::from_be_bytes(self.read_buf[..4].try_into().unwrap());
        if len > MAX_FRAME_LEN {
            return Err(FrameError::FrameTooLarge(len));
        }
 
        let total = 4 + len as usize;
        if self.read_buf.len() < total {
            // Make sure the next socket read can land the rest of this
            // frame in one shot instead of dribbling in READ_CHUNK pieces.
            let needed = total - self.read_buf.len();
            if self.read_buf.capacity() - self.read_buf.len() < needed {
                self.read_buf.reserve(needed);
            }
            return Ok(None);
        }
 
        // split_to gives back the header+payload bytes and leaves any
        // following, already-buffered data (the start of the next frame)
        // in `self.read_buf` no copying either side.
        let mut frame = self.read_buf.split_to(total);
        let payload = frame.split_off(4).freeze();
        Ok(Some(payload))
    }
}
 
impl<S: AsyncWrite + Unpin> FramedStream<S> {
    /// Writes one length-prefixed frame. Accepts anything cheaply
    /// convertible into `Bytes` (a `Bytes` itself, `Vec<u8>`, `&'static
    /// [u8]`, ...) so passing an already-owned buffer doesn't copy it.
    pub async fn write_frame(&mut self, payload: &[u8]) -> Result<(), FrameError> {
        if payload.len() as u64 > MAX_FRAME_LEN as u64 {
            return Err(FrameError::FrameTooLarge(payload.len() as u32));
        }
 
        let header = (payload.len() as u32).to_be_bytes();
        self.inner.write_all(&header).await?;
        self.inner.write_all(&payload).await?;
        Ok(())
    }
}

