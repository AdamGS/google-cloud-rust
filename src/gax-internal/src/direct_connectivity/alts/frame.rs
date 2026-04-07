// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! ALTS record protocol frame encryption and decryption.
//!
//! The ALTS record protocol wraps data in frames with the following structure:
//!
//! ```text
//! +------------------+-------------------------------------------+
//! | Frame Length (4B) | Encrypted Payload + GCM Tag               |
//! +------------------+-------------------------------------------+
//! ```
//!
//! - Frame length is a 32-bit little-endian unsigned integer indicating the
//!   length of the encrypted payload (including the 16-byte GCM tag).
//! - The encrypted payload uses AES-128-GCM with a counter-based nonce.
//! - The nonce is 12 bytes: the first 4 bytes are a fixed counter prefix,
//!   and the last 8 bytes are a 64-bit little-endian sequence number.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Size of the frame length prefix in bytes.
const FRAME_LENGTH_SIZE: usize = 4;
/// Size of the AES-128-GCM authentication tag.
const TAG_SIZE: usize = 16;
/// Size of the AES-128 key.
const KEY_SIZE: usize = 16;
/// Size of the GCM nonce.
const NONCE_SIZE: usize = 12;
/// Maximum frame payload size (1 MiB).
const MAX_FRAME_SIZE: usize = 1024 * 1024;
/// Size of the nonce counter portion (last 8 bytes of the nonce).
const COUNTER_SIZE: usize = 8;
/// Fixed nonce prefix size.
const NONCE_PREFIX_SIZE: usize = NONCE_SIZE - COUNTER_SIZE;

/// Derives the client and server keys from the ALTS handshake key material.
///
/// The key material from the handshaker is split into:
/// - Client key: first KEY_SIZE bytes
/// - Server key: next KEY_SIZE bytes
///
/// For a client-side crypter, we encrypt with the client key and decrypt
/// with the server key.
pub fn derive_keys(key_data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), io::Error> {
    if key_data.len() < 2 * KEY_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "ALTS key data too short: {} bytes, need at least {}",
                key_data.len(),
                2 * KEY_SIZE
            ),
        ));
    }
    let client_key = key_data[..KEY_SIZE].to_vec();
    let server_key = key_data[KEY_SIZE..2 * KEY_SIZE].to_vec();
    Ok((client_key, server_key))
}

/// Encrypts a plaintext frame using AES-128-GCM.
///
/// Returns the encrypted payload (ciphertext + tag).
pub fn encrypt_frame(key: &[u8], counter: u64, plaintext: &[u8]) -> Result<Vec<u8>, io::Error> {
    use aws_lc_rs::aead;

    let unbound_key = aead::UnboundKey::new(&aead::AES_128_GCM, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad key: {e}")))?;
    let sealing_key = aead::LessSafeKey::new(unbound_key);

    let nonce_bytes = make_nonce(counter);
    let nonce = aead::Nonce::assume_unique_for_key(nonce_bytes);

    let mut in_out = plaintext.to_vec();
    sealing_key
        .seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut in_out)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("encryption failed: {e}")))?;

    Ok(in_out)
}

/// Decrypts a ciphertext frame (ciphertext + tag) using AES-128-GCM.
///
/// Returns the decrypted plaintext.
pub fn decrypt_frame(
    key: &[u8],
    counter: u64,
    ciphertext_and_tag: &[u8],
) -> Result<Vec<u8>, io::Error> {
    use aws_lc_rs::aead;

    let unbound_key = aead::UnboundKey::new(&aead::AES_128_GCM, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad key: {e}")))?;
    let opening_key = aead::LessSafeKey::new(unbound_key);

    let nonce_bytes = make_nonce(counter);
    let nonce = aead::Nonce::assume_unique_for_key(nonce_bytes);

    let mut in_out = ciphertext_and_tag.to_vec();
    let plaintext = opening_key
        .open_in_place(nonce, aead::Aad::empty(), &mut in_out)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "ALTS frame decryption failed"))?;

    Ok(plaintext.to_vec())
}

fn make_nonce(counter: u64) -> [u8; NONCE_SIZE] {
    let mut nonce = [0u8; NONCE_SIZE];
    // The first NONCE_PREFIX_SIZE bytes are zero (fixed prefix).
    // The last COUNTER_SIZE bytes are the counter in little-endian.
    nonce[NONCE_PREFIX_SIZE..].copy_from_slice(&counter.to_le_bytes());
    nonce
}

/// An ALTS-encrypted transport stream that wraps a raw TCP connection.
///
/// Implements `AsyncRead` and `AsyncWrite` by transparently encrypting and
/// decrypting ALTS frames.
pub struct AltsStream<S> {
    inner: S,
    /// Key used for encrypting outgoing frames (client key).
    encrypt_key: Vec<u8>,
    /// Key used for decrypting incoming frames (server key).
    decrypt_key: Vec<u8>,
    /// Sequence number for outgoing frames.
    write_counter: u64,
    /// Sequence number for incoming frames.
    read_counter: u64,
    /// Buffer for decrypted data that hasn't been consumed yet.
    read_buf: Vec<u8>,
    /// Current read position in read_buf.
    read_pos: usize,
    /// Buffer for accumulating incoming frame data.
    frame_buf: Vec<u8>,
    /// Expected total size of the current incoming frame (including length prefix).
    frame_expected: Option<usize>,
}

impl<S> AltsStream<S> {
    /// Creates a new ALTS-encrypted stream.
    ///
    /// `encrypt_key` and `decrypt_key` should be derived from the ALTS
    /// handshake result via `derive_keys`.
    pub fn new(inner: S, encrypt_key: Vec<u8>, decrypt_key: Vec<u8>) -> Self {
        Self {
            inner,
            encrypt_key,
            decrypt_key,
            write_counter: 0,
            read_counter: 0,
            read_buf: Vec::new(),
            read_pos: 0,
            frame_buf: Vec::new(),
            frame_expected: None,
        }
    }
}

impl<S> AsyncRead for AltsStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // Return any buffered decrypted data first.
        if this.read_pos < this.read_buf.len() {
            let available = &this.read_buf[this.read_pos..];
            let to_copy = available.len().min(buf.remaining());
            buf.put_slice(&available[..to_copy]);
            this.read_pos += to_copy;
            if this.read_pos == this.read_buf.len() {
                this.read_buf.clear();
                this.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // Read more data from the inner stream to assemble a complete frame.
        let mut tmp = [0u8; 8192];
        let mut tmp_buf = ReadBuf::new(&mut tmp);
        match Pin::new(&mut this.inner).poll_read(cx, &mut tmp_buf) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {
                let n = tmp_buf.filled().len();
                if n == 0 {
                    return Poll::Ready(Ok(()));
                }
                this.frame_buf.extend_from_slice(&tmp[..n]);
            }
        }

        // Try to parse a complete frame from the buffer.
        loop {
            if this.frame_expected.is_none() && this.frame_buf.len() >= FRAME_LENGTH_SIZE {
                let len_bytes: [u8; 4] = this.frame_buf[..FRAME_LENGTH_SIZE].try_into().unwrap();
                let frame_len = u32::from_le_bytes(len_bytes) as usize;
                if frame_len > MAX_FRAME_SIZE {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("ALTS frame too large: {frame_len}"),
                    )));
                }
                this.frame_expected = Some(FRAME_LENGTH_SIZE + frame_len);
            }

            if let Some(expected) = this.frame_expected {
                if this.frame_buf.len() >= expected {
                    let ciphertext = &this.frame_buf[FRAME_LENGTH_SIZE..expected];
                    let plaintext =
                        decrypt_frame(&this.decrypt_key, this.read_counter, ciphertext)?;
                    this.read_counter += 1;

                    // Remove the consumed frame from the buffer.
                    this.frame_buf.drain(..expected);
                    this.frame_expected = None;

                    // Copy decrypted data to output.
                    let to_copy = plaintext.len().min(buf.remaining());
                    buf.put_slice(&plaintext[..to_copy]);
                    if to_copy < plaintext.len() {
                        this.read_buf = plaintext;
                        this.read_pos = to_copy;
                    }
                    return Poll::Ready(Ok(()));
                }
            }

            // Not enough data for a complete frame yet.
            // We need to return Pending and wait for more data.
            // But we already consumed some data from the inner stream,
            // so we need to wake up and try again.
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
    }
}

impl<S> AsyncWrite for AltsStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // Limit the plaintext to the max frame payload size.
        let plaintext_len = buf.len().min(MAX_FRAME_SIZE - TAG_SIZE);
        let plaintext = &buf[..plaintext_len];

        let encrypted = encrypt_frame(&this.encrypt_key, this.write_counter, plaintext)?;

        // Write the frame: length prefix + encrypted payload.
        let frame_len = (encrypted.len() as u32).to_le_bytes();
        let mut frame = Vec::with_capacity(FRAME_LENGTH_SIZE + encrypted.len());
        frame.extend_from_slice(&frame_len);
        frame.extend_from_slice(&encrypted);

        // Write the entire frame to the inner stream.
        match Pin::new(&mut this.inner).poll_write(cx, &frame) {
            Poll::Ready(Ok(n)) => {
                if n == frame.len() {
                    this.write_counter += 1;
                    Poll::Ready(Ok(plaintext_len))
                } else {
                    // Partial write of an encrypted frame is problematic.
                    // In practice, tonic/hyper uses buffered I/O, so this
                    // shouldn't happen with normal usage.
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "partial ALTS frame write",
                    )))
                }
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_nonce() {
        let nonce = make_nonce(0);
        assert_eq!(nonce, [0u8; NONCE_SIZE]);

        let nonce = make_nonce(1);
        let mut expected = [0u8; NONCE_SIZE];
        expected[NONCE_PREFIX_SIZE] = 1;
        assert_eq!(nonce, expected);

        let nonce = make_nonce(0x0102030405060708);
        let mut expected = [0u8; NONCE_SIZE];
        expected[NONCE_PREFIX_SIZE..].copy_from_slice(&0x0102030405060708u64.to_le_bytes());
        assert_eq!(nonce, expected);
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = [0x42u8; KEY_SIZE];
        let plaintext = b"Hello, ALTS!";

        let encrypted = encrypt_frame(&key, 0, plaintext).unwrap();
        assert_ne!(&encrypted[..plaintext.len()], plaintext);
        assert_eq!(encrypted.len(), plaintext.len() + TAG_SIZE);

        let decrypted = decrypt_frame(&key, 0, &encrypted).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decrypt_wrong_counter_fails() {
        let key = [0x42u8; KEY_SIZE];
        let plaintext = b"Hello, ALTS!";

        let encrypted = encrypt_frame(&key, 0, plaintext).unwrap();
        let result = decrypt_frame(&key, 1, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_decrypt_wrong_key_fails() {
        let key1 = [0x42u8; KEY_SIZE];
        let key2 = [0x43u8; KEY_SIZE];
        let plaintext = b"Hello, ALTS!";

        let encrypted = encrypt_frame(&key1, 0, plaintext).unwrap();
        let result = decrypt_frame(&key2, 0, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_derive_keys() {
        let key_data = vec![0u8; 32];
        let (client_key, server_key) = derive_keys(&key_data).unwrap();
        assert_eq!(client_key.len(), KEY_SIZE);
        assert_eq!(server_key.len(), KEY_SIZE);
    }

    #[test]
    fn test_derive_keys_too_short() {
        let key_data = vec![0u8; 15];
        assert!(derive_keys(&key_data).is_err());
    }
}
