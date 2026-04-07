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

//! ALTS connector for tonic gRPC channels.
//!
//! Implements `tower::Service<Uri>` to produce ALTS-secured connections
//! that can be used with `tonic::transport::Endpoint::connect_with_connector`.

use super::frame::{self, AltsStream};
use super::handshaker;
use hyper_util::rt::TokioIo;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::TcpStream;

/// A connector that establishes TCP connections and secures them using
/// the ALTS handshake protocol.
///
/// This is designed to be used with `tonic::transport::Endpoint::connect_with_connector()`.
#[derive(Clone)]
pub struct AltsConnector {
    target_name: String,
}

impl AltsConnector {
    pub fn new(target_name: impl Into<String>) -> Self {
        Self {
            target_name: target_name.into(),
        }
    }
}

impl tower::Service<http::Uri> for AltsConnector {
    type Response = TokioIo<AltsStream<TcpStream>>;
    type Error = AltsConnectError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: http::Uri) -> Self::Future {
        let target_name = self.target_name.clone();
        Box::pin(async move {
            // Resolve the address from the URI.
            let host = uri
                .host()
                .ok_or_else(|| AltsConnectError::InvalidUri("missing host in URI".to_string()))?;
            let port = uri.port_u16().unwrap_or(443);
            let addr = format!("{host}:{port}");

            // Establish TCP connection.
            let mut tcp = TcpStream::connect(&addr)
                .await
                .map_err(AltsConnectError::TcpConnect)?;

            // Perform ALTS handshake.
            let result = handshaker::client_handshake(&mut tcp, &target_name)
                .await
                .map_err(AltsConnectError::Handshake)?;

            // Derive encryption keys from the handshake result.
            let (client_key, server_key) =
                frame::derive_keys(&result.key_data).map_err(AltsConnectError::KeyDerivation)?;

            // Wrap the TCP stream with ALTS frame encryption.
            let alts_stream = AltsStream::new(tcp, client_key, server_key);

            Ok(TokioIo::new(alts_stream))
        })
    }
}

#[derive(Debug)]
pub enum AltsConnectError {
    InvalidUri(String),
    TcpConnect(std::io::Error),
    Handshake(handshaker::HandshakeError),
    KeyDerivation(std::io::Error),
}

impl std::fmt::Display for AltsConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUri(msg) => write!(f, "invalid URI for ALTS connection: {msg}"),
            Self::TcpConnect(e) => write!(f, "TCP connection failed: {e}"),
            Self::Handshake(e) => write!(f, "ALTS handshake failed: {e}"),
            Self::KeyDerivation(e) => write!(f, "ALTS key derivation failed: {e}"),
        }
    }
}

impl std::error::Error for AltsConnectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TcpConnect(e) => Some(e),
            Self::Handshake(e) => Some(e),
            Self::KeyDerivation(e) => Some(e),
            _ => None,
        }
    }
}
