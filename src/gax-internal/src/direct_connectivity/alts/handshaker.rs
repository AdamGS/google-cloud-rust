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

use crate::direct_connectivity::proto;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

const DEFAULT_HANDSHAKER_ADDR: &str = "http://metadata.google.internal:8080";
const ALTS_RECORD_PROTOCOL: &str = "ALTSRP_GCM_AES128";
const ALTS_RECORD_PROTOCOL_REKEY: &str = "ALTSRP_GCM_AES128_REKEY";
const ALTS_APPLICATION_PROTOCOL: &str = "grpc";

/// Result of a successful ALTS handshake.
#[derive(Debug)]
pub struct HandshakeResult {
    /// Key material for the ALTS record protocol.
    pub key_data: Vec<u8>,
    /// The negotiated record protocol (e.g., "ALTSRP_GCM_AES128").
    pub record_protocol: String,
    /// The peer's authenticated identity (service account).
    pub peer_identity: Option<String>,
    /// Maximum frame size for the ALTS record protocol.
    pub max_frame_size: u32,
    /// Any unconsumed bytes from the handshake that are actually
    /// application data.
    pub unconsumed_bytes: Vec<u8>,
}

fn handshaker_address() -> String {
    std::env::var("ALTS_HANDSHAKER_SERVICE_ADDRESS")
        .unwrap_or_else(|_| DEFAULT_HANDSHAKER_ADDR.to_string())
}

/// Performs a client-side ALTS handshake over the given transport connection,
/// using the local handshaker service.
///
/// `transport` is the raw TCP connection to the remote peer.
/// Returns the handshake result containing key material and peer identity,
/// along with any unconsumed bytes.
pub async fn client_handshake<S>(
    transport: &mut S,
    target_name: &str,
) -> Result<HandshakeResult, HandshakeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let addr = handshaker_address();
    tracing::debug!("connecting to ALTS handshaker at {addr}");

    let handshaker_channel = Channel::from_shared(addr.clone())
        .map_err(|e| HandshakeError::InvalidAddress(addr.clone(), e.to_string()))?
        .connect()
        .await
        .map_err(HandshakeError::ConnectHandshaker)?;
    tracing::debug!("connected to ALTS handshaker");

    let mut client =
        proto::handshaker_service_client::HandshakerServiceClient::new(handshaker_channel);

    let (tx, rx) = tokio::sync::mpsc::channel(4);

    // Send the initial StartClientHandshakeReq.
    let start_req = proto::HandshakerReq {
        req_oneof: Some(proto::handshaker_req::ReqOneof::ClientStart(
            proto::StartClientHandshakeReq {
                handshake_security_protocol: proto::HandshakeProtocol::Alts as i32,
                application_protocols: vec![ALTS_APPLICATION_PROTOCOL.to_string()],
                record_protocols: vec![
                    ALTS_RECORD_PROTOCOL_REKEY.to_string(),
                    ALTS_RECORD_PROTOCOL.to_string(),
                ],
                target_name: target_name.to_string(),
                rpc_versions: Some(proto::RpcProtocolVersions {
                    max_rpc_version: Some(proto::rpc_protocol_versions::Version {
                        major: 2,
                        minor: 1,
                    }),
                    min_rpc_version: Some(proto::rpc_protocol_versions::Version {
                        major: 2,
                        minor: 1,
                    }),
                }),
                max_frame_size: 1024 * 1024, // 1 MiB
                ..Default::default()
            },
        )),
    };
    tx.send(start_req)
        .await
        .map_err(|_| HandshakeError::ChannelClosed)?;

    let request_stream = ReceiverStream::new(rx);
    tracing::debug!("calling DoHandshake RPC");
    let mut response_stream = client
        .do_handshake(request_stream)
        .await
        .map_err(|s| {
            tracing::error!("DoHandshake RPC failed: {s}");
            HandshakeError::Rpc(s)
        })?
        .into_inner();
    tracing::debug!("DoHandshake stream established, awaiting first response");

    let mut round = 0u32;
    loop {
        round += 1;
        let resp = response_stream
            .message()
            .await
            .map_err(|s| {
                tracing::error!("handshaker stream error on round {round}: {s}");
                HandshakeError::Rpc(s)
            })?
            .ok_or_else(|| {
                tracing::error!(
                    "handshaker stream ended on round {round} without result"
                );
                HandshakeError::UnexpectedEnd
            })?;

        tracing::debug!(
            "handshaker response round {round}: out_frames={} bytes, \
             bytes_consumed={}, has_result={}, status={:?}",
            resp.out_frames.len(),
            resp.bytes_consumed,
            resp.result.is_some(),
            resp.status,
        );

        // Check handshaker status.
        if let Some(ref status) = resp.status {
            if status.code != 0 {
                return Err(HandshakeError::HandshakerError(
                    status.code,
                    status.details.clone(),
                ));
            }
        }

        // If the handshaker has out_frames, send them to the peer.
        if !resp.out_frames.is_empty() {
            transport
                .write_all(&resp.out_frames)
                .await
                .map_err(HandshakeError::Io)?;
            transport.flush().await.map_err(HandshakeError::Io)?;
            tracing::debug!("sent {} out_frames bytes to peer", resp.out_frames.len());
        }

        // If result is set, handshake is complete.
        if let Some(result) = resp.result {
            let peer_identity = result.peer_identity.and_then(|id| match id.identity_oneof {
                Some(proto::identity::IdentityOneof::ServiceAccount(sa)) => Some(sa),
                Some(proto::identity::IdentityOneof::Hostname(h)) => Some(h),
                None => None,
            });

            tracing::info!(
                "ALTS handshake complete: protocol={}, peer={:?}, max_frame_size={}",
                result.record_protocol,
                peer_identity,
                result.max_frame_size,
            );

            return Ok(HandshakeResult {
                key_data: result.key_data,
                record_protocol: result.record_protocol,
                peer_identity,
                max_frame_size: result.max_frame_size,
                unconsumed_bytes: Vec::new(),
            });
        }

        // Handshake not yet complete. Read bytes from the peer and send
        // them to the handshaker service as a NextHandshakeMessageReq.
        let mut buf = vec![0u8; 64 * 1024];
        let n = transport
            .read(&mut buf)
            .await
            .map_err(HandshakeError::Io)?;
        if n == 0 {
            tracing::error!("peer connection closed during handshake round {round}");
            return Err(HandshakeError::UnexpectedEnd);
        }
        buf.truncate(n);
        tracing::debug!("read {n} bytes from peer, forwarding to handshaker");

        let next_req = proto::HandshakerReq {
            req_oneof: Some(proto::handshaker_req::ReqOneof::Next(
                proto::NextHandshakeMessageReq {
                    in_bytes: buf,
                    ..Default::default()
                },
            )),
        };
        tx.send(next_req)
            .await
            .map_err(|_| HandshakeError::ChannelClosed)?;
    }
}

/// Diagnostic function to check if the ALTS handshaker service is reachable
/// and responds to a basic request. Does NOT perform a full handshake.
pub async fn check_handshaker_reachable() -> Result<String, HandshakeError> {
    let addr = handshaker_address();
    tracing::info!("checking ALTS handshaker at {addr}");

    let channel = Channel::from_shared(addr.clone())
        .map_err(|e| HandshakeError::InvalidAddress(addr.clone(), e.to_string()))?
        .connect()
        .await
        .map_err(|e| {
            tracing::error!("cannot connect to handshaker at {addr}: {e}");
            HandshakeError::ConnectHandshaker(e)
        })?;
    tracing::info!("TCP/gRPC connection to handshaker established");

    let mut client =
        proto::handshaker_service_client::HandshakerServiceClient::new(channel);

    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let start_req = proto::HandshakerReq {
        req_oneof: Some(proto::handshaker_req::ReqOneof::ClientStart(
            proto::StartClientHandshakeReq {
                handshake_security_protocol: proto::HandshakeProtocol::Alts as i32,
                application_protocols: vec!["grpc".to_string()],
                record_protocols: vec![
                    ALTS_RECORD_PROTOCOL_REKEY.to_string(),
                    ALTS_RECORD_PROTOCOL.to_string(),
                ],
                target_name: "diagnostic_check".to_string(),
                rpc_versions: Some(proto::RpcProtocolVersions {
                    max_rpc_version: Some(proto::rpc_protocol_versions::Version {
                        major: 2,
                        minor: 1,
                    }),
                    min_rpc_version: Some(proto::rpc_protocol_versions::Version {
                        major: 2,
                        minor: 1,
                    }),
                }),
                max_frame_size: 1024 * 1024,
                ..Default::default()
            },
        )),
    };
    tx.send(start_req)
        .await
        .map_err(|_| HandshakeError::ChannelClosed)?;

    let request_stream = ReceiverStream::new(rx);
    let mut response_stream = client
        .do_handshake(request_stream)
        .await
        .map_err(|s| {
            tracing::error!("DoHandshake RPC failed: {s}");
            HandshakeError::Rpc(s)
        })?
        .into_inner();

    // We just want to see if the handshaker responds at all.
    let resp = response_stream
        .message()
        .await
        .map_err(|s| {
            tracing::error!("handshaker stream error: {s}");
            HandshakeError::Rpc(s)
        })?
        .ok_or_else(|| {
            tracing::error!("handshaker stream ended immediately without response");
            HandshakeError::UnexpectedEnd
        })?;

    let msg = format!(
        "handshaker responded: out_frames={} bytes, status={:?}, has_result={}",
        resp.out_frames.len(),
        resp.status,
        resp.result.is_some(),
    );
    tracing::info!("{msg}");
    Ok(msg)
}

#[derive(Debug)]
pub enum HandshakeError {
    /// Invalid handshaker service address.
    InvalidAddress(String, String),
    /// Failed to connect to the local handshaker service.
    ConnectHandshaker(tonic::transport::Error),
    /// gRPC error from the handshaker service.
    Rpc(tonic::Status),
    /// I/O error on the transport connection.
    Io(std::io::Error),
    /// The handshaker service returned an error status.
    HandshakerError(u32, String),
    /// The handshaker stream or transport connection ended unexpectedly.
    UnexpectedEnd,
    /// Internal channel was closed unexpectedly.
    ChannelClosed,
}

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidAddress(addr, e) => {
                write!(f, "invalid ALTS handshaker address '{addr}': {e}")
            }
            Self::ConnectHandshaker(e) => {
                write!(f, "failed to connect to ALTS handshaker service: {e}")
            }
            Self::Rpc(s) => write!(f, "ALTS handshaker RPC error: {s}"),
            Self::Io(e) => write!(f, "ALTS handshake I/O error: {e}"),
            Self::HandshakerError(code, details) => {
                write!(f, "ALTS handshaker error (code={code}): {details}")
            }
            Self::UnexpectedEnd => write!(f, "ALTS handshake stream ended unexpectedly"),
            Self::ChannelClosed => write!(f, "ALTS handshaker channel closed unexpectedly"),
        }
    }
}

impl std::error::Error for HandshakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ConnectHandshaker(e) => Some(e),
            Self::Rpc(s) => Some(s),
            Self::Io(e) => Some(e),
            Self::InvalidAddress(_, _)
            | Self::HandshakerError(_, _)
            | Self::UnexpectedEnd
            | Self::ChannelClosed => None,
        }
    }
}
