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

//! Minimal xDS endpoint discovery for DirectPath.
//!
//! Connects to Traffic Director via ADS and follows the
//! LDS → CDS → EDS chain to discover backend endpoint IPs
//! for a given service.

use super::types::*;
use prost::Message;
use std::net::SocketAddr;
use tokio_stream::wrappers::ReceiverStream;

const TRAFFIC_DIRECTOR_ADDR: &str = "https://directpath-pa.googleapis.com";

/// A discovered backend endpoint (IP + port).
#[derive(Debug, Clone)]
pub struct BackendEndpoint {
    pub address: String,
    pub port: u32,
}

impl BackendEndpoint {
    pub fn to_socket_addr(&self) -> Option<SocketAddr> {
        format!("{}:{}", self.address, self.port).parse().ok()
    }
}

/// Discovers backend endpoints for the given service via xDS.
///
/// Connects to Traffic Director at `directpath-pa.googleapis.com:443`,
/// follows the LDS → CDS → EDS discovery chain, and returns the
/// backend endpoint addresses.
pub async fn discover_endpoints(
    service_name: &str,
    node: Node,
) -> Result<Vec<BackendEndpoint>, XdsError> {
    tracing::info!(
        "starting xDS endpoint discovery for '{service_name}'"
    );

    // Connect to Traffic Director with TLS.
    let channel = tonic::transport::Channel::from_static(TRAFFIC_DIRECTOR_ADDR)
        .tls_config(
            tonic::transport::ClientTlsConfig::new().with_enabled_roots(),
        )
        .map_err(|e| XdsError::Transport(e.to_string()))?
        .connect()
        .await
        .map_err(|e| XdsError::Transport(e.to_string()))?;
    tracing::debug!("connected to Traffic Director");

    // We need to inject auth headers. Get credentials.
    let creds = google_cloud_auth::credentials::Builder::default()
        .build()
        .map_err(|e| XdsError::Auth(e.to_string()))?;

    let (tx, rx) = tokio::sync::mpsc::channel::<DiscoveryRequest>(8);
    let request_stream = ReceiverStream::new(rx);

    // The ADS service path is:
    // /envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources
    //
    // We manually construct the gRPC call since we don't have the
    // generated ADS client.
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready()
        .await
        .map_err(|e| XdsError::Transport(e.to_string()))?;

    // Add auth header.
    use google_cloud_auth::credentials::CacheableResource;
    let headers = creds
        .headers(http::Extensions::new())
        .await
        .map_err(|e| XdsError::Auth(e.to_string()))?;
    let auth_headers = match headers {
        CacheableResource::New { data, .. } => data,
        _ => http::HeaderMap::new(),
    };

    let mut request = tonic::Request::new(request_stream);
    for (k, v) in auth_headers.iter() {
        if let Ok(s) = v.to_str() {
            let key = k.as_str().to_string();
            if let Ok(mv) = s.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>() {
                if let Ok(mk) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
                    request.metadata_mut().insert(mk, mv);
                }
            }
        }
    }
    drop(auth_headers);

    let codec = tonic_prost::ProstCodec::default();
    let path = http::uri::PathAndQuery::from_static(
        "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources",
    );

    let response: tonic::Response<tonic::Streaming<DiscoveryResponse>> = grpc
        .streaming(request, path, codec)
        .await
        .map_err(|s: tonic::Status| {
            tracing::error!("ADS StreamAggregatedResources failed: {s}");
            XdsError::Rpc(s.to_string())
        })?;
    let mut response_stream = response.into_inner();

    // Step 1: LDS - discover the listener for our service
    tracing::debug!("sending LDS request for '{service_name}'");
    let lds_req = DiscoveryRequest {
        node: Some(node.clone()),
        type_url: LDS_TYPE_URL.to_string(),
        resource_names: vec![service_name.to_string()],
        ..Default::default()
    };
    tx.send(lds_req)
        .await
        .map_err(|_| XdsError::ChannelClosed)?;

    let lds_resp = recv_response(&mut response_stream, LDS_TYPE_URL).await?;
    let cluster_name = extract_cluster_from_lds(&lds_resp)?;
    tracing::info!("LDS resolved to cluster: '{cluster_name}'");

    // ACK the LDS response
    let lds_ack = DiscoveryRequest {
        node: Some(node.clone()),
        type_url: LDS_TYPE_URL.to_string(),
        resource_names: vec![service_name.to_string()],
        version_info: lds_resp.version_info.clone(),
        response_nonce: lds_resp.nonce.clone(),
        ..Default::default()
    };
    tx.send(lds_ack)
        .await
        .map_err(|_| XdsError::ChannelClosed)?;

    // Step 2: CDS - discover the cluster config
    tracing::debug!("sending CDS request for '{cluster_name}'");
    let cds_req = DiscoveryRequest {
        node: Some(node.clone()),
        type_url: CDS_TYPE_URL.to_string(),
        resource_names: vec![cluster_name.clone()],
        ..Default::default()
    };
    tx.send(cds_req)
        .await
        .map_err(|_| XdsError::ChannelClosed)?;

    let cds_resp = recv_response(&mut response_stream, CDS_TYPE_URL).await?;
    let eds_service_name = extract_eds_name_from_cds(&cds_resp, &cluster_name)?;
    tracing::info!("CDS resolved to EDS service: '{eds_service_name}'");

    // ACK the CDS response
    let cds_ack = DiscoveryRequest {
        node: Some(node.clone()),
        type_url: CDS_TYPE_URL.to_string(),
        resource_names: vec![cluster_name.clone()],
        version_info: cds_resp.version_info.clone(),
        response_nonce: cds_resp.nonce.clone(),
        ..Default::default()
    };
    tx.send(cds_ack)
        .await
        .map_err(|_| XdsError::ChannelClosed)?;

    // Step 3: EDS - discover backend endpoints
    tracing::debug!("sending EDS request for '{eds_service_name}'");
    let eds_req = DiscoveryRequest {
        node: Some(node.clone()),
        type_url: EDS_TYPE_URL.to_string(),
        resource_names: vec![eds_service_name.clone()],
        ..Default::default()
    };
    tx.send(eds_req)
        .await
        .map_err(|_| XdsError::ChannelClosed)?;

    let eds_resp = recv_response(&mut response_stream, EDS_TYPE_URL).await?;
    let endpoints = extract_endpoints_from_eds(&eds_resp)?;
    tracing::info!("EDS discovered {} endpoints", endpoints.len());
    for ep in &endpoints {
        tracing::debug!("  endpoint: {}:{}", ep.address, ep.port);
    }

    Ok(endpoints)
}

/// Build a Node message from GCE metadata.
pub async fn build_node_from_metadata() -> Result<Node, XdsError> {
    let zone = super::super::gce::instance_zone()
        .await
        .ok_or_else(|| XdsError::Metadata("failed to get instance zone".into()))?;

    // Zone format: "projects/{project_number}/zones/{zone_name}"
    let parts: Vec<&str> = zone.split('/').collect();
    let (project_number, zone_name) = if parts.len() >= 4 {
        (parts[1].to_string(), parts[3].to_string())
    } else {
        return Err(XdsError::Metadata(format!("unexpected zone format: {zone}")));
    };

    // Extract region from zone (e.g., "us-central1-f" -> "us-central1")
    let region = zone_name
        .rfind('-')
        .map(|i| &zone_name[..i])
        .unwrap_or(&zone_name)
        .to_string();

    let node_id = format!(
        "projects/{project_number}/networks/default/nodes/{}",
        uuid_v4()
    );

    let metadata = prost_types::Struct {
        fields: [
            (
                "TRAFFICDIRECTOR_GCP_PROJECT_NUMBER".to_string(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue(
                        project_number,
                    )),
                },
            ),
            (
                "TRAFFICDIRECTOR_NETWORK_NAME".to_string(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue(
                        "default".to_string(),
                    )),
                },
            ),
        ]
        .into_iter()
        .collect(),
    };

    Ok(Node {
        id: node_id,
        cluster: String::new(),
        metadata: Some(metadata),
        locality: Some(Locality { region, zone: zone_name }),
    })
}

// ── Internal helpers ──

async fn recv_response(
    stream: &mut tonic::Streaming<DiscoveryResponse>,
    expected_type: &str,
) -> Result<DiscoveryResponse, XdsError> {
    // Traffic Director may send responses for different types on the
    // same ADS stream. We read until we get the one we want.
    for _ in 0..10 {
        let resp = stream
            .message()
            .await
            .map_err(|s| {
                tracing::error!("ADS stream error waiting for {expected_type}: {s}");
                XdsError::Rpc(s.to_string())
            })?
            .ok_or_else(|| {
                tracing::error!("ADS stream ended waiting for {expected_type}");
                XdsError::StreamEnded
            })?;
        tracing::debug!(
            "received xDS response: type={}, resources={}, version={}, nonce={}",
            resp.type_url,
            resp.resources.len(),
            resp.version_info,
            resp.nonce,
        );
        if resp.type_url == expected_type {
            return Ok(resp);
        }
        tracing::debug!(
            "skipping response type '{}', waiting for '{expected_type}'",
            resp.type_url
        );
    }
    Err(XdsError::NoResponse(expected_type.to_string()))
}

fn extract_cluster_from_lds(resp: &DiscoveryResponse) -> Result<String, XdsError> {
    for any in &resp.resources {
        let listener = Listener::decode(any.value.as_ref())
            .map_err(|e| XdsError::Decode(format!("Listener: {e}")))?;

        tracing::debug!("LDS listener name: '{}'", listener.name);

        let api_listener = listener
            .api_listener
            .and_then(|al| al.api_listener)
            .ok_or_else(|| XdsError::Decode("Listener missing api_listener".into()))?;

        if api_listener.type_url == HTTP_CONNECTION_MANAGER_TYPE_URL {
            let hcm = HttpConnectionManager::decode(api_listener.value.as_ref())
                .map_err(|e| XdsError::Decode(format!("HttpConnectionManager: {e}")))?;

            // Try inline route config first.
            if let Some(rc) = &hcm.route_config {
                if let Some(cluster) = first_cluster_from_route_config(rc) {
                    return Ok(cluster);
                }
            }
            // If RDS is configured, use the route_config_name as cluster name
            // (simplification — real impl would do an RDS request).
            if let Some(rds) = &hcm.rds {
                tracing::debug!("LDS uses RDS with route_config_name: '{}'", rds.route_config_name);
                // Fall back to using the service name as the cluster name.
                // This is a simplification.
            }
        }
    }
    Err(XdsError::Decode(
        "no cluster found in LDS response".into(),
    ))
}

fn first_cluster_from_route_config(rc: &RouteConfiguration) -> Option<String> {
    for vh in &rc.virtual_hosts {
        for route in &vh.routes {
            if let Some(action) = &route.route {
                if !action.cluster.is_empty() {
                    return Some(action.cluster.clone());
                }
            }
        }
    }
    None
}

fn extract_eds_name_from_cds(
    resp: &DiscoveryResponse,
    cluster_name: &str,
) -> Result<String, XdsError> {
    for any in &resp.resources {
        let cluster = Cluster::decode(any.value.as_ref())
            .map_err(|e| XdsError::Decode(format!("Cluster: {e}")))?;

        tracing::debug!("CDS cluster name: '{}'", cluster.name);

        if let Some(eds_config) = &cluster.eds_cluster_config {
            let name = if eds_config.service_name.is_empty() {
                cluster.name.clone()
            } else {
                eds_config.service_name.clone()
            };
            return Ok(name);
        }
        // No EDS config — use the cluster name itself.
        return Ok(cluster.name);
    }
    Err(XdsError::Decode(format!(
        "cluster '{cluster_name}' not found in CDS response"
    )))
}

fn extract_endpoints_from_eds(resp: &DiscoveryResponse) -> Result<Vec<BackendEndpoint>, XdsError> {
    let mut endpoints = Vec::new();
    for any in &resp.resources {
        let cla = ClusterLoadAssignment::decode(any.value.as_ref())
            .map_err(|e| XdsError::Decode(format!("ClusterLoadAssignment: {e}")))?;

        tracing::debug!(
            "EDS cluster '{}': {} locality groups",
            cla.cluster_name,
            cla.endpoints.len()
        );

        for locality in &cla.endpoints {
            for lb_ep in &locality.lb_endpoints {
                if let Some(ep) = &lb_ep.endpoint {
                    if let Some(addr) = &ep.address {
                        if let Some(sa) = &addr.socket_address {
                            endpoints.push(BackendEndpoint {
                                address: sa.address.clone(),
                                port: sa.port_value,
                            });
                        }
                    }
                }
            }
        }
    }
    if endpoints.is_empty() {
        return Err(XdsError::Decode("no endpoints in EDS response".into()));
    }
    Ok(endpoints)
}

fn uuid_v4() -> String {
    use rand::RngExt;
    let mut rng = rand::rng();
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 1
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        u16::from_be_bytes([bytes[4], bytes[5]]),
        u16::from_be_bytes([bytes[6], bytes[7]]),
        u16::from_be_bytes([bytes[8], bytes[9]]),
        u64::from_be_bytes([0, 0, bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]]),
    )
}

#[derive(Debug)]
pub enum XdsError {
    Transport(String),
    Auth(String),
    Rpc(String),
    Decode(String),
    Metadata(String),
    StreamEnded,
    NoResponse(String),
    ChannelClosed,
}

impl std::fmt::Display for XdsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "xDS transport error: {e}"),
            Self::Auth(e) => write!(f, "xDS auth error: {e}"),
            Self::Rpc(e) => write!(f, "xDS RPC error: {e}"),
            Self::Decode(e) => write!(f, "xDS decode error: {e}"),
            Self::Metadata(e) => write!(f, "xDS metadata error: {e}"),
            Self::StreamEnded => write!(f, "xDS ADS stream ended unexpectedly"),
            Self::NoResponse(t) => write!(f, "no xDS response received for {t}"),
            Self::ChannelClosed => write!(f, "xDS request channel closed"),
        }
    }
}

impl std::error::Error for XdsError {}
