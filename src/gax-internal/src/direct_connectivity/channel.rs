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

//! DirectPath channel factory.
//!
//! Uses xDS to discover backend endpoints from Traffic Director, then
//! connects to those endpoints using ALTS transport security.

use super::alts::connector::AltsConnector;
use super::xds;
use tonic::transport::{Channel, Endpoint};

/// Creates a gRPC channel to the given service via DirectPath with ALTS.
///
/// 1. Queries Traffic Director via xDS to discover backend endpoint IPs
/// 2. Connects to the discovered backend using ALTS transport
///
/// `target_name` is the service DNS name (e.g., `storage.googleapis.com`),
/// used for both xDS resource lookup and ALTS secure naming.
pub async fn make_direct_path_channel(
    target_name: &str,
) -> Result<Channel, DirectPathError> {
    // Build node identity from GCE metadata.
    let node = xds::build_node_from_metadata()
        .await
        .map_err(DirectPathError::Xds)?;

    // Discover backend endpoints from Traffic Director.
    let endpoints = xds::discover_endpoints(target_name, node)
        .await
        .map_err(DirectPathError::Xds)?;

    if endpoints.is_empty() {
        return Err(DirectPathError::NoEndpoints);
    }

    // Connect to the first discovered endpoint using ALTS.
    let ep = &endpoints[0];
    let uri = format!("http://{}:{}", ep.address, ep.port);
    tracing::info!(
        "connecting to DirectPath backend at {uri} with ALTS (target={target_name})"
    );

    let connector = AltsConnector::new(target_name);
    let endpoint = Endpoint::from_shared(uri)
        .map_err(|e| DirectPathError::InvalidEndpoint(e.to_string()))?
        .concurrency_limit(100);
    endpoint
        .connect_with_connector(connector)
        .await
        .map_err(DirectPathError::Connect)
}

#[derive(Debug)]
pub enum DirectPathError {
    Xds(xds::XdsError),
    NoEndpoints,
    InvalidEndpoint(String),
    Connect(tonic::transport::Error),
}

impl std::fmt::Display for DirectPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Xds(e) => write!(f, "DirectPath xDS discovery failed: {e}"),
            Self::NoEndpoints => write!(f, "DirectPath: xDS returned no endpoints"),
            Self::InvalidEndpoint(e) => write!(f, "DirectPath: invalid endpoint: {e}"),
            Self::Connect(e) => write!(f, "DirectPath: ALTS connection failed: {e}"),
        }
    }
}

impl std::error::Error for DirectPathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Xds(e) => Some(e),
            Self::Connect(e) => Some(e),
            _ => None,
        }
    }
}
