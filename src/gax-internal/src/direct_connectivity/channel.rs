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
//! connects to those endpoints using ALTS transport security. Discovered
//! endpoints are cached for 5 minutes to avoid repeated xDS discovery
//! on every client creation.

use super::alts::connector::AltsConnector;
use super::xds;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

/// How long cached endpoints remain valid before re-discovery.
const ENDPOINT_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

struct CacheEntry {
    endpoints: Vec<xds::BackendEndpoint>,
    cached_at: Instant,
}

impl CacheEntry {
    fn is_fresh(&self) -> bool {
        self.cached_at.elapsed() < ENDPOINT_CACHE_TTL
    }
}

static ENDPOINT_CACHE: LazyLock<RwLock<HashMap<String, CacheEntry>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Creates a gRPC channel to the given service via DirectPath with ALTS.
///
/// 1. Checks the endpoint cache for a recent xDS discovery result
/// 2. If stale or missing, queries Traffic Director via xDS
/// 3. Connects to the discovered backend using ALTS transport
///
/// `target_name` is the service DNS name (e.g., `storage.googleapis.com`),
/// used for both xDS resource lookup and ALTS secure naming.
pub async fn make_direct_path_channel(
    target_name: &str,
) -> Result<Channel, DirectPathError> {
    let endpoints = resolve_endpoints(target_name).await?;
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

async fn resolve_endpoints(
    target_name: &str,
) -> Result<Vec<xds::BackendEndpoint>, DirectPathError> {
    // Check cache first.
    {
        let cache = ENDPOINT_CACHE.read().await;
        if let Some(entry) = cache.get(target_name) {
            if entry.is_fresh() {
                tracing::debug!(
                    "using cached endpoints for '{target_name}' ({} endpoints, age={:?})",
                    entry.endpoints.len(),
                    entry.cached_at.elapsed(),
                );
                return Ok(entry.endpoints.clone());
            }
            tracing::debug!(
                "cached endpoints for '{target_name}' expired (age={:?})",
                entry.cached_at.elapsed(),
            );
        }
    }

    // Cache miss or stale — run xDS discovery.
    let node = xds::build_node_from_metadata()
        .await
        .map_err(DirectPathError::Xds)?;

    let endpoints = xds::discover_endpoints(target_name, node)
        .await
        .map_err(DirectPathError::Xds)?;

    if endpoints.is_empty() {
        return Err(DirectPathError::NoEndpoints);
    }

    // Update cache.
    {
        let mut cache = ENDPOINT_CACHE.write().await;
        cache.insert(
            target_name.to_string(),
            CacheEntry {
                endpoints: endpoints.clone(),
                cached_at: Instant::now(),
            },
        );
    }
    tracing::info!(
        "cached {} endpoints for '{target_name}' (ttl={:?})",
        endpoints.len(),
        ENDPOINT_CACHE_TTL,
    );

    Ok(endpoints)
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
