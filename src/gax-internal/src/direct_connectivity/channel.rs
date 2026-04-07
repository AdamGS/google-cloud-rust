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
//! Creates gRPC channels that connect to Google Cloud services via
//! DirectPath (bypassing Google Front Ends) using ALTS transport security.

use super::alts::connector::AltsConnector;
use tonic::transport::{Channel, Endpoint};

const DIRECT_PATH_ENDPOINT: &str = "https://directpath-pa.googleapis.com";

/// Creates a gRPC channel to the given service via DirectPath with ALTS.
///
/// The channel connects to `directpath-pa.googleapis.com:443` using ALTS
/// for transport security instead of standard TLS.
///
/// `target_name` is used for ALTS secure naming verification (typically
/// the service's DNS name, e.g., `storage.googleapis.com`).
pub async fn make_direct_path_channel(
    target_name: &str,
) -> Result<Channel, tonic::transport::Error> {
    let connector = AltsConnector::new(target_name);
    let endpoint = Endpoint::from_static(DIRECT_PATH_ENDPOINT).concurrency_limit(100);
    endpoint.connect_with_connector(connector).await
}
