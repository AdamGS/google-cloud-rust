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

pub mod alts;
pub mod channel;
pub mod config;
pub mod gce;
pub mod xds;

#[allow(dead_code)]
pub(crate) mod proto {
    include!("../generated/protos/gcp/grpc.gcp.rs");
}

pub use config::DirectConnectivityMode;

/// Determines whether direct connectivity should be used based on
/// configuration and environment.
///
/// Returns `true` if the configuration enables direct connectivity AND
/// we are running on a GCE VM.
pub async fn should_use_direct_connectivity(mode: &DirectConnectivityMode) -> bool {
    match mode {
        DirectConnectivityMode::Disabled => false,
        DirectConnectivityMode::Enabled => true,
        DirectConnectivityMode::Auto => gce::is_on_gce().await,
    }
}
