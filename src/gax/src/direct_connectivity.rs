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

/// Controls whether direct connectivity (ALTS + DirectPath) is used for
/// gRPC connections to Google Cloud services.
///
/// Direct connectivity enables gRPC requests from Compute Engine VMs to
/// bypass Google Front Ends (GFEs) and route directly to the backend
/// service, reducing latency and connection overhead.
///
/// This requires the application to be running on a Compute Engine VM
/// that is co-located with the target resource's region.
#[derive(Clone, Debug, Default)]
pub enum DirectConnectivityMode {
    /// Automatically detect if running on GCE and enable direct connectivity
    /// if available. Falls back to standard TLS via GFEs if not on GCE.
    Auto,
    /// Force-enable direct connectivity. Connection will fail if the
    /// environment does not support direct connectivity (e.g., not on GCE).
    Enabled,
    /// Never use direct connectivity. Always use standard TLS via GFEs.
    #[default]
    Disabled,
}
