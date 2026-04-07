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

use std::time::Duration;
use tokio::sync::OnceCell;

const METADATA_FLAVOR_HEADER: &str = "Metadata-Flavor";
const METADATA_FLAVOR_VALUE: &str = "Google";

const DEFAULT_METADATA_HOST: &str = "metadata.google.internal";
const GCE_METADATA_HOST_ENV_VAR: &str = "GCE_METADATA_HOST";
const DETECTION_TIMEOUT: Duration = Duration::from_millis(500);

static ON_GCE: OnceCell<bool> = OnceCell::const_new();

fn metadata_host() -> String {
    std::env::var(GCE_METADATA_HOST_ENV_VAR).unwrap_or_else(|_| DEFAULT_METADATA_HOST.to_string())
}

/// Returns `true` if the current process is running on a Google Compute Engine
/// VM (or similar environment that provides a metadata server).
///
/// The result is cached after the first successful detection.
pub async fn is_on_gce() -> bool {
    *ON_GCE.get_or_init(|| async { detect_gce().await }).await
}

async fn detect_gce() -> bool {
    let host = metadata_host();
    let url = format!("http://{host}/computeMetadata/v1/");
    let client = match reqwest::Client::builder()
        .timeout(DETECTION_TIMEOUT)
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    let result = client
        .get(&url)
        .header(METADATA_FLAVOR_HEADER, METADATA_FLAVOR_VALUE)
        .send()
        .await;
    match result {
        Ok(resp) => resp
            .headers()
            .get(METADATA_FLAVOR_HEADER)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v == METADATA_FLAVOR_VALUE),
        Err(_) => false,
    }
}

/// Returns the zone of the current GCE instance, e.g.
/// `projects/123456/zones/us-central1-a`.
///
/// Returns `None` if not running on GCE or the metadata query fails.
pub async fn instance_zone() -> Option<String> {
    let host = metadata_host();
    let url = format!("http://{host}/computeMetadata/v1/instance/zone");
    let client = reqwest::Client::builder()
        .timeout(DETECTION_TIMEOUT)
        .build()
        .ok()?;
    let resp = client
        .get(&url)
        .header(METADATA_FLAVOR_HEADER, METADATA_FLAVOR_VALUE)
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    resp.text().await.ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_detect_gce_unreachable() {
        // Point to a non-existent host to verify quick timeout and false result.
        let _guard = scoped_env::ScopedEnv::set(GCE_METADATA_HOST_ENV_VAR, "127.0.0.1:1");
        let result = detect_gce().await;
        assert!(!result);
    }

    #[tokio::test]
    async fn test_instance_zone_unreachable() {
        let _guard = scoped_env::ScopedEnv::set(GCE_METADATA_HOST_ENV_VAR, "127.0.0.1:1");
        let result = instance_zone().await;
        assert!(result.is_none());
    }

    #[test]
    fn test_metadata_host_default() {
        let _guard = scoped_env::ScopedEnv::remove(GCE_METADATA_HOST_ENV_VAR);
        assert_eq!(metadata_host(), DEFAULT_METADATA_HOST);
    }

    #[test]
    fn test_metadata_host_env_override() {
        let _guard = scoped_env::ScopedEnv::set(GCE_METADATA_HOST_ENV_VAR, "custom.host:8080");
        assert_eq!(metadata_host(), "custom.host:8080");
    }
}
