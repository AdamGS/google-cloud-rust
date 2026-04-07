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

//! Integration tests for GCP direct connectivity (ALTS + DirectPath).
//!
//! These tests MUST be run on a GCE VM that is co-located with the target
//! bucket's region. They verify the full direct connectivity stack:
//!
//! - GCE metadata server detection
//! - ALTS handshake with the local handshaker service
//! - DirectPath routing to Cloud Storage
//!
//! # Environment Variables
//!
//! - `GOOGLE_CLOUD_RUST_TEST_BUCKET` (required) - Bucket ID (without
//!   `projects/_/buckets/` prefix) to use for read/write tests. Must be
//!   in the same region as the VM.
//!
//! # Running
//!
//! ```bash
//! GOOGLE_CLOUD_RUST_TEST_BUCKET=my-bucket \
//! RUSTFLAGS='--cfg google_cloud_unstable_direct_connectivity' \
//!   cargo test -p integration-tests-direct-connectivity \
//!     --features run-integration-tests
//! ```

use anyhow::{Context, Result};

fn bucket_name() -> Result<String> {
    let id = std::env::var("GOOGLE_CLOUD_RUST_TEST_BUCKET").context(
        "GOOGLE_CLOUD_RUST_TEST_BUCKET must be set to a bucket ID in the same region as this VM",
    )?;
    Ok(format!("projects/_/buckets/{id}"))
}

/// Verify that GCE metadata server detection works on this VM.
#[cfg(google_cloud_unstable_direct_connectivity)]
pub async fn gce_detection() -> Result<()> {
    use google_cloud_gax_internal::direct_connectivity::gce;

    tracing::info!("testing GCE detection");
    let on_gce = gce::is_on_gce().await;
    tracing::info!("is_on_gce() = {on_gce}");
    anyhow::ensure!(on_gce, "expected to be running on GCE");

    tracing::info!("testing instance zone");
    let zone = gce::instance_zone().await;
    tracing::info!("instance_zone() = {zone:?}");
    anyhow::ensure!(zone.is_some(), "expected to get instance zone on GCE");
    let zone = zone.unwrap();
    anyhow::ensure!(
        zone.contains("/zones/"),
        "zone should contain '/zones/', got: {zone}"
    );

    tracing::info!("GCE detection: PASSED");
    Ok(())
}

/// Verify that direct connectivity config resolves correctly on GCE.
#[cfg(google_cloud_unstable_direct_connectivity)]
pub async fn config_resolution() -> Result<()> {
    use google_cloud_gax::direct_connectivity::DirectConnectivityMode;
    use google_cloud_gax_internal::direct_connectivity;

    tracing::info!("testing config resolution with Auto mode");
    let should_use =
        direct_connectivity::should_use_direct_connectivity(&DirectConnectivityMode::Auto).await;
    tracing::info!("should_use_direct_connectivity(Auto) = {should_use}");
    anyhow::ensure!(
        should_use,
        "Auto mode should enable direct connectivity on GCE"
    );

    tracing::info!("testing config resolution with Disabled mode");
    let should_use =
        direct_connectivity::should_use_direct_connectivity(&DirectConnectivityMode::Disabled)
            .await;
    anyhow::ensure!(
        !should_use,
        "Disabled mode should not enable direct connectivity"
    );

    tracing::info!("config resolution: PASSED");
    Ok(())
}

/// Verify that we can write and read an object via the standard TLS path
/// (baseline test to confirm the bucket/credentials are working).
pub async fn baseline_storage_operations() -> Result<()> {
    use google_cloud_storage::client::Storage;

    let bucket = bucket_name()?;
    tracing::info!("baseline test: writing/reading object via standard TLS");

    let client = Storage::builder().build().await?;

    let object_name = format!("dc-test/baseline-{}", std::process::id());
    let contents = "direct connectivity baseline test";

    tracing::info!("writing object: {object_name}");
    client
        .write_object(&bucket, &object_name, contents)
        .send_unbuffered()
        .await
        .context("baseline write_object failed")?;

    tracing::info!("reading object: {object_name}");
    let mut response = client
        .read_object(&bucket, &object_name)
        .send()
        .await
        .context("baseline read_object failed")?;

    let mut data = Vec::new();
    while let Some(chunk) = response.next().await.transpose()? {
        data.extend_from_slice(&chunk);
    }
    anyhow::ensure!(
        data == contents.as_bytes(),
        "baseline read data mismatch: got {} bytes, expected {}",
        data.len(),
        contents.len()
    );

    tracing::info!("baseline storage operations: PASSED");
    Ok(())
}

/// Verify that Storage operations work via DirectPath with ALTS.
///
/// This is the core end-to-end test. It creates a Storage client with
/// direct connectivity enabled and performs a write + read cycle.
#[cfg(google_cloud_unstable_direct_connectivity)]
pub async fn direct_path_storage_operations() -> Result<()> {
    use google_cloud_gax::direct_connectivity::DirectConnectivityMode;
    use google_cloud_storage::client::Storage;

    let bucket = bucket_name()?;
    tracing::info!("DirectPath test: writing/reading object via ALTS + DirectPath");

    // Build client with direct connectivity forced on.
    // This will fail if ALTS handshake or DirectPath connection fails.
    let client = Storage::builder()
        .with_direct_connectivity(DirectConnectivityMode::Enabled)
        .build()
        .await
        .context("failed to build Storage client with direct connectivity")?;

    let object_name = format!("dc-test/directpath-{}", std::process::id());
    let contents = "direct connectivity end-to-end test via DirectPath + ALTS";

    tracing::info!("writing object via DirectPath: {object_name}");
    let insert = client
        .write_object(&bucket, &object_name, contents)
        .send_unbuffered()
        .await
        .context("DirectPath write_object failed")?;
    tracing::info!("write succeeded: generation={}", insert.generation);

    tracing::info!("reading object via DirectPath: {object_name}");
    let mut response = client
        .read_object(&bucket, &object_name)
        .send()
        .await
        .context("DirectPath read_object failed")?;

    let mut data = Vec::new();
    while let Some(chunk) = response.next().await.transpose()? {
        data.extend_from_slice(&chunk);
    }
    anyhow::ensure!(
        data == contents.as_bytes(),
        "DirectPath read data mismatch: got {} bytes, expected {}",
        data.len(),
        contents.len()
    );

    tracing::info!("DirectPath storage operations: PASSED");
    Ok(())
}

/// Verify that Auto mode falls back gracefully when DirectPath fails.
///
/// This test uses Auto mode, which should succeed regardless of whether
/// DirectPath is available (it falls back to standard TLS).
#[cfg(google_cloud_unstable_direct_connectivity)]
pub async fn auto_mode_fallback() -> Result<()> {
    use google_cloud_gax::direct_connectivity::DirectConnectivityMode;
    use google_cloud_storage::client::Storage;

    let bucket = bucket_name()?;
    tracing::info!("fallback test: Auto mode should work regardless of DirectPath availability");

    let client = Storage::builder()
        .with_direct_connectivity(DirectConnectivityMode::Auto)
        .build()
        .await
        .context("failed to build Storage client with Auto mode")?;

    let object_name = format!("dc-test/auto-{}", std::process::id());
    let contents = "auto mode fallback test";

    client
        .write_object(&bucket, &object_name, contents)
        .send_unbuffered()
        .await
        .context("Auto mode write_object failed")?;

    let mut response = client
        .read_object(&bucket, &object_name)
        .send()
        .await
        .context("Auto mode read_object failed")?;

    let mut data = Vec::new();
    while let Some(chunk) = response.next().await.transpose()? {
        data.extend_from_slice(&chunk);
    }
    anyhow::ensure!(
        data == contents.as_bytes(),
        "Auto mode read data mismatch"
    );

    tracing::info!("Auto mode fallback: PASSED");
    Ok(())
}
