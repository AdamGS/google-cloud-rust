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

#[cfg(all(
    test,
    feature = "run-integration-tests",
    google_cloud_unstable_direct_connectivity
))]
mod direct_connectivity {
    use google_cloud_test_utils::errors::anydump;

    fn enable_tracing() -> tracing::subscriber::DefaultGuard {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_target(false)
            .finish();
        tracing::subscriber::set_default(subscriber)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_gce_detection() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_direct_connectivity::gce_detection()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_config_resolution() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_direct_connectivity::config_resolution()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_baseline_storage_operations() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_direct_connectivity::baseline_storage_operations()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_direct_path_storage_operations() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_direct_connectivity::direct_path_storage_operations()
            .await
            .inspect_err(anydump)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_auto_mode_fallback() -> anyhow::Result<()> {
        let _guard = enable_tracing();
        integration_tests_direct_connectivity::auto_mode_fallback()
            .await
            .inspect_err(anydump)
    }
}
