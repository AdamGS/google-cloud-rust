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

//! Benchmark comparing Cloud Storage latency across connection modes.
//!
//! NOTE: Storage `write_object` and `read_object` use the HTTP/JSON API,
//! NOT gRPC. DirectPath only affects the gRPC channel. To measure the
//! actual DirectPath benefit, this benchmark uses `open_object` (gRPC
//! bidi streaming) for reads when available.
//!
//! # Usage
//!
//! ```bash
//! GOOGLE_CLOUD_RUST_TEST_BUCKET=my-bucket \
//! RUSTFLAGS='--cfg google_cloud_unstable_direct_connectivity' \
//!   cargo run --release -p integration-tests-direct-connectivity \
//!     --bin dc-bench -- [--iterations N] [--size BYTES]
//! ```

use anyhow::{Context, Result};
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use std::time::{Duration, Instant};

#[cfg(google_cloud_unstable_direct_connectivity)]
use google_cloud_gax::direct_connectivity::DirectConnectivityMode;

fn bucket_name() -> Result<String> {
    let id = std::env::var("GOOGLE_CLOUD_RUST_TEST_BUCKET").context(
        "GOOGLE_CLOUD_RUST_TEST_BUCKET must be set",
    )?;
    Ok(format!("projects/_/buckets/{id}"))
}

struct BenchConfig {
    iterations: usize,
    payload_size: usize,
}

impl BenchConfig {
    fn from_args() -> Self {
        let mut iterations = 20;
        let mut payload_size = 1024;
        let args: Vec<String> = std::env::args().collect();
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--iterations" | "-n" => {
                    i += 1;
                    if i < args.len() {
                        iterations = args[i].parse().unwrap_or(iterations);
                    }
                }
                "--size" | "-s" => {
                    i += 1;
                    if i < args.len() {
                        payload_size = args[i].parse().unwrap_or(payload_size);
                    }
                }
                "--help" | "-h" => {
                    eprintln!("Usage: dc-bench [--iterations N] [--size BYTES]");
                    eprintln!("  --iterations, -n  Number of iterations (default: 20)");
                    eprintln!("  --size, -s        Payload size in bytes (default: 1024)");
                    std::process::exit(0);
                }
                _ => {}
            }
            i += 1;
        }
        Self {
            iterations,
            payload_size,
        }
    }
}

struct BenchResult {
    mode: String,
    times: Vec<Duration>,
    connect_time: Duration,
    error: Option<String>,
}

impl BenchResult {
    fn print(&self) {
        if let Some(err) = &self.error {
            println!("  {:<20}  FAILED: {err}", self.mode);
            return;
        }
        let p50 = percentile(&self.times, 50);
        let p99 = percentile(&self.times, 99);
        let mean = self.times.iter().sum::<Duration>() / self.times.len() as u32;
        println!(
            "  {:<20}  connect={:>8.1?}  p50={:>8.1?}  p99={:>8.1?}  mean={:>8.1?}  n={}",
            self.mode, self.connect_time, p50, p99, mean, self.times.len(),
        );
    }
}

fn percentile(times: &[Duration], pct: usize) -> Duration {
    if times.is_empty() {
        return Duration::ZERO;
    }
    let mut sorted = times.to_vec();
    sorted.sort();
    let idx = (pct * sorted.len() / 100).min(sorted.len() - 1);
    sorted[idx]
}

/// Seed a test object via HTTP (works regardless of DirectPath).
async fn seed_object(bucket: &str, name: &str, size: usize) -> Result<()> {
    let client = Storage::builder().build().await?;
    let payload = "x".repeat(size);
    client
        .write_object(bucket, name, payload)
        .send_unbuffered()
        .await?;
    Ok(())
}

/// Benchmark: HTTP read_object (does NOT use gRPC / DirectPath).
async fn bench_http_read(bucket: &str, object: &str, config: &BenchConfig) -> BenchResult {
    let t0 = Instant::now();
    let client = match Storage::builder().build().await {
        Ok(c) => c,
        Err(e) => {
            return BenchResult {
                mode: "HTTP (read_object)".into(),
                times: vec![],
                connect_time: t0.elapsed(),
                error: Some(e.to_string()),
            };
        }
    };
    let connect_time = t0.elapsed();
    let mut times = Vec::with_capacity(config.iterations);

    for _ in 0..config.iterations {
        let t = Instant::now();
        match client.read_object(bucket, object).send().await {
            Ok(mut resp) => {
                while let Some(chunk) = resp.next().await {
                    let _ = chunk;
                }
                times.push(t.elapsed());
            }
            Err(e) => {
                return BenchResult {
                    mode: "HTTP (read_object)".into(),
                    times,
                    connect_time,
                    error: Some(e.to_string()),
                };
            }
        }
    }
    BenchResult {
        mode: "HTTP (read_object)".into(),
        times,
        connect_time,
        error: None,
    }
}

/// Benchmark: gRPC open_object read with a specific DirectConnectivity mode.
#[cfg(google_cloud_unstable_direct_connectivity)]
async fn bench_grpc_read(
    mode_name: &str,
    mode: DirectConnectivityMode,
    bucket: &str,
    object: &str,
    config: &BenchConfig,
) -> BenchResult {
    let label = format!("gRPC {mode_name}");
    let t0 = Instant::now();
    let client = match Storage::builder()
        .with_direct_connectivity(mode)
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return BenchResult {
                mode: label,
                times: vec![],
                connect_time: t0.elapsed(),
                error: Some(e.to_string()),
            };
        }
    };
    let connect_time = t0.elapsed();
    let mut times = Vec::with_capacity(config.iterations);

    for _ in 0..config.iterations {
        let t = Instant::now();
        match client
            .open_object(bucket, object)
            .send_and_read(ReadRange::all())
            .await
        {
            Ok((_desc, mut reader)) => {
                while let Some(chunk) = reader.next().await {
                    let _ = chunk;
                }
                times.push(t.elapsed());
            }
            Err(e) => {
                return BenchResult {
                    mode: label,
                    times,
                    connect_time,
                    error: Some(e.to_string()),
                };
            }
        }
    }
    BenchResult {
        mode: label,
        times,
        connect_time,
        error: None,
    }
}

/// Benchmark: gRPC open_object read WITHOUT DirectPath (Disabled mode).
#[cfg(google_cloud_unstable_direct_connectivity)]
async fn bench_grpc_read_standard(
    bucket: &str,
    object: &str,
    config: &BenchConfig,
) -> BenchResult {
    bench_grpc_read("Disabled", DirectConnectivityMode::Disabled, bucket, object, config).await
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_target(false)
        .init();

    let config = BenchConfig::from_args();
    let bucket = bucket_name()?;
    let object_name = format!("dc-bench/payload-{}", std::process::id());

    println!("Direct Connectivity Benchmark");
    println!("  bucket:     {bucket}");
    println!("  iterations: {}", config.iterations);
    println!("  payload:    {} bytes", config.payload_size);
    println!();

    // Seed the test object via HTTP.
    println!("seeding test object...");
    seed_object(&bucket, &object_name, config.payload_size).await?;

    // Warmup.
    println!("warming up...");
    let warmup = BenchConfig {
        iterations: 2,
        payload_size: config.payload_size,
    };
    let _ = bench_http_read(&bucket, &object_name, &warmup).await;

    println!();
    println!("--- HTTP transport (read_object) ---");
    println!("  (DirectPath has NO effect here — this is the JSON API baseline)");
    let http = bench_http_read(&bucket, &object_name, &config).await;
    http.print();

    #[cfg(google_cloud_unstable_direct_connectivity)]
    {
        println!();
        println!("--- gRPC transport (open_object / BidiStreamingRead) ---");
        println!("  (DirectPath affects this path)");

        let grpc_disabled = bench_grpc_read_standard(&bucket, &object_name, &config).await;
        grpc_disabled.print();

        let grpc_auto = bench_grpc_read(
            "Auto",
            DirectConnectivityMode::Auto,
            &bucket,
            &object_name,
            &config,
        )
        .await;
        grpc_auto.print();

        let grpc_enabled = bench_grpc_read(
            "Enabled",
            DirectConnectivityMode::Enabled,
            &bucket,
            &object_name,
            &config,
        )
        .await;
        grpc_enabled.print();
    }

    println!();
    Ok(())
}
