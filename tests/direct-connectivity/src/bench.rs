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

//! Benchmark comparing Cloud Storage latency across connection modes:
//!
//! - **Disabled**: Standard TLS via Google Front Ends (baseline)
//! - **Enabled**: DirectPath with ALTS (bypasses GFEs)
//! - **Auto**: Automatically selects DirectPath if available
//!
//! # Usage
//!
//! ```bash
//! GOOGLE_CLOUD_RUST_TEST_BUCKET=my-bucket \
//! RUSTFLAGS='--cfg google_cloud_unstable_direct_connectivity' \
//!   cargo run -p integration-tests-direct-connectivity \
//!     --bin dc-bench -- [--iterations N] [--size BYTES]
//! ```

use anyhow::{Context, Result};
use std::time::{Duration, Instant};

#[cfg(google_cloud_unstable_direct_connectivity)]
use google_cloud_gax::direct_connectivity::DirectConnectivityMode;
use google_cloud_storage::client::Storage;

fn bucket_name() -> Result<String> {
    let id = std::env::var("GOOGLE_CLOUD_RUST_TEST_BUCKET").context(
        "GOOGLE_CLOUD_RUST_TEST_BUCKET must be set to a bucket ID in the same region as this VM",
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
                    eprintln!("  --iterations, -n  Number of read/write iterations (default: 20)");
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
    write_times: Vec<Duration>,
    read_times: Vec<Duration>,
    connect_time: Duration,
    error: Option<String>,
}

impl BenchResult {
    fn print(&self) {
        if let Some(err) = &self.error {
            println!("  {:<12}  FAILED: {err}", self.mode);
            return;
        }
        let write_p50 = percentile(&self.write_times, 50);
        let write_p99 = percentile(&self.write_times, 99);
        let read_p50 = percentile(&self.read_times, 50);
        let read_p99 = percentile(&self.read_times, 99);
        println!(
            "  {:<12}  connect={:>8.1?}  write p50={:>8.1?} p99={:>8.1?}  read p50={:>8.1?} p99={:>8.1?}  n={}",
            self.mode,
            self.connect_time,
            write_p50,
            write_p99,
            read_p50,
            read_p99,
            self.write_times.len(),
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

async fn bench_standard(bucket: &str, config: &BenchConfig) -> BenchResult {
    let t0 = Instant::now();
    let client = match Storage::builder().build().await {
        Ok(c) => c,
        Err(e) => {
            return BenchResult {
                mode: "Disabled".into(),
                write_times: vec![],
                read_times: vec![],
                connect_time: t0.elapsed(),
                error: Some(format!("{e}")),
            };
        }
    };
    let connect_time = t0.elapsed();

    let payload: String = "x".repeat(config.payload_size);
    let object_name = format!("dc-bench/standard-{}", std::process::id());
    let mut write_times = Vec::with_capacity(config.iterations);
    let mut read_times = Vec::with_capacity(config.iterations);

    for i in 0..config.iterations {
        let name = format!("{object_name}-{i}");
        let data = payload.clone();

        let t = Instant::now();
        if let Err(e) = client
            .write_object(bucket, &name, data)
            .send_unbuffered()
            .await
        {
            return BenchResult {
                mode: "Disabled".into(),
                write_times,
                read_times,
                connect_time,
                error: Some(format!("write #{i}: {e}")),
            };
        }
        write_times.push(t.elapsed());

        let t = Instant::now();
        match client.read_object(bucket, &name).send().await {
            Ok(mut resp) => {
                while let Some(chunk) = resp.next().await {
                    let _ = chunk;
                }
                read_times.push(t.elapsed());
            }
            Err(e) => {
                return BenchResult {
                    mode: "Disabled".into(),
                    write_times,
                    read_times,
                    connect_time,
                    error: Some(format!("read #{i}: {e}")),
                };
            }
        }
    }

    BenchResult {
        mode: "Disabled".into(),
        write_times,
        read_times,
        connect_time,
        error: None,
    }
}

#[cfg(google_cloud_unstable_direct_connectivity)]
async fn bench_mode(
    mode_name: &str,
    mode: DirectConnectivityMode,
    bucket: &str,
    config: &BenchConfig,
) -> BenchResult {
    let t0 = Instant::now();
    let client = match Storage::builder()
        .with_direct_connectivity(mode)
        .build()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            return BenchResult {
                mode: mode_name.into(),
                write_times: vec![],
                read_times: vec![],
                connect_time: t0.elapsed(),
                error: Some(format!("{e}")),
            };
        }
    };
    let connect_time = t0.elapsed();

    let payload: String = "x".repeat(config.payload_size);
    let object_name = format!("dc-bench/{}-{}", mode_name.to_lowercase(), std::process::id());
    let mut write_times = Vec::with_capacity(config.iterations);
    let mut read_times = Vec::with_capacity(config.iterations);

    for i in 0..config.iterations {
        let name = format!("{object_name}-{i}");
        let data = payload.clone();

        let t = Instant::now();
        if let Err(e) = client
            .write_object(bucket, &name, data)
            .send_unbuffered()
            .await
        {
            return BenchResult {
                mode: mode_name.into(),
                write_times,
                read_times,
                connect_time,
                error: Some(format!("write #{i}: {e}")),
            };
        }
        write_times.push(t.elapsed());

        let t = Instant::now();
        match client.read_object(bucket, &name).send().await {
            Ok(mut resp) => {
                while let Some(chunk) = resp.next().await {
                    let _ = chunk;
                }
                read_times.push(t.elapsed());
            }
            Err(e) => {
                return BenchResult {
                    mode: mode_name.into(),
                    write_times,
                    read_times,
                    connect_time,
                    error: Some(format!("read #{i}: {e}")),
                };
            }
        }
    }

    BenchResult {
        mode: mode_name.into(),
        write_times,
        read_times,
        connect_time,
        error: None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_target(false)
        .init();

    let config = BenchConfig::from_args();
    let bucket = bucket_name()?;

    println!("Direct Connectivity Benchmark");
    println!("  bucket:     {bucket}");
    println!("  iterations: {}", config.iterations);
    println!("  payload:    {} bytes", config.payload_size);
    println!();

    // Warmup DNS / metadata caches.
    println!("warming up...");
    let warmup_cfg = BenchConfig {
        iterations: 1,
        payload_size: 64,
    };
    let _ = bench_standard(&bucket, &warmup_cfg).await;

    println!();
    println!("Results:");

    let standard = bench_standard(&bucket, &config).await;
    standard.print();

    #[cfg(google_cloud_unstable_direct_connectivity)]
    {
        let auto = bench_mode("Auto", DirectConnectivityMode::Auto, &bucket, &config).await;
        auto.print();

        let direct =
            bench_mode("Enabled", DirectConnectivityMode::Enabled, &bucket, &config).await;
        direct.print();
    }

    println!();
    Ok(())
}
