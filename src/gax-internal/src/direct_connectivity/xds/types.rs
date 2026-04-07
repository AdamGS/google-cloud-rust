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

//! Hand-written prost types for the minimal subset of xDS protos needed
//! for DirectPath endpoint discovery. These avoid pulling in the full
//! Envoy proto dependency tree.

// ── ADS (envoy.service.discovery.v3) ──

#[derive(Clone, PartialEq, prost::Message)]
pub struct DiscoveryRequest {
    #[prost(string, tag = "1")]
    pub version_info: String,
    #[prost(message, optional, tag = "2")]
    pub node: Option<Node>,
    #[prost(string, repeated, tag = "3")]
    pub resource_names: Vec<String>,
    #[prost(string, tag = "4")]
    pub type_url: String,
    #[prost(string, tag = "5")]
    pub response_nonce: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct DiscoveryResponse {
    #[prost(string, tag = "1")]
    pub version_info: String,
    #[prost(message, repeated, tag = "2")]
    pub resources: Vec<prost_types::Any>,
    #[prost(string, tag = "4")]
    pub type_url: String,
    #[prost(string, tag = "5")]
    pub nonce: String,
}

// ── Node (envoy.config.core.v3) ──

#[derive(Clone, PartialEq, prost::Message)]
pub struct Node {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(string, tag = "2")]
    pub cluster: String,
    #[prost(message, optional, tag = "3")]
    pub metadata: Option<prost_types::Struct>,
    #[prost(message, optional, tag = "4")]
    pub locality: Option<Locality>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Locality {
    #[prost(string, tag = "1")]
    pub region: String,
    #[prost(string, tag = "2")]
    pub zone: String,
}

// ── Listener (envoy.config.listener.v3) ── (LDS response)

#[derive(Clone, PartialEq, prost::Message)]
pub struct Listener {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, optional, tag = "19")]
    pub api_listener: Option<ApiListener>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct ApiListener {
    #[prost(message, optional, tag = "1")]
    pub api_listener: Option<prost_types::Any>,
}

// ── HttpConnectionManager (envoy.extensions.filters.network.http_connection_manager.v3) ──
// Only the fields needed to extract inline route config.

#[derive(Clone, PartialEq, prost::Message)]
pub struct HttpConnectionManager {
    /// RDS configuration (route_specifier oneof, tag 3).
    #[prost(message, optional, tag = "3")]
    pub rds: Option<Rds>,
    /// Inline route configuration (route_specifier oneof, tag 4).
    #[prost(message, optional, tag = "4")]
    pub route_config: Option<RouteConfiguration>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Rds {
    #[prost(string, tag = "2")]
    pub route_config_name: String,
}

// ── RouteConfiguration (envoy.config.route.v3) ──

#[derive(Clone, PartialEq, prost::Message)]
pub struct RouteConfiguration {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, repeated, tag = "2")]
    pub virtual_hosts: Vec<VirtualHost>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct VirtualHost {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, repeated, tag = "3")]
    pub routes: Vec<Route>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Route {
    #[prost(message, optional, tag = "2")]
    pub route: Option<RouteAction>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct RouteAction {
    /// cluster (oneof tag 1)
    #[prost(string, tag = "1")]
    pub cluster: String,
}

// ── Cluster (envoy.config.cluster.v3) ── (CDS response)

#[derive(Clone, PartialEq, prost::Message)]
pub struct Cluster {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, optional, tag = "3")]
    pub eds_cluster_config: Option<EdsClusterConfig>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct EdsClusterConfig {
    #[prost(string, tag = "2")]
    pub service_name: String,
}

// ── ClusterLoadAssignment (envoy.config.endpoint.v3) ── (EDS response)

#[derive(Clone, PartialEq, prost::Message)]
pub struct ClusterLoadAssignment {
    #[prost(string, tag = "1")]
    pub cluster_name: String,
    #[prost(message, repeated, tag = "2")]
    pub endpoints: Vec<LocalityLbEndpoints>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct LocalityLbEndpoints {
    #[prost(message, repeated, tag = "2")]
    pub lb_endpoints: Vec<LbEndpoint>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct LbEndpoint {
    #[prost(message, optional, tag = "1")]
    pub endpoint: Option<Endpoint>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Endpoint {
    #[prost(message, optional, tag = "1")]
    pub address: Option<Address>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Address {
    #[prost(message, optional, tag = "1")]
    pub socket_address: Option<SocketAddress>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct SocketAddress {
    #[prost(string, tag = "2")]
    pub address: String,
    #[prost(uint32, tag = "3")]
    pub port_value: u32,
}

// ── Type URLs ──

pub const LDS_TYPE_URL: &str =
    "type.googleapis.com/envoy.config.listener.v3.Listener";
pub const RDS_TYPE_URL: &str =
    "type.googleapis.com/envoy.config.route.v3.RouteConfiguration";
pub const CDS_TYPE_URL: &str =
    "type.googleapis.com/envoy.config.cluster.v3.Cluster";
pub const EDS_TYPE_URL: &str =
    "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";
pub const HTTP_CONNECTION_MANAGER_TYPE_URL: &str =
    "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager";
