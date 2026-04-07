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

pub use google_cloud_gax::direct_connectivity::DirectConnectivityMode;

const DIRECT_PATH_ENV_VAR: &str = "GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS";

/// Resolves the effective direct connectivity mode by checking the explicit
/// configuration first, then falling back to environment variables.
pub fn resolve_mode(explicit: Option<&DirectConnectivityMode>) -> DirectConnectivityMode {
    if let Some(mode) = explicit {
        return mode.clone();
    }
    if let Ok(val) = std::env::var(DIRECT_PATH_ENV_VAR) {
        if val.eq_ignore_ascii_case("true") || val == "1" {
            return DirectConnectivityMode::Auto;
        }
    }
    DirectConnectivityMode::Disabled
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_mode_explicit_takes_precedence() {
        let mode = resolve_mode(Some(&DirectConnectivityMode::Enabled));
        assert!(matches!(mode, DirectConnectivityMode::Enabled));
    }

    #[test]
    fn test_resolve_mode_default_is_disabled() {
        // Without env var set, should be disabled
        let mode = resolve_mode(None);
        assert!(matches!(mode, DirectConnectivityMode::Disabled));
    }

    #[test]
    fn test_resolve_mode_env_var() {
        let _guard = scoped_env::ScopedEnv::set(DIRECT_PATH_ENV_VAR, "true");
        let mode = resolve_mode(None);
        assert!(matches!(mode, DirectConnectivityMode::Auto));
    }

    #[test]
    fn test_resolve_mode_env_var_false() {
        let _guard = scoped_env::ScopedEnv::set(DIRECT_PATH_ENV_VAR, "false");
        let mode = resolve_mode(None);
        assert!(matches!(mode, DirectConnectivityMode::Disabled));
    }
}
