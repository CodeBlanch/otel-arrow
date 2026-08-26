// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration for the Flat File Credentials extension.

use std::path::PathBuf;
use std::time::Duration;

use secrecy::SecretString;
use serde::Deserialize;

use otel_arrow_dfe_engine::capability::auth::credential_provider::CREDENTIAL_USABLE_MARGIN;

/// Default duration ahead of expiry at which a token is refreshed.
fn default_expiry_buffer() -> Duration {
    Duration::from_secs(300)
}

/// Default password secret file refresh.
fn default_password_secret_file_refresh() -> Duration {
    Duration::from_secs(60 * 60)
}

/// Configuration for the Flat File Credentials extension.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Username. Must be non-empty.
    ///
    /// Held as a [`SecretString`] so it is redacted from `Debug` output.
    pub username: SecretString,

    /// Password secret.
    ///
    /// Held as a [`SecretString`] so it is redacted from `Debug` output.
    /// Note that the raw pipeline config retains the cleartext;
    /// prefer `password_secret_file`.
    #[serde(default)]
    pub password_secret: Option<SecretString>,

    /// Path to a file holding the password secret. Re-read at
    /// `password_secret_file_refresh` interval and takes precedence over
    /// `password_secret`.
    #[serde(default)]
    pub password_secret_file: Option<PathBuf>,

    /// Refresh duration for the password secret file (if specified). Accepts
    /// human-readable durations (e.g. `5m`, `30s`). Must be non-zero.
    #[serde(
        with = "humantime_serde",
        default = "default_password_secret_file_refresh"
    )]
    pub password_secret_file_refresh: Duration,

    /// Refresh this far ahead of the credential's expiry. Accepts human-readable
    /// durations (e.g. `5m`, `30s`). Must be greater than the fixed usability
    /// margin ([`CREDENTIAL_USABLE_MARGIN`]).
    #[serde(with = "humantime_serde", default = "default_expiry_buffer")]
    pub expiry_buffer: Duration,
}

impl Config {
    /// Validates the configuration beyond what deserialization checks.
    ///
    /// Rejects missing secret or an `expiry_buffer` that does not clear the
    /// usability margin, a zero `startup_timeout`.
    pub fn validate(&self) -> Result<(), String> {
        // A credential is treated as unusable once it is within `CREDENTIAL_USABLE_MARGIN`
        // of expiry, so a refresh scheduled at or inside that margin only ever
        // lands after consumers have already started back-pressuring: every
        // credential cycle would stall for the difference, with nothing in the
        // diagnostics pointing at this setting.
        if self.expiry_buffer <= CREDENTIAL_USABLE_MARGIN {
            return Err(format!(
                "`expiry_buffer` must be greater than {}s, the window before expiry in which a credential is no longer used",
                CREDENTIAL_USABLE_MARGIN.as_secs()
            ));
        }

        let secret_fields_set =
            self.password_secret.is_some() || self.password_secret_file.is_some();

        if !secret_fields_set {
            return Err(
                "either `password_secret` or `password_secret_file` must be set".to_string(),
            );
        }

        if self.password_secret_file_refresh.is_zero() {
            return Err("`password_secret_file_refresh` must be greater than zero".to_string());
        }

        Ok(())
    }
}
