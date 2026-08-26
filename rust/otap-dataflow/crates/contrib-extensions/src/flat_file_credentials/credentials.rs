// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Flat File Credentials logic.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use otel_arrow_dfe_engine::capability::auth::credential_provider::{
    CREDENTIAL_USABLE_MARGIN, Credential,
};
use otel_arrow_dfe_otap::tls_utils::read_file_with_limit_async;
use secrecy::{ExposeSecret, SecretString};

use crate::common::background_refresh::BackgroundProviderSource;
use crate::flat_file_credentials::config::Config;
use crate::flat_file_credentials::error::Error;

pub(crate) struct FlatFileCredentials {
    config: Config,
}

impl FlatFileCredentials {
    pub fn new(config: Config) -> FlatFileCredentials {
        Self { config }
    }
}

#[async_trait]
impl BackgroundProviderSource<Credential> for FlatFileCredentials {
    type Error = Error;

    fn usable_margin() -> Duration {
        CREDENTIAL_USABLE_MARGIN
    }

    fn expires_on(value: &Credential) -> Option<Instant> {
        value.expires_on()
    }

    async fn fetch(&self) -> Result<Credential, Error> {
        let (password, expires_after) = read_credential(
            self.config.password_secret_file.as_ref(),
            self.config.password_secret_file_refresh,
            self.config
                .password_secret
                .as_ref()
                .map(SecretString::expose_secret),
            "password_secret",
        )
        .await?;

        Ok(Credential::with_expiry(
            SecretString::expose_secret(&self.config.username),
            password,
            expires_after.map(|expires_after| Instant::now() + expires_after),
        ))
    }

    fn log_refresh_failure(&self, error: &Error) {
        otel_warn!("flat_file_credentials.refresh_failed", error = %error);
    }
}

/// Reads a credential value, preferring the file form (re-read on each call so
/// the credential can rotate without a restart) over the inline value.
///
/// File reads go through the collector's shared size-limited reader: this runs
/// on the per-acquisition path, so an oversized or hostile path would otherwise
/// be re-read into memory on every refresh.
async fn read_credential(
    file: Option<&PathBuf>,
    file_refresh: Duration,
    inline: Option<&str>,
    field: &str,
) -> Result<(String, Option<Duration>), Error> {
    if let Some(path) = file {
        let contents =
            read_file_with_limit_async(path)
                .await
                .map_err(|source| Error::ReadCredentialFile {
                    path: path.clone(),
                    source,
                })?;
        let contents = String::from_utf8(contents).map_err(|_| Error::CredentialAcquisition {
            message: format!("`{field}_file` does not contain valid UTF-8"),
        })?;
        return Ok((contents.trim().to_owned(), Some(file_refresh)));
    }
    if let Some(value) = inline {
        return Ok((value.to_owned(), None));
    }
    Err(Error::CredentialAcquisition {
        message: format!("no `{field}` or `{field}_file` configured"),
    })
}
