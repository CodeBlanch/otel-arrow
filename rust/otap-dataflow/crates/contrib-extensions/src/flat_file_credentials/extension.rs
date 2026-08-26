// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Flat File Credentials extension.

use async_trait::async_trait;
use futures::StreamExt;
use otel_arrow_dfe_engine::capability::CapabilityError;
use otel_arrow_dfe_engine::capability::auth::credential_provider::{
    Credential, CredentialProvider as CredentialProviderCap, CredentialStream,
};
use otel_arrow_dfe_engine::shared::capability::auth::credential_provider::CredentialProvider as SharedCredentialProvider;
use tokio_stream::wrappers::WatchStream;

use crate::common::background_refresh::BackgroundProviderExtension;
use crate::flat_file_credentials::credentials::FlatFileCredentials;
use crate::flat_file_credentials::metrics::FlatFileCredentialsMetrics;

/// The Flat File Credentials extension.
pub type FlatFileCredentialsExtension = BackgroundProviderExtension<
    FlatFileCredentials,
    FlatFileCredentialsMetrics,
    Credential,
    CredentialProviderCap,
>;

#[async_trait]
impl SharedCredentialProvider for FlatFileCredentialsExtension {
    async fn get_credential(&self) -> Result<Credential, CapabilityError> {
        // Fast path: lock-free read of the watch cache.
        if let Some(credential) = self.current_fresh_value() {
            return Ok(credential);
        }

        // Slow path: coalesce concurrent cache-miss callers onto a single
        // in-flight credential call, with a double-check after acquiring the lock.
        let _guard = self.acquire_fetch_lock().await;
        if let Some(credential) = self.current_fresh_value() {
            return Ok(credential);
        }
        // Negative cache: if the most recent acquisition failed within the
        // cooldown window, surface the throttle instead of hitting the credential
        // endpoint again. The background loop keeps retrying on its own cadence.
        if self.recently_failed() {
            return Err(
                self.capability_error("credential acquisition throttled after recent failure")
            );
        }
        self.refresh_once()
            .await
            .map_err(|err| self.capability_error(err))
    }

    fn credential_stream(&self) -> CredentialStream {
        let rx = self.subscribe();
        // Yield the current cached value immediately, then each refresh. The
        // initial `None` (and any future `None`) is filtered out. The stream
        // item is a plain `Credential`: a refresh failure does not terminate
        // the subscription, it simply does not emit until the next success.
        let stream = WatchStream::new(rx).filter_map(|opt| async move { opt });
        Box::pin(stream)
    }
}
