// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Flat File Credentials extension.

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = FLAT_FILE_CREDENTIALS_URN,
    target = "otel.extension.flat_file_credentials",
);

pub mod config;
mod credentials;
pub mod error;
mod extension;
mod metrics;

use std::sync::Arc;

use linkme::distributed_slice;
use otel_arrow_dfe_config::error::Error as ConfigError;
use otel_arrow_dfe_config::extension::ExtensionUserConfig;
use otel_arrow_dfe_engine::ExtensionFactory;
use otel_arrow_dfe_engine::capability::auth::credential_provider::CredentialProvider;
use otel_arrow_dfe_engine::config::ExtensionConfig;
use otel_arrow_dfe_engine::context::ExtensionContext;
use otel_arrow_dfe_engine::extension::wrapper::ExtensionVariant;
use otel_arrow_dfe_engine::extension::{ExtensionBundle, ExtensionWrapper};
use otel_arrow_dfe_engine::extension_capabilities;
use otel_arrow_dfe_otap::OTAP_EXTENSION_FACTORIES;
use tokio::sync::watch;

use crate::common::background_refresh::BackgroundProviderMetricsTracker;

use self::config::Config;
use self::credentials::FlatFileCredentials;
use self::extension::FlatFileCredentialsExtension;
use self::metrics::FlatFileCredentialsMetrics;

/// URN under which this extension is registered.
pub const FLAT_FILE_CREDENTIALS_URN: &str = "urn:otel:extension:flat_file_credentials";

/// Deserializes and validates the extension's user configuration.
fn parse_config(config: &serde_json::Value) -> Result<Config, ConfigError> {
    let parsed: Config =
        serde_json::from_value(config.clone()).map_err(|e| ConfigError::InvalidUserConfig {
            error: e.to_string(),
        })?;
    parsed
        .validate()
        .map_err(|error| ConfigError::InvalidUserConfig { error })?;
    Ok(parsed)
}

/// Static config validation hook for the factory.
fn validate_config(config: &serde_json::Value) -> Result<(), ConfigError> {
    parse_config(config).map(|_| ())
}

/// Builds an `FlatFileCredentialsExtension` bundle.
fn create(
    ext_ctx: &ExtensionContext,
    name: otel_arrow_dfe_config::ExtensionId,
    ext_config: Arc<ExtensionUserConfig>,
    extension_config: &ExtensionConfig,
) -> Result<ExtensionBundle, ConfigError> {
    // Validate config now so a bad config fails fast at wiring time.
    let config = parse_config(&ext_config.config)?;

    // Empty credential cache; the background refresh loop publishes the first credential.
    let (tx, _rx) = watch::channel(None);

    // Register a dedicated entity + metric set for this extension instance.
    let entity_key = ext_ctx.register_extension_entity(name.clone(), ExtensionVariant::Shared);
    let metric_set =
        ext_ctx.register_metric_set_for_entity::<FlatFileCredentialsMetrics>(entity_key);
    let tracker = BackgroundProviderMetricsTracker::new(metric_set);

    let expiry_buffer = config.expiry_buffer;

    let extension = FlatFileCredentialsExtension::new(
        &name,
        FlatFileCredentials::new(config),
        expiry_buffer,
        tx,
        tracker,
    );

    ExtensionWrapper::builder(name, ext_config, extension_config)
        .active()
        .shared::<FlatFileCredentialsExtension>(extension)
        .build()
        .map_err(|e| ConfigError::InvalidUserConfig {
            error: e.to_string(),
        })
}

/// Factory registration for the OAuth 2.0 Client Auth extension.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Extension)]
#[distributed_slice(OTAP_EXTENSION_FACTORIES)]
pub static FLAT_FILE_CREDENTIALS_EXTENSION: ExtensionFactory = ExtensionFactory {
    name: FLAT_FILE_CREDENTIALS_URN,
    description: "Active+Shared extension exposing CredentialProvider via flat files",
    documentation_url: "",
    capabilities: Some(extension_capabilities!(
        shared: FlatFileCredentialsExtension => [CredentialProvider]
    )),
    create,
    validate_config,
};
