// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Telemetry for the Flat File Credentials extension.

use otel_arrow_dfe_telemetry::instrument::{Counter, Mmsc};
use otel_arrow_dfe_telemetry_macros::metric_set;

use crate::common::background_refresh::BackgroundProviderMetrics;

/// Telemetry metrics for the Flat File Credentials extension.
#[metric_set(name = "extension.flat_file_credentials")]
#[derive(Debug, Default, Clone)]
pub struct FlatFileCredentialsMetrics {
    /// Number of successful credentials acquisitions.
    #[metric(unit = "{acquisition}")]
    pub read_successes: Counter<u64>,
    /// Number of failed credentials acquisitions.
    #[metric(unit = "{acquisition}")]
    pub read_failures: Counter<u64>,
    /// Number of credentials published to consumers via the watch channel.
    #[metric(unit = "{token}")]
    pub credentials_publish: Counter<u64>,
    /// Latency of successful acquisitions in milliseconds (min/max/sum/count).
    #[metric(unit = "ms")]
    pub read_success_latency: Mmsc,
}

impl BackgroundProviderMetrics for FlatFileCredentialsMetrics {
    fn successes(&mut self) -> &mut Counter<u64> {
        &mut self.read_successes
    }

    fn failures(&mut self) -> &mut Counter<u64> {
        &mut self.read_failures
    }

    fn publishes(&mut self) -> &mut Counter<u64> {
        &mut self.credentials_publish
    }

    fn success_latency(&mut self) -> &mut Mmsc {
        &mut self.read_success_latency
    }
}
