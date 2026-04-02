// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BridgeError {
    #[error("Pipeline could not be initialized: {0}")]
    PipelineInitializationError(String),
}
