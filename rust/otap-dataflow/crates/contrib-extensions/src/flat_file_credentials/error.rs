// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Error types for the Flat File Credentials extension.

use std::path::PathBuf;

/// Errors raised while acquiring credentials.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Reading a credential file (`*_file` config field) failed.
    #[error("failed to read credential file {}: {source}", .path.display())]
    ReadCredentialFile {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// Acquiring a credential failed.
    #[error("credential acquisition failed: {message}")]
    CredentialAcquisition {
        /// Human-readable cause reported.
        message: String,
    },
}
