// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

use crate::QueryLocation;

#[derive(Error, Debug)]
pub enum ExpressionError {
    #[error("{1}")]
    TypeMismatch(QueryLocation, String),

    #[error("{1}")]
    ValidationFailure(QueryLocation, String),

    #[error("{1}")]
    ParseError(QueryLocation, String),

    #[error("{1}")]
    NotSupported(QueryLocation, String),
}

impl ExpressionError {
    pub fn get_query_location(&self) -> &QueryLocation {
        match self {
            ExpressionError::TypeMismatch(l, _) => l,
            ExpressionError::ValidationFailure(l, _) => l,
            ExpressionError::ParseError(l, _) => l,
            ExpressionError::NotSupported(l, _) => l,
        }
    }

    pub fn get_message(&self) -> &str {
        match self {
            ExpressionError::TypeMismatch(_, message) => message,
            ExpressionError::ValidationFailure(_, message) => message,
            ExpressionError::ParseError(_, message) => message,
            ExpressionError::NotSupported(_, message) => message,
        }
    }

    pub fn into_parts(self) -> (QueryLocation, String) {
        match self {
            ExpressionError::TypeMismatch(l, m) => (l, m),
            ExpressionError::ValidationFailure(l, m) => (l, m),
            ExpressionError::ParseError(l, m) => (l, m),
            ExpressionError::NotSupported(l, m) => (l, m),
        }
    }
}
