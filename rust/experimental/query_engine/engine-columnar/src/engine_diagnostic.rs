// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use data_engine_expressions::*;

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub enum ColumnarEngineDiagnosticLevel {
    Verbose = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
}

impl ColumnarEngineDiagnosticLevel {
    pub fn get_name(&self) -> &str {
        match self {
            ColumnarEngineDiagnosticLevel::Verbose => "Verbose",
            ColumnarEngineDiagnosticLevel::Info => "Info",
            ColumnarEngineDiagnosticLevel::Warn => "Warn",
            ColumnarEngineDiagnosticLevel::Error => "Error",
        }
    }

    pub fn from_usize(n: usize) -> Option<ColumnarEngineDiagnosticLevel> {
        match n {
            0 => Some(ColumnarEngineDiagnosticLevel::Verbose),
            1 => Some(ColumnarEngineDiagnosticLevel::Info),
            2 => Some(ColumnarEngineDiagnosticLevel::Warn),
            3 => Some(ColumnarEngineDiagnosticLevel::Error),
            _ => None,
        }
    }
}

impl FromStr for ColumnarEngineDiagnosticLevel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Verbose" | "verbose" => Ok(ColumnarEngineDiagnosticLevel::Verbose),
            "Info" | "info" => Ok(ColumnarEngineDiagnosticLevel::Verbose),
            "Warn" | "warn" => Ok(ColumnarEngineDiagnosticLevel::Warn),
            "Error" | "error" => Ok(ColumnarEngineDiagnosticLevel::Error),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct ColumnarEngineDiagnostic<'a> {
    diagnostic_level: ColumnarEngineDiagnosticLevel,
    expression: &'a dyn Expression,
    message: String,
    //nested_diagnostics: Option<Vec<ColumnarEngineDiagnostic<'a>>>,
}

impl<'a> ColumnarEngineDiagnostic<'a> {
    pub(crate) fn new(
        diagnostic_level: ColumnarEngineDiagnosticLevel,
        expression: &'a dyn Expression,
        message: String,
    ) -> ColumnarEngineDiagnostic<'a> {
        Self {
            diagnostic_level,
            expression,
            message,
            //nested_diagnostics: None,
        }
    }

    pub fn get_diagnostic_level(&self) -> ColumnarEngineDiagnosticLevel {
        self.diagnostic_level
    }

    pub fn get_expression(&self) -> &dyn Expression {
        self.expression
    }

    pub fn get_message(&self) -> &str {
        &self.message
    }
}
