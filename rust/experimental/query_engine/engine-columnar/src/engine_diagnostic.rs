// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use data_engine_expressions::*;

use crate::{ColumnarRecords, execution_context::ExecutionContext};

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

pub(crate) trait DiagnosticReceiver {
    fn add_diagnostic_if_enabled<F>(
        &self,
        diagnostic_level: ColumnarEngineDiagnosticLevel,
        generate_message: F,
    ) where
        F: FnOnce() -> String;
}

pub(crate) struct ExecutionContextDiagnosticReceiver<'a, 'pipeline, TRecords: ColumnarRecords> {
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    expression: &'pipeline dyn Expression,
}

impl<'a, 'pipeline, TRecords: ColumnarRecords>
    ExecutionContextDiagnosticReceiver<'a, 'pipeline, TRecords>
{
    pub fn new(
        execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
        expression: &'pipeline dyn Expression,
    ) -> ExecutionContextDiagnosticReceiver<'a, 'pipeline, TRecords> {
        Self {
            execution_context,
            expression,
        }
    }
}

impl<TRecords: ColumnarRecords> DiagnosticReceiver
    for ExecutionContextDiagnosticReceiver<'_, '_, TRecords>
{
    fn add_diagnostic_if_enabled<F>(
        &self,
        diagnostic_level: ColumnarEngineDiagnosticLevel,
        generate_message: F,
    ) where
        F: FnOnce() -> String,
    {
        self.execution_context.add_diagnostic_if_enabled(
            diagnostic_level,
            self.expression,
            generate_message,
        );
    }
}

#[cfg(test)]
pub(crate) struct NoopDiagnosticReceiver {}

#[cfg(test)]
impl DiagnosticReceiver for NoopDiagnosticReceiver {
    fn add_diagnostic_if_enabled<F>(
        &self,
        _diagnostic_level: ColumnarEngineDiagnosticLevel,
        _generate_message: F,
    ) where
        F: FnOnce() -> String,
    {
    }
}
