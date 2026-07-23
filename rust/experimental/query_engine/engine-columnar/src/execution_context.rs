// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::{Ref, RefCell, RefMut},
    fmt::Display,
    ops::Deref,
};

use ahash::AHashMap;
use data_engine_expressions::*;

use crate::{engine_diagnostic::*, resolved_value::ResolvedSingleOrDictionaryValue, *};

pub struct ExecutionContext<'a, 'pipeline, TRecords: ColumnarRecords<'pipeline>> {
    diagnostics: ColumnarEngineDiagnosticReceiverImpl<'a, 'pipeline>,
    pipeline: &'pipeline PipelineExpression,
    variables: ExecutionContextVariables<'a, 'pipeline>,
    records: Option<TRecords>,
}

impl<'a, 'pipeline, TRecords: ColumnarRecords<'pipeline>>
    ExecutionContext<'a, 'pipeline, TRecords>
{
    pub(crate) fn new(
        diagnostic_level: ColumnarEngineDiagnosticLevel,
        diagnostics: &'a RefCell<Vec<ColumnarEngineDiagnostic<'pipeline>>>,
        pipeline: &'pipeline PipelineExpression,
        global_variables: &'a RefCell<
            AHashMap<Box<str>, ResolvedSingleOrDictionaryValue<'pipeline>>,
        >,
        //summaries: &'b Summaries<'a>,
        records: Option<TRecords>,
        //arguments: Option<&'b dyn ExecutionContextArguments>,
    ) -> ExecutionContext<'a, 'pipeline, TRecords> {
        Self {
            diagnostics: ColumnarEngineDiagnosticReceiverImpl::new(diagnostic_level, diagnostics),
            pipeline,
            records,
            variables: ExecutionContextVariables::new(global_variables),
            //summaries,
            //arguments,
        }
    }

    pub fn is_diagnostic_level_enabled(
        &self,
        diagnostic_level: ColumnarEngineDiagnosticLevel,
    ) -> bool {
        self.diagnostics
            .is_diagnostic_level_enabled(diagnostic_level)
    }

    pub fn add_diagnostic_if_enabled<F>(
        &self,
        diagnostic_level: ColumnarEngineDiagnosticLevel,
        expression: &'pipeline dyn Expression,
        generate_message: F,
    ) where
        F: FnOnce() -> String,
    {
        self.diagnostics
            .add_diagnostic_if_enabled(diagnostic_level, expression, generate_message)
    }

    pub fn add_diagnostic(&self, diagnostic: ColumnarEngineDiagnostic<'pipeline>) {
        self.diagnostics.add_diagnostic(diagnostic)
    }

    pub fn get_pipeline(&self) -> &'pipeline PipelineExpression {
        self.pipeline
    }

    pub fn get_records(&self) -> Option<&TRecords> {
        self.records.as_ref()
    }

    pub fn get_records_mut(&mut self) -> Option<&mut TRecords> {
        self.records.as_mut()
    }

    pub fn get_variables(&self) -> &ExecutionContextVariables<'a, 'pipeline> {
        &self.variables
    }

    pub fn into_parts(self) -> Option<TRecords> {
        self.records
    }
}

impl<'pipeline, TRecords: ColumnarRecords<'pipeline>> Display
    for ExecutionContext<'_, 'pipeline, TRecords>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_diagnostics(
            self.pipeline.get_query(),
            self.diagnostics.get_diagnostics().deref(),
            f,
        )
    }
}

pub struct ExecutionContextVariables<'a, 'pipeline> {
    global_variables: &'a RefCell<AHashMap<Box<str>, ResolvedSingleOrDictionaryValue<'pipeline>>>,
    local_variables: RefCell<AHashMap<Box<str>, ResolvedSingleOrDictionaryValue<'pipeline>>>,
}

impl<'a, 'pipeline> ExecutionContextVariables<'a, 'pipeline> {
    pub(crate) fn new(
        global_variables: &'a RefCell<
            AHashMap<Box<str>, ResolvedSingleOrDictionaryValue<'pipeline>>,
        >,
    ) -> Self {
        Self {
            global_variables,
            local_variables: RefCell::new(AHashMap::new()),
        }
    }

    pub fn get_global_or_local_variable(
        &self,
        name: &str,
    ) -> Option<Ref<'_, ResolvedSingleOrDictionaryValue<'pipeline>>> {
        let vars = self.local_variables.borrow();

        let var = Ref::filter_map(vars, |v| v.get(name));

        if let Ok(v) = var {
            return Some(v);
        }

        Ref::filter_map(self.global_variables.borrow(), |v| v.get(name)).ok()
    }

    #[cfg(test)]
    pub fn get_local_variables(
        &self,
    ) -> Ref<'_, AHashMap<Box<str>, ResolvedSingleOrDictionaryValue<'pipeline>>> {
        self.local_variables.borrow()
    }

    pub fn get_local_variables_mut(
        &self,
    ) -> RefMut<'_, AHashMap<Box<str>, ResolvedSingleOrDictionaryValue<'pipeline>>> {
        self.local_variables.borrow_mut()
    }

    #[cfg(test)]
    pub fn get_global_variables(
        &self,
    ) -> Ref<'_, AHashMap<Box<str>, ResolvedSingleOrDictionaryValue<'pipeline>>> {
        self.global_variables.borrow()
    }
}
