// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::RefCell,
    collections::HashSet,
    fmt::{Debug, Display, Write},
};

use arrow::{array::*, datatypes::*};
use data_engine_expressions::*;

use crate::{
    engine_diagnostic::{ColumnarEngineDiagnostic, ColumnarEngineDiagnosticLevel},
    execution_context::ExecutionContext,
    logical_expressions::execute_logical_expression,
    resolved_value::*,
    transform::set_transform_expression::execute_set_transform_expression,
    *,
};

pub struct ColumnarEngineOptions {
    pub(crate) diagnostic_level: ColumnarEngineDiagnosticLevel,
    pub(crate) summary_cardinality_limit: usize,
}

impl Default for ColumnarEngineOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarEngineOptions {
    pub fn new() -> ColumnarEngineOptions {
        Self {
            diagnostic_level: ColumnarEngineDiagnosticLevel::Warn,
            summary_cardinality_limit: 8192,
        }
    }

    pub fn with_diagnostic_level(
        mut self,
        diagnostic_level: ColumnarEngineDiagnosticLevel,
    ) -> ColumnarEngineOptions {
        self.diagnostic_level = diagnostic_level;
        self
    }

    pub fn with_summary_cardinality_limit(
        mut self,
        summary_cardinality_limit: usize,
    ) -> ColumnarEngineOptions {
        self.summary_cardinality_limit = summary_cardinality_limit;
        self
    }
}

#[derive(Debug)]
pub struct ColumnarEngine {
    diagnostic_level: ColumnarEngineDiagnosticLevel,
    summary_cardinality_limit: usize,
    pipeline: PipelineExpression,
}

impl ColumnarEngine {
    pub fn new(pipeline: PipelineExpression) -> ColumnarEngine {
        Self::new_with_options(pipeline, ColumnarEngineOptions::new())
    }

    pub fn new_with_options(
        mut pipeline: PipelineExpression,
        options: ColumnarEngineOptions,
    ) -> ColumnarEngine {
        for expression in pipeline.get_expressions_mut() {
            if let DataExpression::Discard(d) = expression
                && let Some(predicate) = d.get_predicate_mut()
            {
                if let LogicalExpression::Not(n) = predicate {
                    // Note: If the predicate is a Not() we remove that and
                    // just call the inner expression. The reason for this
                    // is arrow filter operation works inversely to what the
                    // expression tree is set up to express.
                    *predicate = n.get_inner_expression().clone();
                } else {
                    // Note: If the predicate is something other than Not()
                    // we introduce one to invert the expression logic to
                    // match arrow filter behavior.
                    *predicate = LogicalExpression::Not(NotLogicalExpression::new(
                        predicate.get_query_location().clone(),
                        predicate.clone(),
                    ));
                }
            }
        }

        Self {
            diagnostic_level: options.diagnostic_level,
            summary_cardinality_limit: options.summary_cardinality_limit,
            pipeline,
        }
    }

    pub fn get_pipeline(&self) -> &PipelineExpression {
        &self.pipeline
    }

    pub fn begin_batch<const BATCH_SIZE: usize>(
        &self,
    ) -> Result<ColumnarEngineBatch<'_, BATCH_SIZE>, ExpressionError> {
        let mut batch = ColumnarEngineBatch::new(self);
        batch.initialize()?;
        Ok(batch)
    }
}

pub struct ColumnarEngineBatch<'a, const BATCH_SIZE: usize> {
    engine: &'a ColumnarEngine,
    diagnostics: RefCell<Vec<ColumnarEngineDiagnostic<'a>>>,
    //global_variables: RefCell<MapValueStorage<OwnedValue>>,
    //summaries: Summaries<'a>,
    included_batches: Vec<[Option<RecordBatch>; BATCH_SIZE]>,
    included_record_count: usize,
    dropped_record_count: usize,
}

impl<'a, const BATCH_SIZE: usize> ColumnarEngineBatch<'a, BATCH_SIZE> {
    pub(crate) fn new(engine: &'a ColumnarEngine) -> ColumnarEngineBatch<'a, BATCH_SIZE> {
        Self {
            engine,
            diagnostics: RefCell::new(Vec::new()),
            //global_variables: RefCell::new(MapValueStorage::new(HashMap::new())),
            //summaries: Summaries::new(engine.summary_cardinality_limit),
            included_batches: Vec::new(),
            included_record_count: 0,
            dropped_record_count: 0,
        }
    }

    pub(crate) fn initialize(&mut self) -> Result<(), ExpressionError> {
        //todo!()
        Ok(())
    }

    pub fn push_records<TRecordFactory: ColumnarRecordsFactory<BATCH_SIZE>>(
        &mut self,
        factory: &TRecordFactory,
        mut batches: [Option<RecordBatch>; BATCH_SIZE],
    ) {
        let records = factory.create(&batches);

        let diagnostic_level = records
            .get_diagnostic_level()
            .unwrap_or(self.engine.diagnostic_level);

        let mut current_batch_record_count = records.len();

        let pipeline = &self.engine.pipeline;

        let mut execution_context = ExecutionContext::new(
            diagnostic_level,
            //&self.engine.external_function_implementations,
            &self.diagnostics,
            pipeline,
            //&self.global_variables,
            //&self.summaries,
            Some(records),
            //None,
        );

        for expression in pipeline.get_expressions() {
            match expression {
                DataExpression::Discard(d) => {
                    if let Some(predicate) = d.get_predicate() {
                        match execute_logical_expression(&execution_context, predicate) {
                            Ok(logical_result) => {
                                match logical_result {
                                    ResolvedLogicalValue::Single(single) => {
                                        if single {
                                            execution_context.add_diagnostic_if_enabled(
                                                ColumnarEngineDiagnosticLevel::Verbose,
                                                d,
                                                || "All records included".into(),
                                            );
                                            continue;
                                        }
                                    }
                                    ResolvedLogicalValue::Array(array) => {
                                        let new_batches = factory.filter(
                                            execution_context.get_records().unwrap(),
                                            array.as_array(),
                                        );

                                        std::mem::drop(execution_context);

                                        batches = new_batches;

                                        let new_records = factory.create(&batches);

                                        let dropped_count =
                                            current_batch_record_count - new_records.len();

                                        execution_context = ExecutionContext::new(
                                            diagnostic_level,
                                            //&self.engine.external_function_implementations,
                                            &self.diagnostics,
                                            pipeline,
                                            //&self.global_variables,
                                            //&self.summaries,
                                            Some(new_records),
                                            //None,
                                        );

                                        current_batch_record_count -= dropped_count;
                                        self.dropped_record_count += dropped_count;

                                        execution_context.add_diagnostic_if_enabled(
                                            ColumnarEngineDiagnosticLevel::Info,
                                            d,
                                            || format!("Dropped {dropped_count} record(s)"),
                                        );

                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                execution_context.add_diagnostic_if_enabled(
                                    ColumnarEngineDiagnosticLevel::Error,
                                    d,
                                    || e.to_string(),
                                );
                                break;
                            }
                        }
                    }

                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Info,
                        d,
                        || "All records dropped".into(),
                    );

                    self.dropped_record_count += current_batch_record_count;

                    return;
                }
                DataExpression::Summary(_) => todo!(),
                DataExpression::Transform(t) => {
                    if let Err(e) = match t {
                        TransformExpression::Move(_) => todo!(),
                        TransformExpression::ReduceMap(_) => todo!(),
                        TransformExpression::Remove(_) => todo!(),
                        TransformExpression::RemoveMapKeys(_) => todo!(),
                        TransformExpression::RenameMapKeys(_) => todo!(),
                        TransformExpression::Set(s) => {
                            execute_set_transform_expression(&execution_context, s)
                        }
                    } {
                        execution_context.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Error,
                            t,
                            || e.into_parts().1,
                        );
                        break;
                    }
                }
                DataExpression::Conditional(_) => todo!(),
                DataExpression::Output(_) => todo!(),
            }
        }

        std::mem::drop(execution_context);

        self.included_record_count += current_batch_record_count;

        self.included_batches.push(batches);
    }

    pub fn flush(self) -> ColumnarEngineResults<'a, BATCH_SIZE> {
        ColumnarEngineResults {
            pipeline: &self.engine.pipeline,
            included_batches: self.included_batches,
            included_record_count: self.included_record_count,
            dropped_record_count: self.dropped_record_count,
            diagnostics: self.diagnostics.take(),
        }
    }
}

#[derive(Debug)]
pub struct ColumnarEngineResults<'a, const BATCH_SIZE: usize> {
    pub pipeline: &'a PipelineExpression,
    pub included_batches: Vec<[Option<RecordBatch>; BATCH_SIZE]>,
    pub included_record_count: usize,
    pub dropped_record_count: usize,
    pub diagnostics: Vec<ColumnarEngineDiagnostic<'a>>,
}

impl<const BATCH_SIZE: usize> Display for ColumnarEngineResults<'_, BATCH_SIZE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_diagnostics(self.pipeline.get_query(), &self.diagnostics, f)
    }
}

pub trait ColumnarRecordsFactory<const BATCH_SIZE: usize> {
    type Records<'a>: ColumnarRecords
    where
        Self: 'a;

    fn create<'a>(&self, batches: &'a [Option<RecordBatch>]) -> Self::Records<'a>;

    fn filter(
        &self,
        batch: &Self::Records<'_>,
        filter: &BooleanArray,
    ) -> [Option<RecordBatch>; BATCH_SIZE];
}

pub trait ColumnarRecords: RecordTable
where
    Self: Sized,
{
    fn get_diagnostic_level(&self) -> Option<ColumnarEngineDiagnosticLevel>;

    fn get_key_data_type(&self) -> DataType;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() > 0
    }

    fn get_attached_records(&self, name: &str) -> Option<&dyn RecordTable>;
}

pub trait RecordTable: Display + Debug {
    //fn get_keys(&self) -> &[&str];

    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>>;
}

#[derive(Debug)]
pub enum RecordTableValue<'a> {
    Dictionary(Dictionary<'a>),
    Table(&'a dyn RecordTable),
}

pub fn format_diagnostics(
    query: &str,
    diagnostics: &[ColumnarEngineDiagnostic<'_>],
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let d = write_diagnostics(query, diagnostics, true);

    write!(f, "{d}")
}

fn write_diagnostics(
    query: &str,
    diagnostics: &[ColumnarEngineDiagnostic<'_>],
    all_lines: bool,
) -> String {
    let mut lines: Vec<(&str, Vec<&ColumnarEngineDiagnostic<'_>>)> = Vec::new();

    for line in query.lines() {
        lines.push((line, Vec::new()));
    }

    if lines.is_empty() {
        lines.push(("", Vec::new()));
    }

    for log in diagnostics {
        let location = log.get_expression().get_query_location();
        let (line, _) = location.get_line_and_column_numbers();
        if let Some(l) = lines.get_mut(line - 1) {
            l.1.push(log);
        } else {
            lines[0].1.push(log);
        }
    }

    let mut output = String::new();
    let mut line_number = 1;
    let mut is_first_line = true;

    for (query_line, messages) in lines.iter_mut() {
        if !all_lines && messages.is_empty() {
            line_number += 1;
            continue;
        }

        if is_first_line {
            is_first_line = false;
        } else {
            output.push('\n');
        }

        messages.sort_by(|a, b| {
            let l = a
                .get_expression()
                .get_query_location()
                .get_line_and_column_numbers()
                .1;
            let r = b
                .get_expression()
                .get_query_location()
                .get_line_and_column_numbers()
                .1;
            r.cmp(&l)
        });

        let mut diagnostics = Vec::with_capacity(messages.len());
        let mut columns = HashSet::new();

        for message in messages {
            let mut diagnostic = String::new();

            let (_, column) = message
                .get_expression()
                .get_query_location()
                .get_line_and_column_numbers();

            diagnostic.push_str(&" ".repeat(column + 7));
            diagnostic.push_str("| [");
            diagnostic.push_str(message.get_diagnostic_level().get_name());
            diagnostic.push_str("] ");
            diagnostic.push_str(message.get_expression().get_name());
            diagnostic.push_str(": ");
            diagnostic.push_str(message.get_message());

            diagnostics.push((column, diagnostic));

            columns.insert(column);

            /*if let Some(nested_diagnostics) = message.get_nested_diagnostics() {
                let nested = write_diagnostics(query, nested_diagnostics, false);
                for line in nested.lines() {
                    diagnostics.push((column, format!("{}|    {line}", &" ".repeat(column + 7))));
                }
            }*/
        }

        let mut line = String::new();
        line.push_str(query_line);
        for (diagnostic_column, mut diagnostic) in diagnostics {
            line.push('\n');
            for column in &columns {
                if diagnostic_column > *column {
                    diagnostic.replace_range(column + 7..column + 8, "|");
                }
            }
            line.push_str(&diagnostic);
        }

        write!(output, "ln {line_number:>3}: {line}").unwrap();
        line_number += 1;
    }

    output
}
