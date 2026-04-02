// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::RefCell,
    collections::HashSet,
    fmt::{Debug, Display, Write},
    hash::{Hash, Hasher},
};

use arrow::array::*;
use chrono::{DateTime, FixedOffset, TimeDelta};
use data_engine_expressions::*;
use regex::Regex;

use crate::{
    engine_diagnostic::{ColumnarEngineDiagnostic, ColumnarEngineDiagnosticLevel},
    execution_context::ExecutionContext,
    logical_expressions::execute_logical_expression,
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

    pub fn push_records<'c, TRecordFactory: ColumnarRecordsFactory<BATCH_SIZE>>(
        &mut self,
        factory: &TRecordFactory,
        mut batches: [Option<RecordBatch>; BATCH_SIZE],
    ) {
        let records = factory.create(&batches);

        let diagnostic_level = records
            .get_diagnostic_level()
            .unwrap_or(self.engine.diagnostic_level.clone());

        let mut current_batch_record_count = records.len();

        let pipeline = &self.engine.pipeline;

        let mut execution_context = ExecutionContext::new(
            diagnostic_level.clone(),
            //&self.engine.external_function_implementations,
            &self.diagnostics,
            pipeline,
            //&self.global_variables,
            //&self.summaries,
            //attached_records,
            Some(records),
            //None,
        );

        for expression in pipeline.get_expressions() {
            match expression {
                DataExpression::Discard(d) => {
                    if let Some(predicate) = d.get_predicate() {
                        match execute_logical_expression(&execution_context, predicate) {
                            Ok(logical_result) => {
                                if let Some(s) = logical_result.as_single() {
                                    if s {
                                        execution_context.add_diagnostic_if_enabled(
                                            ColumnarEngineDiagnosticLevel::Verbose,
                                            d,
                                            || "All records included".into(),
                                        );
                                        continue;
                                    }
                                } else if let Some(a) = logical_result.as_array() {
                                    let new_batches =
                                        factory.filter(execution_context.get_records().unwrap(), a);

                                    std::mem::drop(execution_context);

                                    batches = new_batches;

                                    let new_records = factory.create(&batches);

                                    let dropped_count =
                                        current_batch_record_count - new_records.len();

                                    execution_context = ExecutionContext::new(
                                        diagnostic_level.clone(),
                                        //&self.engine.external_function_implementations,
                                        &self.diagnostics,
                                        pipeline,
                                        //&self.global_variables,
                                        //&self.summaries,
                                        //attached_records,
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
                                } else {
                                    todo!()
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
                DataExpression::Summary(s) => todo!(),
                DataExpression::Transform(t) => todo!(),
                DataExpression::Conditional(c) => todo!(),
                DataExpression::Output(o) => todo!(),
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

    fn len(&self) -> usize;
}

pub trait RecordTable: Display + Debug {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>>;
}

#[derive(Debug)]
pub enum RecordTableValue<'a> {
    Dictionary(Dictionary<'a>),
    Table(&'a dyn RecordTable),
}

#[derive(Debug, Clone)]
pub enum ValueOrRef<'a> {
    StringRef(&'a str),
    StringOwned(String),
    IntegerRef(IntegerRef<'a>),
    IntegerOwned(i64),
    DoubleRef(DoubleRef<'a>),
    DoubleOwned(f64),
    BooleanOwned(bool),
    DateTimeOwned(DateTime<FixedOffset>),
    TimeSpanOwned(TimeDelta),
    RegexRef(&'a Regex),
    RegexOwned(Regex),
}

#[derive(Debug, Clone)]
pub enum IntegerRef<'a> {
    Int8(&'a i8),
    Int16(&'a i16),
    Int32(&'a i32),
    Int64(&'a i64),

    UInt8(&'a u8),
    UInt16(&'a u16),
    UInt32(&'a u32),
    UInt64(&'a u64),
}

impl IntegerValue for IntegerRef<'_> {
    fn get_value(&self) -> i64 {
        match self {
            IntegerRef::Int8(i) => (**i).into(),
            IntegerRef::Int16(i) => (**i).into(),
            IntegerRef::Int32(i) => (**i).into(),
            IntegerRef::Int64(i) => **i,
            IntegerRef::UInt8(u) => (**u).into(),
            IntegerRef::UInt16(u) => (**u).into(),
            IntegerRef::UInt32(u) => (**u).into(),
            IntegerRef::UInt64(u) => **u as i64,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DoubleRef<'a> {
    Float16(&'a half::f16),
    Float32(&'a f32),
    Float64(&'a f64),
}

impl DoubleValue for DoubleRef<'_> {
    fn get_value(&self) -> f64 {
        match self {
            DoubleRef::Float16(f) => (**f).into(),
            DoubleRef::Float32(f) => (**f).into(),
            DoubleRef::Float64(f) => **f,
        }
    }
}

impl<'a, 'b> From<&'b ValueOrRef<'a>> for ValueOrRef<'b> {
    fn from(value: &'b ValueOrRef<'a>) -> Self {
        match value {
            ValueOrRef::StringRef(s) => ValueOrRef::StringRef(s),
            ValueOrRef::StringOwned(s) => ValueOrRef::StringRef(s.as_ref()),
            ValueOrRef::IntegerRef(i) => ValueOrRef::IntegerRef(i.clone()),
            ValueOrRef::IntegerOwned(i) => ValueOrRef::IntegerOwned(*i),
            ValueOrRef::DoubleRef(i) => ValueOrRef::DoubleRef(i.clone()),
            ValueOrRef::DoubleOwned(d) => ValueOrRef::DoubleOwned(*d),
            ValueOrRef::BooleanOwned(b) => ValueOrRef::BooleanOwned(*b),
            ValueOrRef::DateTimeOwned(d) => ValueOrRef::DateTimeOwned(*d),
            ValueOrRef::TimeSpanOwned(t) => ValueOrRef::TimeSpanOwned(*t),
            ValueOrRef::RegexRef(r) => ValueOrRef::RegexRef(r),
            ValueOrRef::RegexOwned(r) => ValueOrRef::RegexRef(r),
        }
    }
}

impl Hash for ValueOrRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        //core::mem::discriminant(self).hash(state);
        match self {
            ValueOrRef::StringRef(s) => {
                [0].hash(state);
                s.hash(state);
            }
            ValueOrRef::StringOwned(s) => {
                [0].hash(state);
                s.hash(state);
            }
            ValueOrRef::IntegerRef(i) => {
                [1].hash(state);
                i.get_value().hash(state);
            }
            ValueOrRef::IntegerOwned(i) =>{
                [1].hash(state);
                i.get_value().hash(state);
            }
            ValueOrRef::DoubleRef(d) => {
                [2].hash(state);
                state.write_u64(d.get_value().to_bits());
            }
            ValueOrRef::DoubleOwned(d) => {
                [2].hash(state);
                state.write_u64(d.get_value().to_bits());
            }
            ValueOrRef::BooleanOwned(b) => {
                [3].hash(state);
                b.hash(state);
            }
            ValueOrRef::DateTimeOwned(d) => {
                [4].hash(state);
                d.hash(state);
            }
            ValueOrRef::TimeSpanOwned(t) => {
                [5].hash(state);
                t.hash(state);
            }
            ValueOrRef::RegexRef(r) => {
                [6].hash(state);
                r.as_str().hash(state);
            }
            ValueOrRef::RegexOwned(r) => {
                [6].hash(state);
                r.as_str().hash(state);
            }
        }
    }
}

impl PartialEq for ValueOrRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            ValueOrRef::StringRef(s) => eq_str(s, other),
            ValueOrRef::StringOwned(s) => eq_str(s, other),
            ValueOrRef::IntegerRef(i) => eq_int(i.get_value(), other),
            ValueOrRef::IntegerOwned(i) => eq_int(*i, other),
            ValueOrRef::DoubleRef(d) => todo!(),
            ValueOrRef::DoubleOwned(d) => todo!(),
            ValueOrRef::BooleanOwned(b) => todo!(),
            ValueOrRef::DateTimeOwned(d) => todo!(),
            ValueOrRef::TimeSpanOwned(t) => todo!(),
            ValueOrRef::RegexRef(r) => todo!(),
            ValueOrRef::RegexOwned(r) => todo!(),
        }
    }
}

fn eq_str(left: &str, right: &ValueOrRef) -> bool {
    match right {
        ValueOrRef::StringRef(s) => left == *s,
        ValueOrRef::StringOwned(s) => left == s,
        _ => false
    }
}

fn eq_int(left: i64, right: &ValueOrRef) -> bool {
    match right {
        ValueOrRef::IntegerRef(i) => left == i.get_value(),
        ValueOrRef::IntegerOwned(i) => left == *i,
        _ => false
    }
}

impl Eq for ValueOrRef<'_> {}

impl AsValue for ValueOrRef<'_> {
    fn get_value_type(&self) -> ValueType {
        match self {
            ValueOrRef::StringRef(_) => ValueType::String,
            ValueOrRef::StringOwned(_) => ValueType::String,
            ValueOrRef::IntegerRef(_) => ValueType::Integer,
            ValueOrRef::IntegerOwned(_) => ValueType::Integer,
            ValueOrRef::DoubleRef(_) => ValueType::Double,
            ValueOrRef::DoubleOwned(_) => ValueType::Double,
            ValueOrRef::BooleanOwned(_) => ValueType::Boolean,
            ValueOrRef::DateTimeOwned(_) => ValueType::DateTime,
            ValueOrRef::TimeSpanOwned(_) => ValueType::TimeSpan,
            ValueOrRef::RegexRef(_) => ValueType::Regex,
            ValueOrRef::RegexOwned(_) => ValueType::Regex,
        }
    }

    fn to_value(&self) -> Value<'_> {
        match self {
            ValueOrRef::StringRef(s) => Value::String(s),
            ValueOrRef::StringOwned(s) => Value::String(s),
            ValueOrRef::IntegerRef(i) => Value::Integer(i),
            ValueOrRef::IntegerOwned(i) => Value::Integer(i),
            ValueOrRef::DoubleRef(d) => Value::Double(d),
            ValueOrRef::DoubleOwned(d) => Value::Double(d),
            ValueOrRef::BooleanOwned(b) => Value::Boolean(b),
            ValueOrRef::DateTimeOwned(d) => Value::DateTime(d),
            ValueOrRef::TimeSpanOwned(t) => Value::TimeSpan(t),
            ValueOrRef::RegexRef(r) => Value::Regex(r),
            ValueOrRef::RegexOwned(r) => Value::Regex(r),
        }
    }
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
