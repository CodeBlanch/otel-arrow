// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{cell::RefCell, collections::HashMap, fmt::Display};

use ahash::RandomState;
use arrow::{array::*, datatypes::*};
use data_engine_expressions::*;
use indexmap::IndexSet;

use crate::{execution_context::*, resolved_value::*, scalars::execute_scalar_expression, *};

pub(crate) fn run_scalar_expression_test<FValidate>(
    records: TestRecords,
    expression: ScalarExpression,
    validate: FValidate,
) where
    for<'a> FValidate: FnOnce(ResolvedScalarValue<'a>),
{
    let d = RefCell::new(vec![]);

    let p = Default::default();

    let ec = ExecutionContext::new(ColumnarEngineDiagnosticLevel::Error, &d, &p, Some(records));

    let result = execute_scalar_expression(&ec, &expression);

    validate(result)
}

pub(crate) fn build_indexset_dictionary<'a>(
    keys: Vec<Option<u16>>,
    values: Vec<ValueOrRef<'a>>,
) -> Dictionary<'a> {
    let mut key_builder = PrimitiveBuilder::<UInt16Type>::new();

    for key in keys {
        match key {
            None => key_builder.append_null(),
            Some(k) => key_builder.append_value(k),
        }
    }

    let keys = key_builder.finish();

    let mut set = IndexSet::with_hasher(RandomState::new());

    for value in values {
        let (_, inserted) = set.insert_full(value);
        assert!(inserted);
    }

    Dictionary::new(keys.into(), set.into())
}

#[derive(Debug)]
pub(crate) struct TestRecords<'a> {
    values: HashMap<Box<str>, Dictionary<'a>>,
    attached_records: Option<HashMap<Box<str>, TestRecords<'a>>>,
}

impl<'a> TestRecords<'a> {
    pub fn new(values: HashMap<Box<str>, Dictionary<'a>>) -> TestRecords<'a> {
        Self {
            values,
            attached_records: None,
        }
    }

    pub fn with_attached_records(
        values: HashMap<Box<str>, Dictionary<'a>>,
        attached_records: HashMap<Box<str>, TestRecords<'a>>,
    ) -> TestRecords<'a> {
        Self {
            values,
            attached_records: Some(attached_records),
        }
    }
}

impl<'a> ColumnarRecords for TestRecords<'a> {
    fn get_diagnostic_level(&self) -> Option<ColumnarEngineDiagnosticLevel> {
        None
    }

    fn get_key_data_type(&self) -> DataType {
        DataType::UInt16
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn get_attached_records(&self, name: &str) -> Option<&dyn RecordTable> {
        self.attached_records
            .as_ref()
            .and_then(|a| a.get(name).map(|v| v as &dyn RecordTable))
    }
}

impl<'a> RecordTable for TestRecords<'a> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'a>> {
        self.values
            .get(key)
            .map(|v| RecordTableValue::Dictionary(v.clone()))
    }
}

impl Display for TestRecords<'_> {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
