// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{cell::RefCell, collections::HashMap, fmt::Display};

use arrow::{array::*, datatypes::*};
use data_engine_expressions::*;

use crate::{execution_context::*, resolved_value::*, scalars::execute_scalar_expression, *};

pub(crate) fn run_scalar_expression_test<FValidate>(
    records: TestRecords,
    expression: ScalarExpression,
    validate: FValidate,
) where
    for<'a, 'b> FValidate: FnOnce(ResolvedScalarValue<'a, 'b>),
{
    let d = RefCell::new(vec![]);

    let p = Default::default();

    let ec = ExecutionContext::new(ColumnarEngineDiagnosticLevel::Error, &d, &p, Some(records));

    let result = execute_scalar_expression(&ec, &expression);

    validate(result)
}

pub(crate) fn build_dictionary(
    keys: Vec<Option<u16>>,
    values: Vec<ValueOrRef<'static>>,
) -> RecordTableDictionary {
    let mut key_builder = PrimitiveBuilder::<UInt16Type>::new();

    for key in keys {
        match key {
            None => key_builder.append_null(),
            Some(k) => key_builder.append_value(k),
        }
    }

    let keys = key_builder.finish();

    RecordTableDictionary::new(
        keys.into(),
        RecordTableDictionaryValueArray::Vec(values.into()),
    )
}

#[derive(Debug)]
pub(crate) struct TestRecords {
    values: HashMap<Box<str>, RecordTableDictionary>,
    attached_records: Option<HashMap<Box<str>, TestRecords>>,
}

impl TestRecords {
    pub fn new(values: HashMap<Box<str>, RecordTableDictionary>) -> TestRecords {
        Self {
            values,
            attached_records: None,
        }
    }

    pub fn with_attached_records(
        values: HashMap<Box<str>, RecordTableDictionary>,
        attached_records: HashMap<Box<str>, TestRecords>,
    ) -> TestRecords {
        Self {
            values,
            attached_records: Some(attached_records),
        }
    }
}

impl ColumnarRecords for TestRecords {
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

impl RecordTable for TestRecords {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>> {
        self.values
            .get(key)
            .map(|v| RecordTableValue::Dictionary(v.clone()))
    }
}

impl Display for TestRecords {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
