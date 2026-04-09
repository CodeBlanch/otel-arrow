// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use ahash::RandomState;
use data_engine_expressions::*;
use indexmap::IndexSet;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, scalars::execute_scalar_expression, *,
};

// todo: can we use one of the dictionary helpers to drive this?
pub fn execute_source_scalar_expression<'a, 'pipeline, 'record, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    source_scalar_expression: &'pipeline SourceScalarExpression,
) -> Result<ResolvedScalarValue<'c>, ExpressionError>
where
    'a: 'c,
    'pipeline: 'c,
    'record: 'c,
{
    let mut root = ResolvedScalarValue::Table(execution_context.get_records().unwrap());

    for selector in source_scalar_expression
        .get_value_accessor()
        .get_selectors()
    {
        let next = execute_scalar_expression(execution_context, selector)?.map_into(
            |s| match s.to_value() {
                Value::String(s) => match root {
                    ResolvedScalarValue::Table(t) => Ok(t.get_values(s.get_value())),
                    _ => todo!(),
                },
                _ => todo!(),
            },
            |d| {
                // todo: Improve the perf of this
                let key_count = d.len();

                let mut key_builder = d.keys().create_builder();
                let mut values = IndexSet::with_hasher(RandomState::new());

                for key in 0..key_count {
                    if let Some(v) = d.get_value(key) {
                        match v.to_value() {
                            Value::String(s) => {
                                let value = match root {
                                    ResolvedScalarValue::Table(t) => {
                                        if let Some(RecordTableValue::Dictionary(d)) =
                                            t.get_values(s.get_value())
                                            && let Some(value_index) = d.get_value_index(key)
                                            && let DictionaryValueArray::ArrayRef(v) = d.values()
                                        {
                                            get_value_from_array(*v, value_index)
                                        } else {
                                            todo!()
                                        }
                                    }
                                    _ => todo!(),
                                };
                                if let Some(v) = value {
                                    let (index, _) = values.insert_full(v);
                                    key_builder.push_value_index(index);
                                } else {
                                    key_builder.push_null();
                                }
                            }
                            _ => todo!(),
                        }
                    } else {
                        key_builder.push_null();
                    }
                }

                Ok(Some(RecordTableValue::Dictionary(Dictionary::new(
                    key_builder.finish(),
                    DictionaryValueArray::IndexAnyOwned(values),
                ))))
            },
            |_| todo!(),
        )?;

        match next {
            None => {
                // todo: Log
                root = ResolvedScalarValue::new_null();
                break;
            }
            Some(v) => match v {
                RecordTableValue::Table(t) => root = ResolvedScalarValue::Table(t),
                RecordTableValue::Dictionary(d) => root = ResolvedScalarValue::Dictionary(d),
            },
        }
    }

    Ok(root)
}
