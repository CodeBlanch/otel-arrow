// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use ahash::RandomState;
use data_engine_expressions::*;
use indexmap::IndexSet;

use crate::{execution_context::ExecutionContext, *};

pub fn execute_scalar_expression<'a, 'pipeline, 'record, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    scalar_expression: &'pipeline ScalarExpression,
) -> Result<ResolvedValue<'c>, ExpressionError>
where
    'a: 'c,
    'pipeline: 'c,
    'record: 'c,
{
    let value = match scalar_expression {
        ScalarExpression::Argument(_) => todo!(),
        ScalarExpression::Attached(_) => todo!(),
        ScalarExpression::Case(_) => todo!(),
        ScalarExpression::Coalesce(_) => todo!(),
        ScalarExpression::Collection(_) => todo!(),
        ScalarExpression::Conditional(_) => todo!(),
        ScalarExpression::Constant(_) => todo!(),
        ScalarExpression::Convert(_) => todo!(),
        ScalarExpression::GetType(_) => todo!(),
        ScalarExpression::InvokeFunction(_) => todo!(),
        ScalarExpression::Length(l) => {
            let inner_value =
                execute_scalar_expression(execution_context, l.get_inner_expression())?;

            inner_value.map_into(
                |single| {
                    Ok(match single.to_value() {
                        Value::String(s) => ResolvedValue::Single(ResolvedSingleValue::Owned(
                            OwnedValue::Integer(s.get_value().chars().count() as i64),
                        )),
                        Value::Array(a) => ResolvedValue::Single(ResolvedSingleValue::Owned(
                            OwnedValue::Integer(a.len() as i64),
                        )),
                        Value::Map(m) => ResolvedValue::Single(ResolvedSingleValue::Owned(
                            OwnedValue::Integer(m.len() as i64),
                        )),
                        v => {
                            execution_context.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                l,
                                || {
                                    format!(
                                        "Cannot calculate the length of '{}' input",
                                        v.get_value_type()
                                    )
                                },
                            );
                            ResolvedValue::Single(ResolvedSingleValue::Owned(
                                OwnedValue::Null,
                            ))
                        }
                    })
                },
                |dictionary| {
                    Ok(ResolvedValue::Dictionary(dictionary.into_len_dictionary(
                        &execution_context.create_diagnostic_receiver_for_expression(l),
                    )?))
                },
                |_| {
                    todo!()
                }
            )?
        }
        ScalarExpression::Logical(_) => todo!(),
        ScalarExpression::Math(_) => todo!(),
        ScalarExpression::Parse(_) => todo!(),
        ScalarExpression::Select(_) => todo!(),
        ScalarExpression::Slice(s) => {
            let inner_value = execute_scalar_expression(execution_context, s.get_source())?;

            let range_start_inclusive = match s.get_range_start_inclusive() {
                Some(start) => execute_scalar_expression(execution_context, start)?,
                None => ResolvedValue::Single(ResolvedSingleValue::Owned(OwnedValue::Integer(0))),
            };

            let range_end_exclusive = match s.get_range_end_exclusive() {
                Some(end) => Some(execute_scalar_expression(execution_context, end)?),
                None => None,
            };

            todo!()
        }
        ScalarExpression::Source(s) => {
            let mut root = ResolvedValue::Table(execution_context.get_records().unwrap());

            for selector in s.get_value_accessor().get_selectors() {
                let next = execute_scalar_expression(execution_context, selector)?.map_into(
                    |s| match s.to_value() {
                        Value::String(s) => match root {
                            ResolvedValue::Table(t) => Ok(t.get_values(s.get_value())),
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
                                            ResolvedValue::Table(t) => {
                                                if let Some(RecordTableValue::Dictionary(d)) =
                                                    t.get_values(s.get_value())
                                                    && let Some(value_index) =
                                                        d.get_value_index(key)
                                                    && let DictionaryValueArray::ArrayRef(v) =
                                                        d.values()
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
                    |_| todo!()
                )?;

                match next {
                    None => {
                        // todo: Log
                        root = ResolvedValue::Single(ResolvedSingleValue::Owned(OwnedValue::Null));
                        break;
                    }
                    Some(v) => match v {
                        RecordTableValue::Table(t) => root = ResolvedValue::Table(t),
                        RecordTableValue::Dictionary(d) => root = ResolvedValue::Dictionary(d),
                    },
                }
            }

            root
        }
        ScalarExpression::Static(s) => {
            ResolvedValue::Single(ResolvedSingleValue::Ref(s.to_value()))
        }
        ScalarExpression::Temporal(_) => todo!(),
        ScalarExpression::Text(_) => todo!(),
        ScalarExpression::Variable(_) => todo!(),
    };

    execution_context.add_diagnostic_if_enabled(
        ColumnarEngineDiagnosticLevel::Verbose,
        scalar_expression,
        || format!("Evaluated as: {value}"),
    );

    Ok(value)
}
