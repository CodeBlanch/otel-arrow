// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use ahash::RandomState;
use data_engine_expressions::*;
use indexmap::IndexSet;

use crate::{
    execution_context::ExecutionContext,
    resolved_value::*,
    scalars::{
        length_scalar_expression::execute_length_scalar_expression,
        slice_scalar_expression::execute_slice_scalar_expression,
    },
    *,
};

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
        ScalarExpression::Length(l) => execute_length_scalar_expression(execution_context, l)?,
        ScalarExpression::Logical(_) => todo!(),
        ScalarExpression::Math(_) => todo!(),
        ScalarExpression::Parse(_) => todo!(),
        ScalarExpression::Select(_) => todo!(),
        ScalarExpression::Slice(s) => execute_slice_scalar_expression(execution_context, s)?,
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
                    |_| todo!(),
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
