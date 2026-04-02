// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, hash_map::Entry};

use arrow::array::*;
use data_engine_expressions::*;

use crate::{execution_context::ExecutionContext, scalars::execute_scalar_expression, *};

pub fn execute_logical_expression<'a, 'pipeline, 'record, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    logical_expression: &'pipeline LogicalExpression,
) -> Result<ResolvedBooleanValue<'c>, ExpressionError>
where
    'a: 'c,
    'pipeline: 'c,
    'record: 'c,
{
    let value = match logical_expression {
        LogicalExpression::Scalar(s) => {
            let inner_value = execute_scalar_expression(execution_context, s)?;

            inner_value.map_into(
                |single| {
                    if let Some(b) = single.to_value().convert_to_bool() {
                        Ok(ResolvedBooleanValue::Single(b))
                    } else {
                        Err(ExpressionError::TypeMismatch(
                            s.get_query_location().clone(),
                            format!(
                                "Value of '{:?}' type returned by scalar expression could not be converted to bool",
                                single.get_value_type()
                            ),
                        ))
                    }
                },
                |dictionary| {
                    let (keys, values) = dictionary.into_parts();
                    Ok(if let DictionaryKeyArray::BooleanRef(a) = keys {
                        ResolvedBooleanValue::ArrayRef(a)
                    } else if let DictionaryKeyArray::BooleanOwned(a) = keys {
                        ResolvedBooleanValue::ArrayOwned(a)
                    } else {
                        ResolvedBooleanValue::ArrayOwned(
                            Dictionary::new(keys, values).transform_into_boolean(
                                &execution_context.create_diagnostic_receiver_for_expression(s),
                                |_, v| {
                                    let value = v.as_ref().map(|v| v.to_value()).unwrap_or_else(|| Value::Null);

                                    if let Some(b) = value.convert_to_bool() {
                                        Ok(Some(b))
                                    } else {
                                        Err(ExpressionError::TypeMismatch(
                                            s.get_query_location().clone(),
                                            format!(
                                                "Value of '{:?}' type returned by scalar expression could not be converted to bool",
                                                value.get_value_type()
                                            ),
                                        ))
                                    }
                                },
                            )?,
                        )
                    })
                },
                |_| {
                    Err(ExpressionError::TypeMismatch(
                        s.get_query_location().clone(),
                        "Table type returned by scalar expression could not be converted to bool".into(),
                    ))
                }
            )?
        }
        LogicalExpression::EqualTo(e) => compare(
            e.get_query_location(),
            &execution_context.create_diagnostic_receiver_for_expression(e),
            execute_scalar_expression(execution_context, e.get_left())?,
            execute_scalar_expression(execution_context, e.get_right())?,
            |l, r| Value::are_values_equal(e.get_query_location(), l, r, e.get_case_insensitive()),
        )?,
        LogicalExpression::GreaterThan(g) => compare(
            g.get_query_location(),
            &execution_context.create_diagnostic_receiver_for_expression(g),
            execute_scalar_expression(execution_context, g.get_left())?,
            execute_scalar_expression(execution_context, g.get_right())?,
            |l, r| {
                Ok(match (l, r) {
                    (Value::Null, _) => false,
                    (_, Value::Null) => false,
                    (l, r) => Value::compare_values(g.get_query_location(), l, r)? > 0,
                })
            },
        )?,
        LogicalExpression::GreaterThanOrEqualTo(g) => compare(
            g.get_query_location(),
            &execution_context.create_diagnostic_receiver_for_expression(g),
            execute_scalar_expression(execution_context, g.get_left())?,
            execute_scalar_expression(execution_context, g.get_right())?,
            |l, r| {
                Ok(match (l, r) {
                    (Value::Null, Value::Null) => true,
                    (Value::Null, _) => false,
                    (_, Value::Null) => false,
                    (l, r) => Value::compare_values(g.get_query_location(), l, r)? >= 0,
                })
            },
        )?,
        LogicalExpression::Not(n) => {
            match execute_logical_expression(execution_context, n.get_inner_expression())? {
                ResolvedBooleanValue::Single(s) => ResolvedBooleanValue::Single(!s),
                ResolvedBooleanValue::ArrayRef(t) => {
                    ResolvedBooleanValue::ArrayOwned(arrow::compute::not(t).unwrap())
                }
                ResolvedBooleanValue::ArrayOwned(t) => {
                    ResolvedBooleanValue::ArrayOwned(arrow::compute::not(&t).unwrap())
                }
            }
        }
        LogicalExpression::And(a) => {
            let left = execute_logical_expression(execution_context, a.get_left())?;

            if let Some(left) = left.as_single() {
                if !left {
                    ResolvedBooleanValue::Single(false)
                } else {
                    execute_logical_expression(execution_context, a.get_right())?
                }
            } else if let Some(left_array) = left.as_array() {
                if left_array.false_count() == left_array.len() {
                    ResolvedBooleanValue::Single(false)
                } else {
                    let right = execute_logical_expression(execution_context, a.get_right())?;

                    if let Some(right) = right.as_single() {
                        if !right {
                            ResolvedBooleanValue::Single(false)
                        } else {
                            left
                        }
                    } else if let Some(right) = right.as_array() {
                        ResolvedBooleanValue::ArrayOwned(
                            arrow::compute::and(left_array, right).expect("and operation failed"),
                        )
                    } else {
                        unreachable!("right wasn't a single or an array")
                    }
                }
            } else {
                unreachable!("left wasn't a single or an array")
            }
        }
        LogicalExpression::Or(o) => {
            let left = execute_logical_expression(execution_context, o.get_left())?;

            if let Some(left) = left.as_single() {
                if left {
                    ResolvedBooleanValue::Single(true)
                } else {
                    execute_logical_expression(execution_context, o.get_right())?
                }
            } else if let Some(left_array) = left.as_array() {
                if left_array.true_count() == left_array.len() {
                    ResolvedBooleanValue::Single(true)
                } else {
                    let right = execute_logical_expression(execution_context, o.get_right())?;

                    if let Some(right) = right.as_single() {
                        if right {
                            ResolvedBooleanValue::Single(true)
                        } else {
                            left
                        }
                    } else if let Some(right) = right.as_array() {
                        ResolvedBooleanValue::ArrayOwned(
                            arrow::compute::or(left_array, right).expect("or operation failed"),
                        )
                    } else {
                        unreachable!("right wasn't a single or an array")
                    }
                }
            } else {
                unreachable!("left wasn't a single or an array")
            }
        }
        LogicalExpression::Contains(c) => compare(
            c.get_query_location(),
            &execution_context.create_diagnostic_receiver_for_expression(c),
            execute_scalar_expression(execution_context, c.get_haystack())?,
            execute_scalar_expression(execution_context, c.get_needle())?,
            |l, r| Value::contains(c.get_query_location(), l, r, c.get_case_insensitive()),
        )?,
        LogicalExpression::Matches(m) => compare(
            m.get_query_location(),
            &execution_context.create_diagnostic_receiver_for_expression(m),
            execute_scalar_expression(execution_context, m.get_haystack())?,
            execute_scalar_expression(execution_context, m.get_pattern())?,
            |l, r| Value::matches(m.get_query_location(), l, r),
        )?,
    };

    execution_context.add_diagnostic_if_enabled(
        ColumnarEngineDiagnosticLevel::Verbose,
        logical_expression,
        || format!("Evaluated as: {value}"),
    );

    Ok(value)
}

fn compare<'record, D: DiagnosticReceiver, F>(
    query_location: &QueryLocation,
    diagnostic_receiver: &D,
    left: ResolvedValue<'_>,
    right: ResolvedValue<'_>,
    compare: F,
) -> Result<ResolvedBooleanValue<'record>, ExpressionError>
where
    F: Fn(&Value, &Value) -> Result<bool, ExpressionError>,
{
    let (left_single, left_dictionary) = match left {
        ResolvedValue::Single(s) => (Some(s), None),
        ResolvedValue::Dictionary(d) => (None, Some(d)),
        _ => unreachable!(),
    };

    let (right_single, right_dictionary) = match right {
        ResolvedValue::Single(s) => (Some(s), None),
        ResolvedValue::Dictionary(d) => (None, Some(d)),
        _ => unreachable!(),
    };

    let value = if let Some(left) = left_single {
        if let Some(right) = right_single {
            ResolvedBooleanValue::Single(compare(&left.to_value(), &right.to_value())?)
        } else {
            ResolvedBooleanValue::ArrayOwned(compare_single_to_dictionary(
                diagnostic_receiver,
                &left,
                right_dictionary.expect("right is dictionary"),
                compare,
            )?)
        }
    } else if let Some(right) = right_single {
        ResolvedBooleanValue::ArrayOwned(compare_dictionary_to_single(
            diagnostic_receiver,
            left_dictionary.expect("left is dictionary"),
            &right,
            compare,
        )?)
    } else {
        ResolvedBooleanValue::ArrayOwned(compare_dictionary_to_dictionary(
            query_location,
            left_dictionary.expect("left is dictionary"),
            right_dictionary.expect("right is dictionary"),
            compare,
        )?)
    };

    Ok(value)
}

fn compare_dictionary_to_single<D: DiagnosticReceiver, F>(
    diagnostic_receiver: &D,
    dictionary: Dictionary,
    value: &ResolvedSingleValue,
    compare: F,
) -> Result<BooleanArray, ExpressionError>
where
    F: Fn(&Value, &Value) -> Result<bool, ExpressionError>,
{
    let right = value.to_value();

    dictionary.transform_into_boolean(diagnostic_receiver, |_, v| {
        Ok(Some(match v {
            None => compare(&Value::Null, &right)?,
            Some(v) => compare(&v.to_value(), &right)?,
        }))
    })
}

fn compare_single_to_dictionary<D: DiagnosticReceiver, F>(
    diagnostic_receiver: &D,
    value: &ResolvedSingleValue,
    dictionary: Dictionary,
    compare: F,
) -> Result<BooleanArray, ExpressionError>
where
    F: Fn(&Value, &Value) -> Result<bool, ExpressionError>,
{
    let left = value.to_value();

    dictionary.transform_into_boolean(diagnostic_receiver, |_, v| {
        Ok(Some(match v {
            None => compare(&left, &Value::Null)?,
            Some(v) => compare(&left, &v.to_value())?,
        }))
    })
}

fn compare_dictionary_to_dictionary<F>(
    query_location: &QueryLocation,
    left: Dictionary,
    right: Dictionary,
    compare: F,
) -> Result<BooleanArray, ExpressionError>
where
    F: Fn(&Value, &Value) -> Result<bool, ExpressionError>,
{
    let key_len = left.len();

    if key_len != right.len() {
        return Err(ExpressionError::ValidationFailure(
            query_location.clone(),
            "Cannot compare tables of different sizes".into(),
        ));
    }

    let left_keys = left.keys();
    let left_values = left.values();

    let right_keys = right.keys();
    let right_values = right.values();

    let mut value_lookup =
        HashMap::with_capacity(std::cmp::max(left_values.len(), right_values.len()));

    let mut builder = BooleanBuilder::with_capacity(key_len);

    for key_index in 0..key_len {
        let value_indicies = (
            left_keys.get_value_index_for_key_index(key_index),
            right_keys.get_value_index_for_key_index(key_index),
        );

        let value = match value_lookup.entry(value_indicies) {
            Entry::Occupied(occupied) => *occupied.get(),
            Entry::Vacant(vacant) => {
                let (left_value_index, right_value_index) = vacant.key();

                let left_value = left_value_index.and_then(|i| left_values.get_value_at(i));
                let right_value = right_value_index.and_then(|i| right_values.get_value_at(i));

                let value = compare(
                    left_value
                        .as_ref()
                        .map(|v| v.to_value())
                        .as_ref()
                        .unwrap_or(&Value::Null),
                    right_value
                        .as_ref()
                        .map(|v| v.to_value())
                        .as_ref()
                        .unwrap_or(&Value::Null),
                )?;

                vacant.insert(value);
                value
            }
        };

        builder.append_value(value);
    }

    Ok(builder.finish())
}
