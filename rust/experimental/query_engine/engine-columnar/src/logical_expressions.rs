// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, hash_map::Entry};

use arrow::array::*;
use data_engine_expressions::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, scalars::execute_scalar_expression, *,
};

pub fn execute_logical_expression<'a, 'pipeline, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    logical_expression: &'pipeline LogicalExpression,
) -> Result<ResolvedLogicalValue<'c>, ExpressionError>
where
    'a: 'c,
    'pipeline: 'c,
{
    let value = match logical_expression {
        LogicalExpression::Scalar(s) => {
            let inner_value = execute_scalar_expression(execution_context, s)?;

            inner_value.map_into(
                |single| {
                    if let Some(b) = single.to_value().convert_to_bool() {
                        Ok(ResolvedLogicalValue::Single(b))
                    } else {
                        Err(ExpressionError::TypeMismatch(
                            s.get_query_location().clone(),
                            format!(
                                "Value of '{}' type returned by scalar expression could not be converted to bool",
                                single.get_value_type()
                            ),
                        ))
                    }
                },
                |dictionary| {
                    let (keys, values) = dictionary.into_parts();
                    Ok(if let DictionaryKeyArray::BooleanRef(a) = keys {
                        ResolvedLogicalValue::DictionaryRef(a)
                    } else if let DictionaryKeyArray::BooleanOwned(a) = keys {
                        ResolvedLogicalValue::DictionaryOwned(a)
                    } else {
                        ResolvedLogicalValue::DictionaryOwned(
                            Dictionary::new(keys, values).transform_into_boolean(
                                |v| {
                                    let value = v.to_value();

                                    if let Some(b) = value.convert_to_bool() {
                                        Ok(Some(b))
                                    } else {
                                        Err(ExpressionError::TypeMismatch(
                                            s.get_query_location().clone(),
                                            format!(
                                                "Value of '{}' type returned by scalar expression could not be converted to bool",
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
            execute_scalar_expression(execution_context, e.get_left())?,
            execute_scalar_expression(execution_context, e.get_right())?,
            |l, r| Value::are_values_equal(e.get_query_location(), l, r, e.get_case_insensitive()),
        )?,
        LogicalExpression::GreaterThan(g) => compare(
            g.get_query_location(),
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
                ResolvedLogicalValue::Single(s) => ResolvedLogicalValue::Single(!s),
                ResolvedLogicalValue::DictionaryRef(t) => {
                    ResolvedLogicalValue::DictionaryOwned(arrow::compute::not(t).unwrap())
                }
                ResolvedLogicalValue::DictionaryOwned(t) => {
                    ResolvedLogicalValue::DictionaryOwned(arrow::compute::not(&t).unwrap())
                }
            }
        }
        LogicalExpression::And(a) => {
            let left = execute_logical_expression(execution_context, a.get_left())?;

            if let Some(left) = left.as_single() {
                if !left {
                    ResolvedLogicalValue::Single(false)
                } else {
                    execute_logical_expression(execution_context, a.get_right())?
                }
            } else if let Some(left_array) = left.as_array() {
                if left_array.false_count() == left_array.len() {
                    ResolvedLogicalValue::Single(false)
                } else {
                    let right = execute_logical_expression(execution_context, a.get_right())?;

                    if let Some(right) = right.as_single() {
                        if !right {
                            ResolvedLogicalValue::Single(false)
                        } else {
                            left
                        }
                    } else if let Some(right) = right.as_array() {
                        ResolvedLogicalValue::DictionaryOwned(
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
                    ResolvedLogicalValue::Single(true)
                } else {
                    execute_logical_expression(execution_context, o.get_right())?
                }
            } else if let Some(left_array) = left.as_array() {
                if left_array.true_count() == left_array.len() {
                    ResolvedLogicalValue::Single(true)
                } else {
                    let right = execute_logical_expression(execution_context, o.get_right())?;

                    if let Some(right) = right.as_single() {
                        if right {
                            ResolvedLogicalValue::Single(true)
                        } else {
                            left
                        }
                    } else if let Some(right) = right.as_array() {
                        ResolvedLogicalValue::DictionaryOwned(
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
            execute_scalar_expression(execution_context, c.get_haystack())?,
            execute_scalar_expression(execution_context, c.get_needle())?,
            |l, r| Value::contains(c.get_query_location(), l, r, c.get_case_insensitive()),
        )?,
        LogicalExpression::Matches(m) => compare(
            m.get_query_location(),
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

fn compare<'record, FCompare>(
    query_location: &QueryLocation,
    left: ResolvedScalarValue<'_>,
    right: ResolvedScalarValue<'_>,
    compare: FCompare,
) -> Result<ResolvedLogicalValue<'record>, ExpressionError>
where
    FCompare: Fn(&Value, &Value) -> Result<bool, ExpressionError>,
{
    let (left_single, left_dictionary) = match left {
        ResolvedScalarValue::Single(s) => (Some(s), None),
        ResolvedScalarValue::Dictionary(d) => (None, Some(d)),
        _ => unreachable!(),
    };

    let (right_single, right_dictionary) = match right {
        ResolvedScalarValue::Single(s) => (Some(s), None),
        ResolvedScalarValue::Dictionary(d) => (None, Some(d)),
        _ => unreachable!(),
    };

    let value = if let Some(left) = left_single {
        if let Some(right) = right_single {
            ResolvedLogicalValue::Single(compare(&left.to_value(), &right.to_value())?)
        } else {
            ResolvedLogicalValue::DictionaryOwned(compare_single_to_dictionary(
                &left,
                right_dictionary.expect("right is dictionary"),
                compare,
            )?)
        }
    } else if let Some(right) = right_single {
        ResolvedLogicalValue::DictionaryOwned(compare_dictionary_to_single(
            left_dictionary.expect("left is dictionary"),
            &right,
            compare,
        )?)
    } else {
        ResolvedLogicalValue::DictionaryOwned(compare_dictionary_to_dictionary(
            query_location,
            left_dictionary.expect("left is dictionary"),
            right_dictionary.expect("right is dictionary"),
            compare,
        )?)
    };

    Ok(value)
}

fn compare_dictionary_to_single<FCompare>(
    dictionary: Dictionary,
    value: &ValueOrRef,
    compare: FCompare,
) -> Result<BooleanArray, ExpressionError>
where
    FCompare: Fn(&Value, &Value) -> Result<bool, ExpressionError>,
{
    let right = value.to_value();

    dictionary.transform_into_boolean(|v| Ok(Some(compare(&v.to_value(), &right)?)))
}

fn compare_single_to_dictionary<FCompare>(
    value: &ValueOrRef,
    dictionary: Dictionary,
    compare: FCompare,
) -> Result<BooleanArray, ExpressionError>
where
    FCompare: Fn(&Value, &Value) -> Result<bool, ExpressionError>,
{
    let left = value.to_value();

    dictionary.transform_into_boolean(|v| Ok(Some(compare(&left, &v.to_value())?)))
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

                let left_value = left_value_index
                    .map(|i| left_values.get_value_at(i))
                    .unwrap_or(ValueOrRef::Null);
                let right_value = right_value_index
                    .map(|i| right_values.get_value_at(i))
                    .unwrap_or(ValueOrRef::Null);

                let value = compare(&left_value.to_value(), &right_value.to_value())?;

                vacant.insert(value);
                value
            }
        };

        builder.append_value(value);
    }

    Ok(builder.finish())
}
