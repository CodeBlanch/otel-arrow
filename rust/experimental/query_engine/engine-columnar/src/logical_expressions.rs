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
            let value = execute_scalar_expression(execution_context, s)?;

            if let Some(single) = value.as_single() {
                if let Some(b) = single.to_value().convert_to_bool() {
                    ResolvedBooleanValue::Single(b)
                } else {
                    return Err(ExpressionError::TypeMismatch(
                        s.get_query_location().clone(),
                        format!(
                            "Value of '{:?}' type returned by scalar expression could not be converted to bool",
                            single.get_value_type()
                        ),
                    ));
                }
            } else if let Ok(t) = value.into_dictionary() {
                let (keys, values) = t.into_parts();
                if let DictionaryKeyArray::BooleanRef(a) = keys {
                    ResolvedBooleanValue::ArrayRef(a)
                } else if let DictionaryKeyArray::BooleanOwned(a) = keys {
                    ResolvedBooleanValue::ArrayOwned(a)
                } else {
                    ResolvedBooleanValue::ArrayOwned(
                        Dictionary::new(keys, values).transform_into_boolean(
                            &execution_context.create_diagnostic_receiver_for_expression(s),
                            |_, v| {
                                if let Some(v) = v.as_ref().map(|v| v.to_value()) {
                                    v.convert_to_bool()
                                } else {
                                    None
                                }
                            },
                        )?,
                    )
                }
            } else {
                todo!()
            }
        }
        LogicalExpression::EqualTo(e) => compare(
            e.get_query_location(),
            &execute_scalar_expression(execution_context, e.get_left())?,
            &execute_scalar_expression(execution_context, e.get_right())?,
            |l, r| {
                Value::are_values_equal(e.get_query_location(), &l, &r, e.get_case_insensitive())
            },
        )?,
        LogicalExpression::GreaterThan(g) => compare(
            g.get_query_location(),
            &execute_scalar_expression(execution_context, g.get_left())?,
            &execute_scalar_expression(execution_context, g.get_right())?,
            |l, r| Ok(Value::compare_values(g.get_query_location(), &l, &r)? > 0),
        )?,
        LogicalExpression::GreaterThanOrEqualTo(g) => compare(
            g.get_query_location(),
            &execute_scalar_expression(execution_context, g.get_left())?,
            &execute_scalar_expression(execution_context, g.get_right())?,
            |l, r| Ok(Value::compare_values(g.get_query_location(), &l, &r)? >= 0),
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
            match execute_logical_expression(execution_context, a.get_left())? {
                ResolvedBooleanValue::Single(l) => {
                    if !l {
                        // todo: Log
                        ResolvedBooleanValue::Single(false)
                    } else {
                        match execute_logical_expression(execution_context, a.get_right())? {
                            ResolvedBooleanValue::Single(r) => ResolvedBooleanValue::Single(r),
                            ResolvedBooleanValue::ArrayRef(a) => ResolvedBooleanValue::ArrayRef(a),
                            ResolvedBooleanValue::ArrayOwned(a) => {
                                ResolvedBooleanValue::ArrayOwned(a)
                            }
                        }
                    }
                }
                ResolvedBooleanValue::ArrayRef(l) => {
                    let right = execute_logical_expression(execution_context, a.get_right())?;

                    compare(
                        a.get_query_location(),
                        &ResolvedValue::Dictionary(l.into()),
                        &right.into(),
                        |l, r| {
                            Ok(match (l, r) {
                                (Value::Null, _) => false,
                                (_, Value::Null) => false,
                                (Value::Boolean(l), Value::Boolean(r)) => {
                                    l.get_value() && r.get_value()
                                }
                                _ => unreachable!(),
                            })
                        },
                    )?
                }
                ResolvedBooleanValue::ArrayOwned(l) => {
                    let right = execute_logical_expression(execution_context, a.get_right())?;

                    compare(
                        a.get_query_location(),
                        &ResolvedValue::Dictionary(l.into()),
                        &right.into(),
                        |l, r| {
                            Ok(match (l, r) {
                                (Value::Null, _) => false,
                                (_, Value::Null) => false,
                                (Value::Boolean(l), Value::Boolean(r)) => {
                                    l.get_value() && r.get_value()
                                }
                                _ => unreachable!(),
                            })
                        },
                    )?
                }
            }
        }
        LogicalExpression::Or(o) => {
            match execute_logical_expression(execution_context, o.get_left())? {
                ResolvedBooleanValue::Single(l) => {
                    if l {
                        // todo: Log
                        ResolvedBooleanValue::Single(true)
                    } else {
                        match execute_logical_expression(execution_context, o.get_right())? {
                            ResolvedBooleanValue::Single(r) => ResolvedBooleanValue::Single(r),
                            ResolvedBooleanValue::ArrayRef(a) => ResolvedBooleanValue::ArrayRef(a),
                            ResolvedBooleanValue::ArrayOwned(a) => {
                                ResolvedBooleanValue::ArrayOwned(a)
                            }
                        }
                    }
                }
                ResolvedBooleanValue::ArrayRef(l) => {
                    let right = execute_logical_expression(execution_context, o.get_right())?;

                    compare(
                        o.get_query_location(),
                        &ResolvedValue::Dictionary(l.into()),
                        &right.into(),
                        |l, r| {
                            Ok(match (l, r) {
                                (Value::Null, Value::Null) => false,
                                (Value::Boolean(l), Value::Null) => l.get_value(),
                                (Value::Null, Value::Boolean(r)) => r.get_value(),
                                (Value::Boolean(l), Value::Boolean(r)) => {
                                    l.get_value() || r.get_value()
                                }
                                _ => unreachable!(),
                            })
                        },
                    )?
                }
                ResolvedBooleanValue::ArrayOwned(l) => {
                    let right = execute_logical_expression(execution_context, o.get_right())?;

                    compare(
                        o.get_query_location(),
                        &ResolvedValue::Dictionary(l.into()),
                        &right.into(),
                        |l, r| {
                            Ok(match (l, r) {
                                (Value::Null, Value::Null) => false,
                                (Value::Boolean(l), Value::Null) => l.get_value(),
                                (Value::Null, Value::Boolean(r)) => r.get_value(),
                                (Value::Boolean(l), Value::Boolean(r)) => {
                                    l.get_value() || r.get_value()
                                }
                                _ => unreachable!(),
                            })
                        },
                    )?
                }
            }
        }
        LogicalExpression::Contains(c) => compare(
            c.get_query_location(),
            &execute_scalar_expression(execution_context, c.get_haystack())?,
            &execute_scalar_expression(execution_context, c.get_needle())?,
            |l, r| Value::contains(c.get_query_location(), &l, &r, c.get_case_insensitive()),
        )?,
        LogicalExpression::Matches(m) => compare(
            m.get_query_location(),
            &execute_scalar_expression(execution_context, m.get_haystack())?,
            &execute_scalar_expression(execution_context, m.get_pattern())?,
            |l, r| Value::matches(m.get_query_location(), &l, &r),
        )?,
    };

    execution_context.add_diagnostic_if_enabled(
        ColumnarEngineDiagnosticLevel::Verbose,
        logical_expression,
        || format!("Evaluated as: {value}"),
    );

    Ok(value)
}

fn compare<'record, F>(
    query_location: &QueryLocation,
    left: &ResolvedValue<'_>,
    right: &ResolvedValue<'_>,
    compare: F,
) -> Result<ResolvedBooleanValue<'record>, ExpressionError>
where
    F: Fn(Value, Value) -> Result<bool, ExpressionError>,
{
    let single_left = left.as_single();
    let single_right = right.as_single();

    let value = if let Some(left) = single_left
        && let Some(right) = single_right
    {
        ResolvedBooleanValue::Single(compare(left.to_value(), right.to_value())?)
    } else if let Some(left) = left.as_dictionary()
        && let Some(right) = single_right
    {
        ResolvedBooleanValue::ArrayOwned(compare_table_to_single(left, right, compare)?)
    } else if let Some(left) = single_left
        && let Some(right) = right.as_dictionary()
    {
        ResolvedBooleanValue::ArrayOwned(compare_single_to_table(left, right, compare)?)
    } else if let Some(left) = left.as_dictionary()
        && let Some(right) = right.as_dictionary()
    {
        ResolvedBooleanValue::ArrayOwned(compare_table_to_table(
            query_location,
            left,
            right,
            compare,
        )?)
    } else {
        todo!()
    };

    Ok(value)
}

fn compare_table_to_single<F>(
    dictionary: &Dictionary,
    value: &ResolvedSingleValue,
    compare: F,
) -> Result<BooleanArray, ExpressionError>
where
    F: Fn(Value, Value) -> Result<bool, ExpressionError>,
{
    let source_keys = dictionary.keys();
    let source_key_len = source_keys.len();
    let source_values = dictionary.values();

    let mut value_lookup = HashMap::with_capacity(source_values.len());
    let mut null_value = None;

    let mut builder = BooleanBuilder::with_capacity(source_key_len);

    for source_key_index in 0..source_key_len {
        match source_keys.get_value_index_for_key_index(source_key_index) {
            Some(value_index) => {
                let value = match value_lookup.entry(value_index) {
                    Entry::Occupied(occupied) => *occupied.get(),
                    Entry::Vacant(vacant) => {
                        let source_value = source_values.get_value_at(*vacant.key());
                        let value = compare(
                            source_value.as_ref().map_or(Value::Null, |v| v.to_value()),
                            value.to_value(),
                        )?;

                        vacant.insert(value);
                        value
                    }
                };

                builder.append_value(value);
            }
            None => {
                let v = match null_value {
                    Some(v) => v,
                    None => {
                        let v = compare(Value::Null, value.to_value())?;
                        null_value = Some(v);
                        v
                    }
                };
                builder.append_value(v);
            }
        }
    }

    Ok(builder.finish())
}

fn compare_single_to_table<F>(
    value: &ResolvedSingleValue,
    dictionary: &Dictionary,
    compare: F,
) -> Result<BooleanArray, ExpressionError>
where
    F: Fn(Value, Value) -> Result<bool, ExpressionError>,
{
    let source_keys = dictionary.keys();
    let source_key_len = source_keys.len();
    let source_values = dictionary.values();

    let mut value_lookup = HashMap::with_capacity(source_values.len());
    let mut null_value = None;

    let mut builder = BooleanBuilder::with_capacity(source_key_len);

    for source_key_index in 0..source_key_len {
        match source_keys.get_value_index_for_key_index(source_key_index) {
            Some(value_index) => {
                let value = match value_lookup.entry(value_index) {
                    Entry::Occupied(occupied) => *occupied.get(),
                    Entry::Vacant(vacant) => {
                        let source_value = source_values.get_value_at(*vacant.key());
                        let value = compare(
                            value.to_value(),
                            source_value.as_ref().map_or(Value::Null, |v| v.to_value()),
                        )?;

                        vacant.insert(value);
                        value
                    }
                };

                builder.append_value(value);
            }
            None => {
                let v = match null_value {
                    Some(v) => v,
                    None => {
                        let v = compare(Value::Null, value.to_value())?;
                        null_value = Some(v);
                        v
                    }
                };
                builder.append_value(v);
            }
        }
    }

    Ok(builder.finish())
}

fn compare_table_to_table<F>(
    query_location: &QueryLocation,
    left: &Dictionary,
    right: &Dictionary,
    compare: F,
) -> Result<BooleanArray, ExpressionError>
where
    F: Fn(Value, Value) -> Result<bool, ExpressionError>,
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
                    left_value.as_ref().map_or(Value::Null, |v| v.to_value()),
                    right_value.as_ref().map_or(Value::Null, |v| v.to_value()),
                )?;

                vacant.insert(value);
                value
            }
        };

        builder.append_value(value);
    }

    Ok(builder.finish())
}
