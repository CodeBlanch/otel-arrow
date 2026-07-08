// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use arrow::array::*;
use data_engine_expressions::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, scalars::execute_scalar_expression, *,
};

pub fn execute_logical_expression<'a, 'pipeline, TRecords: ColumnarRecords<'pipeline>>(
    execution_context: &ExecutionContext<'a, 'pipeline, TRecords>,
    logical_expression: &'pipeline LogicalExpression,
) -> ResolvedLogicalValue {
    let value = match logical_expression {
        LogicalExpression::Scalar(s) => {
            let inner_value = execute_scalar_expression(execution_context, s);

            inner_value.map_into(
                |single| {
                    match single.to_value() {
                        Value::Boolean(b) => ResolvedLogicalValue::Single(b.get_value()),
                        v => {
                            if let Some(b) = v.convert_to_bool() {
                                ResolvedLogicalValue::Single(b)
                            } else {
                                execution_context.add_diagnostic_if_enabled(
                                    ColumnarEngineDiagnosticLevel::Warn,
                                    s,
                                    ||
                                        format!(
                                            "Value of '{}' type returned by scalar expression could not be converted to bool",
                                            v.get_value_type()
                                        ),
                                    );
                                ResolvedLogicalValue::Single(false)
                            }
                        }
                    }
                },
                |dictionary| {
                    let (keys, values) = dictionary.into_parts();
                    if let DictionaryKeyArray::BooleanArray{data_type, values} = keys {
                        ResolvedLogicalValue::Array{data_type, values}
                    } else {
                        ResolvedLogicalValue::Array { data_type: keys.data_type(),
                            values: Arc::new(Dictionary::new(keys, values).transform_into_boolean(
                                |v| {
                                    match v.to_value() {
                                        Value::Null => None,
                                        Value::Boolean(b) => Some(b.get_value()),
                                        v => {
                                            if let Some(b) = v.convert_to_bool() {
                                                Some(b)
                                            } else {
                                                execution_context.add_diagnostic_if_enabled(
                                                    ColumnarEngineDiagnosticLevel::Warn,
                                                    s,
                                                    ||
                                                        format!(
                                                            "Value of '{}' type returned by scalar expression could not be converted to bool",
                                                            v.get_value_type()
                                                        ),
                                                    );
                                                None
                                            }
                                        }
                                    }
                                },
                            )),
                        }
                    }
                },
                |_| {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        s,
                        ||
                            "Table type returned by scalar expression could not be converted to bool".into(),
                        );
                    ResolvedLogicalValue::Single(false)
                }
            )
        }
        LogicalExpression::EqualTo(e) => compare(
            execute_scalar_expression(execution_context, e.get_left()),
            execute_scalar_expression(execution_context, e.get_right()),
            |l, r| match Value::are_values_equal(
                e.get_query_location(),
                l,
                r,
                e.get_case_insensitive(),
            ) {
                Ok(v) => v,
                Err(err) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        e,
                        || err.into_parts().1,
                    );
                    false
                }
            },
        ),
        LogicalExpression::GreaterThan(g) => compare(
            execute_scalar_expression(execution_context, g.get_left()),
            execute_scalar_expression(execution_context, g.get_right()),
            |l, r| match (l, r) {
                (Value::Null, _) => false,
                (_, Value::Null) => false,
                (l, r) => match Value::compare_values(g.get_query_location(), l, r) {
                    Ok(v) => v > 0,
                    Err(err) => {
                        execution_context.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Warn,
                            g,
                            || err.into_parts().1,
                        );
                        false
                    }
                },
            },
        ),
        LogicalExpression::GreaterThanOrEqualTo(g) => compare(
            execute_scalar_expression(execution_context, g.get_left()),
            execute_scalar_expression(execution_context, g.get_right()),
            |l, r| match (l, r) {
                (Value::Null, Value::Null) => true,
                (Value::Null, _) => false,
                (_, Value::Null) => false,
                (l, r) => match Value::compare_values(g.get_query_location(), l, r) {
                    Ok(v) => v >= 0,
                    Err(err) => {
                        execution_context.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Warn,
                            g,
                            || err.into_parts().1,
                        );
                        false
                    }
                },
            },
        ),
        LogicalExpression::Not(n) => {
            match execute_logical_expression(execution_context, n.get_inner_expression()) {
                ResolvedLogicalValue::Single(s) => ResolvedLogicalValue::Single(!s),
                ResolvedLogicalValue::Array { data_type, values } => ResolvedLogicalValue::Array {
                    data_type,
                    values: Arc::new(arrow::compute::not(values.as_boolean()).unwrap()),
                },
            }
        }
        LogicalExpression::And(a) => execute_logical_expression(execution_context, a.get_left())
            .map_into(
                |left_single| {
                    if !left_single {
                        ResolvedLogicalValue::Single(false)
                    } else {
                        execute_logical_expression(execution_context, a.get_right())
                    }
                },
                |data_type, left_array| {
                    let left_as_array = left_array.as_boolean();

                    if left_as_array.false_count() == left_as_array.len() {
                        ResolvedLogicalValue::Single(false)
                    } else {
                        execute_logical_expression(execution_context, a.get_right())
                            .map_into_with_state(
                                left_array,
                                |left_array, right_single| {
                                    if !right_single {
                                        ResolvedLogicalValue::Single(false)
                                    } else {
                                        ResolvedLogicalValue::Array {
                                            data_type,
                                            values: left_array,
                                        }
                                    }
                                },
                                |left_array, data_type, right_array| ResolvedLogicalValue::Array {
                                    data_type,
                                    values: Arc::new(
                                        arrow::compute::and(
                                            left_array.as_boolean(),
                                            right_array.as_boolean(),
                                        )
                                        .expect("and operation failed"),
                                    ),
                                },
                            )
                    }
                },
            ),
        LogicalExpression::Or(o) => execute_logical_expression(execution_context, o.get_left())
            .map_into(
                |left_single| {
                    if left_single {
                        ResolvedLogicalValue::Single(true)
                    } else {
                        execute_logical_expression(execution_context, o.get_right())
                    }
                },
                |data_type, left_array| {
                    let left_as_array = left_array.as_boolean();

                    if left_as_array.true_count() == left_as_array.len() {
                        ResolvedLogicalValue::Single(true)
                    } else {
                        execute_logical_expression(execution_context, o.get_right())
                            .map_into_with_state(
                                left_array,
                                |left_array, right_single| {
                                    if right_single {
                                        ResolvedLogicalValue::Single(true)
                                    } else {
                                        ResolvedLogicalValue::Array {
                                            data_type,
                                            values: left_array,
                                        }
                                    }
                                },
                                |left_array, data_type, right_array| ResolvedLogicalValue::Array {
                                    data_type,
                                    values: Arc::new(
                                        arrow::compute::or(
                                            left_array.as_boolean(),
                                            right_array.as_boolean(),
                                        )
                                        .expect("or operation failed"),
                                    ),
                                },
                            )
                    }
                },
            ),
        LogicalExpression::Contains(c) => compare(
            execute_scalar_expression(execution_context, c.get_haystack()),
            execute_scalar_expression(execution_context, c.get_needle()),
            |l, r| match Value::contains(c.get_query_location(), l, r, c.get_case_insensitive()) {
                Ok(v) => v,
                Err(err) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        c,
                        || err.into_parts().1,
                    );
                    false
                }
            },
        ),
        LogicalExpression::Matches(m) => compare(
            execute_scalar_expression(execution_context, m.get_haystack()),
            execute_scalar_expression(execution_context, m.get_pattern()),
            |l, r| match Value::matches(m.get_query_location(), l, r) {
                Ok(v) => v,
                Err(err) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        m,
                        || err.into_parts().1,
                    );
                    false
                }
            },
        ),
    };

    execution_context.add_diagnostic_if_enabled(
        ColumnarEngineDiagnosticLevel::Verbose,
        logical_expression,
        || format!("Evaluated as: {value}"),
    );

    value
}

fn compare<FCompare>(
    left: ResolvedScalarValue<'_, '_>,
    right: ResolvedScalarValue<'_, '_>,
    compare: FCompare,
) -> ResolvedLogicalValue
where
    FCompare: Fn(&Value, &Value) -> bool,
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

    let (data_type, compare_result) = if let Some(left) = left_single {
        if let Some(right) = right_single {
            return ResolvedLogicalValue::Single(compare(&left.to_value(), &right.to_value()));
        }

        let data_type = right_dictionary
            .as_ref()
            .expect("right is dictionary")
            .keys()
            .data_type();

        let compare_result = compare_single_to_dictionary(
            &left,
            right_dictionary.expect("right is dictionary"),
            compare,
        );

        (data_type, compare_result)
    } else if let Some(right) = right_single {
        let data_type = left_dictionary
            .as_ref()
            .expect("left is dictionary")
            .keys()
            .data_type();

        let compare_result = compare_dictionary_to_single(
            left_dictionary.expect("left is dictionary"),
            &right,
            compare,
        );

        (data_type, compare_result)
    } else {
        let data_type = left_dictionary
            .as_ref()
            .expect("left is dictionary")
            .keys()
            .data_type();

        let compare_result = compare_dictionary_to_dictionary(
            left_dictionary.expect("left is dictionary"),
            right_dictionary.expect("right is dictionary"),
            compare,
        );

        (data_type, compare_result)
    };

    if compare_result.true_count() == compare_result.len() {
        ResolvedLogicalValue::Single(true)
    } else if compare_result.false_count() == compare_result.len() {
        ResolvedLogicalValue::Single(false)
    } else {
        ResolvedLogicalValue::Array {
            data_type,
            values: Arc::new(compare_result),
        }
    }
}

fn compare_dictionary_to_single<FCompare>(
    dictionary: Dictionary,
    value: &ValueOrRef,
    compare: FCompare,
) -> BooleanArray
where
    FCompare: Fn(&Value, &Value) -> bool,
{
    let right = value.to_value();

    dictionary.transform_into_boolean(|v| Some(compare(&v.to_value(), &right)))
}

fn compare_single_to_dictionary<FCompare>(
    value: &ValueOrRef,
    dictionary: Dictionary,
    compare: FCompare,
) -> BooleanArray
where
    FCompare: Fn(&Value, &Value) -> bool,
{
    let left = value.to_value();

    dictionary.transform_into_boolean(|v| Some(compare(&left, &v.to_value())))
}

fn compare_dictionary_to_dictionary<F>(
    left: Dictionary,
    right: Dictionary,
    compare: F,
) -> BooleanArray
where
    F: Fn(&Value, &Value) -> bool,
{
    let key_len = left.len();

    // Note: Left and Right should be the same length but it isn't a panic type
    // of issue if they aren't.
    debug_assert!(key_len == right.len());

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

                let value = compare(&left_value.to_value(), &right_value.to_value());

                vacant.insert(value);
                value
            }
        };

        builder.append_value(value);
    }

    builder.finish()
}
