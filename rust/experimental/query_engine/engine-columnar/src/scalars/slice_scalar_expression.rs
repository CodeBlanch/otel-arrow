// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, scalars::execute_scalar_expression, *,
};

pub fn execute_slice_scalar_expression<'a, 'pipeline, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    slice_scalar_expression: &'pipeline SliceScalarExpression,
) -> Result<ResolvedScalarValue<'c>, ExpressionError>
where
    'a: 'c,
    'pipeline: 'c,
{
    let inner_value =
        execute_scalar_expression(execution_context, slice_scalar_expression.get_source())?;

    let range_start_inclusive_expression = slice_scalar_expression.get_range_start_inclusive();
    let range_start_inclusive = match range_start_inclusive_expression {
        Some(start) => execute_scalar_expression(execution_context, start)?,
        None => ResolvedScalarValue::new_int(0),
    };

    let range_end_exclusive_expression = slice_scalar_expression.get_range_end_exclusive();
    let range_end_exclusive = match range_end_exclusive_expression {
        Some(end) => execute_scalar_expression(execution_context, end)?,
        None => ResolvedScalarValue::new_null(),
    };

    Ok(match (range_start_inclusive, range_end_exclusive) {
        (
            ResolvedScalarValue::Single(range_start_inclusive_single),
            ResolvedScalarValue::Single(range_end_exclusive_single),
        ) => {
            let range_start_inclusive = match range_start_inclusive_expression {
                Some(start) => SliceScalarExpression::validate_resolved_range_value(
                    start.get_query_location(),
                    "start",
                    range_start_inclusive_single.to_value(),
                )?,
                None => 0,
            };

            let range_end_exclusive = match range_end_exclusive_expression {
                Some(end) => Some(SliceScalarExpression::validate_resolved_range_value(
                    end.get_query_location(),
                    "end",
                    range_end_exclusive_single.to_value(),
                )?),
                None => None,
            };

            inner_value.map_into(
                |single| {
                    Ok(match TryInto::<StringValueOrRef>::try_into(single) {
                        Ok(string_value) => {
                            let range_end_exclusive = SliceScalarExpression::validate_slice_range(
                                slice_scalar_expression.get_query_location(),
                                "String",
                                string_value.get_value().chars().count(),
                                range_start_inclusive,
                                range_end_exclusive,
                            )?;

                            StringValueOrRef::new_slice(
                                string_value,
                                range_start_inclusive,
                                range_end_exclusive,
                            )
                            .into()
                        }
                        // todo: support arrays
                        Err(inner_value_single) => {
                            execution_context.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                slice_scalar_expression,
                                || {
                                    format!(
                                        "Cannot take a slice of '{}' input",
                                        inner_value_single.get_value_type()
                                    )
                                },
                            );
                            ResolvedScalarValue::new_null()
                        }
                    })
                },
                |dictionary| {
                    dictionary
                        .transform_into_any(|v| {
                            Ok(match v {
                                Some(ValueOrRef::String(string_value)) => {
                                    let range_end_exclusive =
                                        SliceScalarExpression::validate_slice_range(
                                            slice_scalar_expression.get_query_location(),
                                            "String",
                                            string_value.get_value().chars().count(),
                                            range_start_inclusive,
                                            range_end_exclusive,
                                        )?;

                                    Some(ValueOrRef::String(StringValueOrRef::new_slice(
                                        string_value,
                                        range_start_inclusive,
                                        range_end_exclusive,
                                    )))
                                }
                                // todo: support arrays
                                v => {
                                    execution_context.add_diagnostic_if_enabled(
                                        ColumnarEngineDiagnosticLevel::Warn,
                                        slice_scalar_expression,
                                        || {
                                            format!(
                                                "Cannot take a slice of '{}' input",
                                                v.as_ref().map_or(ValueType::Null, |v| v
                                                    .get_value_type())
                                            )
                                        },
                                    );
                                    None
                                }
                            })
                        })
                        .map(ResolvedScalarValue::Dictionary)
                },
                |_| {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        slice_scalar_expression,
                        || "Cannot take a slice of Map input".into(),
                    );
                    Ok(ResolvedScalarValue::new_null())
                },
            )?
        }
        (range_start_inclusive, range_end_exclusive) => {
            // Note: What we know here is that range_start_inclusive and\or range_end_exclusive is a dictionary.
            let (key_count, key_type) = match ResolvedScalarValue::try_get_key_info(&[
                &inner_value,
                &range_start_inclusive,
                &range_end_exclusive,
            ]) {
                Ok(v) => v,
                Err(_) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        slice_scalar_expression,
                        || "Cannot take a slice of Map input".into(),
                    );
                    return Ok(ResolvedScalarValue::new_null());
                }
            };

            let inner_value = match inner_value.try_into_dictionary(key_count, key_type.clone()) {
                Ok(v) => v,
                Err(_) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        slice_scalar_expression.get_source(),
                        || "Cannot take a slice of Map input".into(),
                    );
                    return Ok(ResolvedScalarValue::new_null());
                }
            };

            let mut range_start = match range_start_inclusive
                .try_into_dictionary(key_count, key_type.clone())
            {
                Ok(v) => v,
                Err(_) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        slice_scalar_expression
                            .get_range_start_inclusive()
                            .expect("has range start"),
                        || "Range start for a slice expression should be an integer type".into(),
                    );
                    return Ok(ResolvedScalarValue::new_null());
                }
            };

            let mut range_end = match range_end_exclusive.try_into_dictionary(key_count, key_type) {
                Ok(v) => v,
                Err(_) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        slice_scalar_expression
                            .get_range_end_exclusive()
                            .expect("has range end"),
                        || "Range end for a slice expression should be an integer type".into(),
                    );
                    return Ok(ResolvedScalarValue::new_null());
                }
            };

            range_start = range_start.transform_into_any(|v| {
                Ok(Some(ValueOrRef::Integer(match v {
                    Some(start) => SliceScalarExpression::validate_resolved_range_value(
                        slice_scalar_expression
                            .get_range_start_inclusive()
                            .expect("has range start")
                            .get_query_location(),
                        "start",
                        start.to_value(),
                    )? as i64,
                    None => 0,
                })))
            })?;

            range_end = range_end.transform_into_any(|v| {
                Ok(match v {
                    Some(end) => Some(ValueOrRef::Integer(
                        SliceScalarExpression::validate_resolved_range_value(
                            slice_scalar_expression
                                .get_range_end_exclusive()
                                .expect("has range end")
                                .get_query_location(),
                            "end",
                            end.to_value(),
                        )? as i64,
                    )),
                    None => None,
                })
            })?;

            ResolvedScalarValue::Dictionary(dictionary_merge::merge(
                [inner_value, range_start, range_end],
                |mut v| {
                    debug_assert!(v.len() == 3);

                    let (start, end) = match (&v[1], &v[2]) {
                        (Some(ValueOrRef::Integer(start)), Some(ValueOrRef::Integer(end))) => {
                            (*start as usize, Some(*end as usize))
                        }
                        (Some(ValueOrRef::Integer(start)), None) => (*start as usize, None),
                        _ => todo!(),
                    };

                    Ok(match v[0].take() {
                        Some(ValueOrRef::String(string_value)) => {
                            let end = SliceScalarExpression::validate_slice_range(
                                slice_scalar_expression.get_query_location(),
                                "String",
                                string_value.get_value().chars().count(),
                                start,
                                end,
                            )?;

                            Some(ValueOrRef::String(StringValueOrRef::new_slice(
                                string_value,
                                start,
                                end,
                            )))
                        }
                        // todo: support arrays
                        v => {
                            execution_context.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                slice_scalar_expression,
                                || {
                                    format!(
                                        "Cannot take a slice of '{}' input",
                                        v.map(|v| v.get_value_type()).unwrap_or(ValueType::Null)
                                    )
                                },
                            );
                            None
                        }
                    })
                },
            )?)
        }
    })
}
