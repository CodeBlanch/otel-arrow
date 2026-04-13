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

    let range_start_inclusive_expression = slice_scalar_expression.get_range_start();
    let range_start_inclusive = match range_start_inclusive_expression {
        Some(start) => execute_scalar_expression(execution_context, start)?,
        None => ResolvedScalarValue::new_int(0),
    };

    let range_length_expression = slice_scalar_expression.get_range_length();
    let range_length = match range_length_expression {
        Some(length) => execute_scalar_expression(execution_context, length)?,
        None => ResolvedScalarValue::new_null(),
    };

    Ok(match (range_start_inclusive, range_length) {
        (
            ResolvedScalarValue::Single(range_start_inclusive_single),
            ResolvedScalarValue::Single(range_length_single),
        ) => {
            let range_start_inclusive = match range_start_inclusive_expression {
                Some(start) => SliceScalarExpression::validate_resolved_range_value(
                    start.get_query_location(),
                    "start",
                    range_start_inclusive_single.to_value(),
                )?,
                None => 0,
            };

            let range_length = match range_length_expression {
                Some(end) => Some(SliceScalarExpression::validate_resolved_range_value(
                    end.get_query_location(),
                    "length",
                    range_length_single.to_value(),
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
                                range_length,
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
                                            range_length,
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
        (range_start_inclusive, range_length) => {
            // Note: What we know here is that range_start_inclusive and\or range_end_exclusive is a dictionary.
            let (key_count, key_type) = match ResolvedScalarValue::try_get_key_info(&[
                &inner_value,
                &range_start_inclusive,
                &range_length,
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

            let mut range_start_inclusive = match range_start_inclusive
                .try_into_dictionary(key_count, key_type.clone())
            {
                Ok(v) => v,
                Err(_) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        slice_scalar_expression
                            .get_range_start()
                            .expect("has range start"),
                        || "Range start for a slice expression should be an integer type".into(),
                    );
                    return Ok(ResolvedScalarValue::new_null());
                }
            };

            let mut range_length = match range_length.try_into_dictionary(key_count, key_type) {
                Ok(v) => v,
                Err(_) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        slice_scalar_expression
                            .get_range_length()
                            .expect("has range end"),
                        || "Range end for a slice expression should be an integer type".into(),
                    );
                    return Ok(ResolvedScalarValue::new_null());
                }
            };

            range_start_inclusive = range_start_inclusive.transform_into_any(|v| {
                Ok(Some(ValueOrRef::Integer(match v {
                    Some(start) => SliceScalarExpression::validate_resolved_range_value(
                        slice_scalar_expression
                            .get_range_start()
                            .expect("has range start")
                            .get_query_location(),
                        "start",
                        start.to_value(),
                    )? as i64,
                    None => 0,
                })))
            })?;

            range_length = range_length.transform_into_any(|v| {
                Ok(match v {
                    Some(length) => Some(ValueOrRef::Integer(
                        SliceScalarExpression::validate_resolved_range_value(
                            slice_scalar_expression
                                .get_range_length()
                                .expect("has range end")
                                .get_query_location(),
                            "length",
                            length.to_value(),
                        )? as i64,
                    )),
                    None => None,
                })
            })?;

            ResolvedScalarValue::Dictionary(dictionary_merge::merge(
                [inner_value, range_start_inclusive, range_length],
                |mut v| {
                    debug_assert!(v.len() == 3);

                    let (start_inclusive, length) = match (&v[1], &v[2]) {
                        (Some(ValueOrRef::Integer(start)), Some(ValueOrRef::Integer(end))) => {
                            (*start as usize, Some(*end as usize))
                        }
                        (Some(ValueOrRef::Integer(start)), None) => (*start as usize, None),
                        _ => unreachable!(),
                    };

                    Ok(match v[0].take() {
                        Some(ValueOrRef::String(string_value)) => {
                            let end_exlusive = SliceScalarExpression::validate_slice_range(
                                slice_scalar_expression.get_query_location(),
                                "String",
                                string_value.get_value().chars().count(),
                                start_inclusive,
                                length,
                            )?;

                            Some(ValueOrRef::String(StringValueOrRef::new_slice(
                                string_value,
                                start_inclusive,
                                end_exlusive,
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::test_helpers::*;

    use super::*;

    #[test]
    fn test_slice_single_with_single_ranges() {
        todo!()
    }

    #[test]
    fn test_slice_dictionary_with_single_ranges() {
        let values_dictionary = build_indexset_dictionary(
            vec![Some(0), Some(0), None, Some(1), Some(2), Some(3)],
            vec![
                ValueOrRef::String(StringValueOrRef::new_owned("hello world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodbye world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodbye".into())),
                ValueOrRef::Integer(0)
            ]
        );

        let slice_no_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![
                    ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values")))
                ]))),
            None,
            None);

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([("values".into(), values_dictionary.clone())])),
            ScalarExpression::Slice(slice_no_ranges),
            |r| {
                valid_full_range_result(r);
            }
        );

        let slice_full_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![
                    ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values")))
                ]))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(IntegerScalarExpression::new(
                QueryLocation::new_fake(),
                0)))),
            None);

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([("values".into(), values_dictionary.clone())])),
            ScalarExpression::Slice(slice_full_ranges),
            |r| {
                valid_full_range_result(r);
            }
        );

        let slice_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![
                    ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values")))
                ]))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(IntegerScalarExpression::new(
                QueryLocation::new_fake(),
                3)))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(IntegerScalarExpression::new(
                QueryLocation::new_fake(),
                2)))),);

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([("values".into(), values_dictionary.clone())])),
            ScalarExpression::Slice(slice_ranges),
            |r| {
            match r.unwrap() {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_indexset_dictionary(
                            vec![Some(0), Some(0), None, Some(1), Some(1), None],
                            vec![
                                ValueOrRef::String(StringValueOrRef::new_owned("lo".into())),
                                ValueOrRef::String(StringValueOrRef::new_owned("db".into())),
                            ]
                        ),
                        actual
                    );
                }
                _ => assert!(false)
            }
        });

        let slice_ranges_long = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![
                    ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values")))
                ]))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(IntegerScalarExpression::new(
                QueryLocation::new_fake(),
                3)))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(IntegerScalarExpression::new(
                QueryLocation::new_fake(),
                10)))),);

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([("values".into(), values_dictionary)])),
            ScalarExpression::Slice(slice_ranges_long),
            |r| {
            match r.unwrap() {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_indexset_dictionary(
                            vec![Some(0), Some(0), None, Some(1), Some(2), None],
                            vec![
                                ValueOrRef::String(StringValueOrRef::new_owned("lo world".into())),
                                ValueOrRef::String(StringValueOrRef::new_owned("dbye world".into())),
                                ValueOrRef::String(StringValueOrRef::new_owned("dbye".into())),
                            ]
                        ),
                        actual
                    );
                }
                _ => assert!(false)
            }
        });

        // todo: start failure

        // todo: length failure

        // todo: range failure
    }

    fn valid_full_range_result(result: Result<ResolvedScalarValue<'_>, ExpressionError>) {
        match result.unwrap() {
            ResolvedScalarValue::Dictionary(actual) => {
                assert_eq!(
                    build_indexset_dictionary(
                        vec![Some(0), Some(0), None, Some(1), Some(2), None],
                        vec![
                            ValueOrRef::String(StringValueOrRef::new_owned("hello world".into())),
                            ValueOrRef::String(StringValueOrRef::new_owned("goodbye world".into())),
                            ValueOrRef::String(StringValueOrRef::new_owned("goodbye".into())),
                        ]
                    ),
                    actual
                );
            }
            _ => assert!(false)
        }
    }

    #[test]
    fn test_slice_any_with_any_ranges() {
        todo!()
    }
}