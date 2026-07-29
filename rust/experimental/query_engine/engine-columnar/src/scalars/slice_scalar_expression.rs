// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, scalars::execute_scalar_expression, *,
};

pub fn execute_slice_scalar_expression<'a, 'pipeline, TRecords: ColumnarRecords<'pipeline>>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    slice_scalar_expression: &'pipeline SliceScalarExpression,
) -> ResolvedScalarValue<'pipeline, 'a> {
    let inner_value =
        execute_scalar_expression(execution_context, slice_scalar_expression.get_source());

    let range_start_inclusive_expression = slice_scalar_expression.get_range_start();
    let range_start_inclusive = match range_start_inclusive_expression {
        Some(start) => execute_scalar_expression(execution_context, start),
        None => ResolvedScalarValue::new_int(0),
    };

    let range_length_expression = slice_scalar_expression.get_range_length();
    let range_length = match range_length_expression {
        Some(length) => execute_scalar_expression(execution_context, length),
        None => ResolvedScalarValue::new_null(),
    };

    match (range_start_inclusive, range_length) {
        (
            ResolvedScalarValue::Single(range_start_inclusive_single),
            ResolvedScalarValue::Single(range_length_single),
        ) => {
            let range_start_inclusive = match range_start_inclusive_expression {
                Some(start) => match SliceScalarExpression::validate_resolved_range_value(
                    start.get_query_location(),
                    "start",
                    range_start_inclusive_single.to_value(),
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        execution_context.add_diagnostic(ColumnarEngineDiagnostic::new(
                            ColumnarEngineDiagnosticLevel::Error,
                            start,
                            e.into_parts().1,
                        ));
                        0
                    }
                },
                None => 0,
            };

            let range_length = match range_length_expression {
                Some(end) => match SliceScalarExpression::validate_resolved_range_value(
                    end.get_query_location(),
                    "length",
                    range_length_single.to_value(),
                ) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        execution_context.add_diagnostic(ColumnarEngineDiagnostic::new(
                            ColumnarEngineDiagnosticLevel::Error,
                            end,
                            e.into_parts().1,
                        ));
                        None
                    }
                },
                None => None,
            };

            inner_value.map_into(
                |single| match single {
                    ValueOrRef::String(string_value) => {
                        match SliceScalarExpression::validate_slice_range(
                            slice_scalar_expression.get_query_location(),
                            "String",
                            string_value.char_len(),
                            range_start_inclusive,
                            range_length,
                        ) {
                            Ok(range_end_exclusive) => StringValueOrRef::new_slice(
                                string_value,
                                range_start_inclusive,
                                range_end_exclusive,
                            )
                            .into(),
                            Err(e) => {
                                execution_context.add_diagnostic(ColumnarEngineDiagnostic::new(
                                    ColumnarEngineDiagnosticLevel::Error,
                                    slice_scalar_expression,
                                    e.into_parts().1,
                                ));
                                ResolvedScalarValue::new_null()
                            }
                        }
                    }
                    ValueOrRef::Array(array_value) => {
                        match SliceScalarExpression::validate_slice_range(
                            slice_scalar_expression.get_query_location(),
                            "Array",
                            array_value.len(),
                            range_start_inclusive,
                            range_length,
                        ) {
                            Ok(range_end_exclusive) => {
                                ArrayValueOrRef::Slice(ArrayValueOrRefSlice::new(
                                    array_value,
                                    range_start_inclusive,
                                    range_end_exclusive,
                                ))
                                .into()
                            }
                            Err(e) => {
                                execution_context.add_diagnostic(ColumnarEngineDiagnostic::new(
                                    ColumnarEngineDiagnosticLevel::Error,
                                    slice_scalar_expression,
                                    e.into_parts().1,
                                ));
                                ResolvedScalarValue::new_null()
                            }
                        }
                    }
                    single => {
                        execution_context.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Warn,
                            slice_scalar_expression,
                            || {
                                format!(
                                    "Cannot take a slice of '{}' input",
                                    single.get_value_type()
                                )
                            },
                        );
                        ResolvedScalarValue::new_null()
                    }
                },
                |dictionary| {
                    ResolvedScalarValue::Dictionary(dictionary.transform_into_any(|v| match v {
                        ValueOrRef::String(string_value) => {
                            match SliceScalarExpression::validate_slice_range(
                                slice_scalar_expression.get_query_location(),
                                "String",
                                string_value.char_len(),
                                range_start_inclusive,
                                range_length,
                            ) {
                                Ok(range_end_exclusive) => {
                                    ValueOrRef::String(StringValueOrRef::new_slice(
                                        string_value.clone(),
                                        range_start_inclusive,
                                        range_end_exclusive,
                                    ))
                                }
                                Err(e) => {
                                    execution_context.add_diagnostic(
                                        ColumnarEngineDiagnostic::new(
                                            ColumnarEngineDiagnosticLevel::Error,
                                            slice_scalar_expression,
                                            e.into_parts().1,
                                        ),
                                    );
                                    ValueOrRef::Null
                                }
                            }
                        }
                        ValueOrRef::Array(array_value) => {
                            match SliceScalarExpression::validate_slice_range(
                                slice_scalar_expression.get_query_location(),
                                "Array",
                                array_value.len(),
                                range_start_inclusive,
                                range_length,
                            ) {
                                Ok(range_end_exclusive) => ValueOrRef::Array(
                                    ArrayValueOrRef::Slice(ArrayValueOrRefSlice::new(
                                        array_value.clone(),
                                        range_start_inclusive,
                                        range_end_exclusive,
                                    )),
                                ),
                                Err(e) => {
                                    execution_context.add_diagnostic(
                                        ColumnarEngineDiagnostic::new(
                                            ColumnarEngineDiagnosticLevel::Error,
                                            slice_scalar_expression,
                                            e.into_parts().1,
                                        ),
                                    );
                                    ValueOrRef::Null
                                }
                            }
                        }
                        v => {
                            execution_context.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                slice_scalar_expression,
                                || format!("Cannot take a slice of '{}' input", v.get_value_type()),
                            );
                            ValueOrRef::Null
                        }
                    }))
                },
                |_| {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        slice_scalar_expression,
                        || "Cannot take a slice of Map input".into(),
                    );
                    ResolvedScalarValue::new_null()
                },
            )
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
                    return ResolvedScalarValue::new_null();
                }
            };

            let inner_value = match inner_value.try_into_dictionary(key_type.clone(), key_count) {
                Ok(v) => v,
                Err(_) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        slice_scalar_expression.get_source(),
                        || "Cannot take a slice of Map input".into(),
                    );
                    return ResolvedScalarValue::new_null();
                }
            };

            let mut range_start_inclusive = match range_start_inclusive
                .try_into_dictionary(key_type.clone(), key_count)
            {
                Ok(v) => v,
                Err(_) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        range_start_inclusive_expression
                            .map(|v| v as &dyn Expression)
                            .unwrap_or(slice_scalar_expression),
                        || "Range start for a slice expression should be an integer type".into(),
                    );
                    return ResolvedScalarValue::new_null();
                }
            };

            let mut range_length = match range_length.try_into_dictionary(key_type, key_count) {
                Ok(v) => v,
                Err(_) => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        range_length_expression
                            .map(|v| v as &dyn Expression)
                            .unwrap_or(slice_scalar_expression),
                        || "Range end for a slice expression should be an integer type".into(),
                    );
                    return ResolvedScalarValue::new_null();
                }
            };

            range_start_inclusive =
                range_start_inclusive.transform_into_any(|v| {
                    let expression = range_start_inclusive_expression
                        .map(|v| v as &dyn Expression)
                        .unwrap_or(slice_scalar_expression);
                    ValueOrRef::Integer(match SliceScalarExpression::validate_resolved_range_value(
                        expression.get_query_location(),
                        "start",
                        v.to_value(),
                    ) {
                        Ok(v) => v as i64,
                        Err(e) => {
                            execution_context.add_diagnostic(ColumnarEngineDiagnostic::new(
                                ColumnarEngineDiagnosticLevel::Error,
                                expression,
                                e.into_parts().1,
                            ));
                            0
                        }
                    })
                });

            range_length = range_length.transform_into_any(|v| {
                let expression = range_length_expression
                    .map(|v| v as &dyn Expression)
                    .unwrap_or(slice_scalar_expression);
                match SliceScalarExpression::validate_resolved_range_value(
                    expression.get_query_location(),
                    "length",
                    v.to_value(),
                ) {
                    Ok(v) => ValueOrRef::Integer(v as i64),
                    Err(e) => {
                        execution_context.add_diagnostic(ColumnarEngineDiagnostic::new(
                            ColumnarEngineDiagnosticLevel::Error,
                            expression,
                            e.into_parts().1,
                        ));
                        ValueOrRef::Null
                    }
                }
            });

            ResolvedScalarValue::Dictionary(dictionary_merge::merge(
                [inner_value, range_start_inclusive, range_length],
                |mut v| {
                    debug_assert!(v.len() == 3);

                    let (start_inclusive, length) = match (v[1].to_value(), v[2].to_value()) {
                        (Value::Integer(start), Value::Integer(end)) => {
                            (start.get_value() as usize, Some(end.get_value() as usize))
                        }
                        (Value::Integer(start), Value::Null) => (start.get_value() as usize, None),
                        _ => unreachable!(),
                    };

                    match std::mem::replace(&mut v[0], ValueOrRef::Null) {
                        ValueOrRef::String(string_value) => {
                            match SliceScalarExpression::validate_slice_range(
                                slice_scalar_expression.get_query_location(),
                                "String",
                                string_value.char_len(),
                                start_inclusive,
                                length,
                            ) {
                                Ok(end_exlusive) => {
                                    ValueOrRef::String(StringValueOrRef::new_slice(
                                        string_value,
                                        start_inclusive,
                                        end_exlusive,
                                    ))
                                }
                                Err(e) => {
                                    execution_context.add_diagnostic(
                                        ColumnarEngineDiagnostic::new(
                                            ColumnarEngineDiagnosticLevel::Error,
                                            slice_scalar_expression,
                                            e.into_parts().1,
                                        ),
                                    );
                                    ValueOrRef::Null
                                }
                            }
                        }
                        ValueOrRef::Array(array_value) => {
                            match SliceScalarExpression::validate_slice_range(
                                slice_scalar_expression.get_query_location(),
                                "Array",
                                array_value.len(),
                                start_inclusive,
                                length,
                            ) {
                                Ok(end_exlusive) => ValueOrRef::Array(ArrayValueOrRef::Slice(
                                    ArrayValueOrRefSlice::new(
                                        array_value,
                                        start_inclusive,
                                        end_exlusive,
                                    ),
                                )),
                                Err(e) => {
                                    execution_context.add_diagnostic(
                                        ColumnarEngineDiagnostic::new(
                                            ColumnarEngineDiagnosticLevel::Error,
                                            slice_scalar_expression,
                                            e.into_parts().1,
                                        ),
                                    );
                                    ValueOrRef::Null
                                }
                            }
                        }
                        v => {
                            execution_context.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                slice_scalar_expression,
                                || format!("Cannot take a slice of '{}' input", v.get_value_type()),
                            );
                            ValueOrRef::Null
                        }
                    }
                },
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::DataType;

    use crate::test_helpers::*;

    use super::*;

    #[test]
    fn test_slice_single_string_with_single_ranges() {
        let slice_no_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                QueryLocation::new_fake(),
                "hello world",
            ))),
            None,
            None,
        );

        run_scalar_expression_test(
            TestRecords::new(),
            ScalarExpression::Slice(slice_no_ranges),
            |r| match r {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(Value::String(&"hello world"), actual.to_value());
                }
                _ => panic!("test failure"),
            },
        );

        let slice_full_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                QueryLocation::new_fake(),
                "hello world",
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 0),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new(),
            ScalarExpression::Slice(slice_full_range),
            |r| match r {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(Value::String(&"hello world"), actual.to_value());
                }
                _ => panic!("test failure"),
            },
        );

        let slice_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                QueryLocation::new_fake(),
                "hello world",
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 3),
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 2),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new(),
            ScalarExpression::Slice(slice_range),
            |r| match r {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(Value::String(&"lo"), actual.to_value());
                }
                _ => panic!("test failure"),
            },
        );

        let slice_invalid_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                QueryLocation::new_fake(),
                "hello world",
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 100),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new(),
            ScalarExpression::Slice(slice_invalid_range),
            |r| {
                matches!(r, ResolvedScalarValue::Single(ValueOrRef::Null));
            },
        );
    }

    #[test]
    fn test_slice_single_array_with_single_ranges() {
        let array_values = vec![
            StaticScalarExpression::Integer(IntegerScalarExpression::new(
                QueryLocation::new_fake(),
                0,
            )),
            StaticScalarExpression::Integer(IntegerScalarExpression::new(
                QueryLocation::new_fake(),
                1,
            )),
            StaticScalarExpression::Integer(IntegerScalarExpression::new(
                QueryLocation::new_fake(),
                2,
            )),
            StaticScalarExpression::Integer(IntegerScalarExpression::new(
                QueryLocation::new_fake(),
                3,
            )),
        ];

        let slice_no_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::Array(ArrayScalarExpression::new(
                QueryLocation::new_fake(),
                array_values.clone(),
            ))),
            None,
            None,
        );

        run_scalar_expression_test(
            TestRecords::new(),
            ScalarExpression::Slice(slice_no_ranges),
            |r| match r {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(
                        Value::Array(&ArrayScalarExpression::new(
                            QueryLocation::new_fake(),
                            array_values.clone()
                        )),
                        actual.to_value()
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let slice_full_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::Array(ArrayScalarExpression::new(
                QueryLocation::new_fake(),
                array_values.clone(),
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 0),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new(),
            ScalarExpression::Slice(slice_full_range),
            |r| match r {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(
                        Value::Array(&ArrayScalarExpression::new(
                            QueryLocation::new_fake(),
                            array_values.clone()
                        )),
                        actual.to_value()
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let slice_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::Array(ArrayScalarExpression::new(
                QueryLocation::new_fake(),
                array_values.clone(),
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 1),
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 2),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new(),
            ScalarExpression::Slice(slice_range),
            |r| match r {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(
                        Value::Array(&ArrayScalarExpression::new(
                            QueryLocation::new_fake(),
                            vec![
                                StaticScalarExpression::Integer(IntegerScalarExpression::new(
                                    QueryLocation::new_fake(),
                                    1
                                )),
                                StaticScalarExpression::Integer(IntegerScalarExpression::new(
                                    QueryLocation::new_fake(),
                                    2
                                )),
                            ]
                        )),
                        actual.to_value()
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let slice_invalid_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::Array(ArrayScalarExpression::new(
                QueryLocation::new_fake(),
                array_values.clone(),
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 100),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new(),
            ScalarExpression::Slice(slice_invalid_range),
            |r| {
                matches!(r, ResolvedScalarValue::Single(ValueOrRef::Null));
            },
        );
    }

    #[test]
    fn test_slice_dictionary_string_with_single_ranges() {
        let values_dictionary = build_dictionary(
            vec![Some(0), Some(0), None, Some(1), Some(2), Some(3)],
            vec![
                ValueOrRef::String(StringValueOrRef::new_owned("hello world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodbye world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodbye".into())),
                ValueOrRef::Integer(0),
            ],
        );

        let slice_no_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            None,
            None,
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_no_ranges),
            |r| {
                valid_full_string_range_result(r);
            },
        );

        let slice_full_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 0),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_full_ranges),
            |r| {
                valid_full_string_range_result(r);
            },
        );

        let slice_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 3),
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 2),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_ranges),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![Some(0), Some(0), None, Some(1), Some(1), None],
                            vec![
                                ValueOrRef::String(StringValueOrRef::new_owned("lo".into())),
                                ValueOrRef::String(StringValueOrRef::new_owned("db".into())),
                            ]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let slice_ranges_long = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 3),
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 10),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_ranges_long),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![Some(0), Some(0), None, Some(1), Some(2), None],
                            vec![
                                ValueOrRef::String(StringValueOrRef::new_owned("lo world".into())),
                                ValueOrRef::String(StringValueOrRef::new_owned(
                                    "dbye world".into()
                                )),
                                ValueOrRef::String(StringValueOrRef::new_owned("dbye".into())),
                            ]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let slice_null = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            None,
            None,
        );

        run_scalar_expression_test(
            TestRecords::new(),
            ScalarExpression::Slice(slice_null),
            |r| match r {
                ResolvedScalarValue::Single(ValueOrRef::Null) => {}
                _ => panic!("test failure"),
            },
        );

        let slice_invalid_start = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::String(
                StringScalarExpression::new(QueryLocation::new_fake(), "invalid"),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_invalid_start),
            |r| {
                valid_full_string_range_result(r);
            },
        );

        let slice_negative_start = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), -1),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_negative_start),
            |r| {
                valid_full_string_range_result(r);
            },
        );

        let slice_invalid_length = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            None,
            Some(ScalarExpression::Static(StaticScalarExpression::String(
                StringScalarExpression::new(QueryLocation::new_fake(), "invalid"),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_invalid_length),
            |r| {
                valid_full_string_range_result(r);
            },
        );

        let slice_negative_length = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            None,
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), -1),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_negative_length),
            |r| {
                valid_full_string_range_result(r);
            },
        );

        let slice_invalid_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 100),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([("values".into(), values_dictionary)])),
            ScalarExpression::Slice(slice_invalid_range),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        Dictionary::new_null_with_data_type(6, DataType::UInt16),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );
    }

    #[test]
    fn test_slice_dictionary_array_with_single_ranges() {
        let values_dictionary = build_dictionary(
            vec![Some(0), Some(1), None, Some(2)],
            vec![
                ValueOrRef::Array([].into()),
                ValueOrRef::Array(
                    [
                        ValueOrRef::Integer(0),
                        ValueOrRef::Integer(1),
                        ValueOrRef::Integer(2),
                        ValueOrRef::Integer(3),
                    ]
                    .into(),
                ),
                ValueOrRef::Integer(0),
            ],
        );

        let slice_no_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            None,
            None,
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_no_ranges),
            |r| {
                valid_full_array_range_result(r);
            },
        );

        let slice_full_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 0),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_full_ranges),
            |r| {
                valid_full_array_range_result(r);
            },
        );

        let slice_ranges = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 1),
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 2),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_ranges),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![None, Some(0), None, None],
                            vec![ValueOrRef::Array(
                                [ValueOrRef::Integer(1), ValueOrRef::Integer(2),].into()
                            ),]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let slice_ranges_long = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 1),
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 10),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Slice(slice_ranges_long),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![None, Some(0), None, None],
                            vec![ValueOrRef::Array(
                                [
                                    ValueOrRef::Integer(1),
                                    ValueOrRef::Integer(2),
                                    ValueOrRef::Integer(3),
                                ]
                                .into()
                            ),]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let slice_invalid_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 100),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([("values".into(), values_dictionary)])),
            ScalarExpression::Slice(slice_invalid_range),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        Dictionary::new_null_with_data_type(4, DataType::UInt16),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );
    }

    #[test]
    fn test_slice_string_any_with_any_ranges() {
        let values_dictionary = build_dictionary(
            vec![
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                None,
                Some(1),
                Some(2),
                Some(3),
            ],
            vec![
                ValueOrRef::String(StringValueOrRef::new_owned("hello world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodbye world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodbye".into())),
                ValueOrRef::Integer(0),
            ],
        );

        let range_start_values = build_dictionary(
            vec![None, None, Some(0), Some(0), None, Some(1), Some(1), None],
            vec![ValueOrRef::Integer(0), ValueOrRef::Integer(3)],
        );

        let range_length_values = build_dictionary(
            vec![None, Some(0), None, Some(0), None, Some(1), Some(1), None],
            vec![ValueOrRef::Integer(2), ValueOrRef::Integer(4)],
        );

        let slice_all_dictionary = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "range_start_values",
                    )),
                )]),
            ))),
            Some(ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "range_length_values",
                    )),
                )]),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([
                ("values".into(), values_dictionary.clone()),
                ("range_start_values".into(), range_start_values),
                ("range_length_values".into(), range_length_values.clone()),
            ])),
            ScalarExpression::Slice(slice_all_dictionary),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![
                                Some(0),
                                Some(1),
                                Some(0),
                                Some(1),
                                None,
                                Some(2),
                                Some(2),
                                None
                            ],
                            vec![
                                ValueOrRef::String(StringValueOrRef::new_owned(
                                    "hello world".into()
                                )),
                                ValueOrRef::String(StringValueOrRef::new_owned("he".into())),
                                ValueOrRef::String(StringValueOrRef::new_owned("dbye".into())),
                            ]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let range_invalid = build_dictionary(
            vec![Some(0)],
            vec![ValueOrRef::String(StringValueOrRef::new_ref("invalid"))],
        );

        let slice_invalid_start = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                QueryLocation::new_fake(),
                "hello world",
            ))),
            Some(ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "range_start_values",
                    )),
                )]),
            ))),
            None,
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "range_start_values".into(),
                range_invalid.clone(),
            )])),
            ScalarExpression::Slice(slice_invalid_start),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![Some(0),],
                            vec![ValueOrRef::String(StringValueOrRef::new_owned(
                                "hello world".into()
                            )),]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let slice_invalid_length = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                QueryLocation::new_fake(),
                "hello world",
            ))),
            None,
            Some(ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "range_length_values",
                    )),
                )]),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "range_length_values".into(),
                range_invalid,
            )])),
            ScalarExpression::Slice(slice_invalid_length),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![Some(0),],
                            vec![ValueOrRef::String(StringValueOrRef::new_owned(
                                "hello world".into()
                            )),]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let range_length_empty = build_dictionary(vec![None], vec![]);

        let slice_invalid_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                QueryLocation::new_fake(),
                "hello world",
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 100),
            ))),
            Some(ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "range_length_values",
                    )),
                )]),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "range_length_values".into(),
                range_length_empty,
            )])),
            ScalarExpression::Slice(slice_invalid_range),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(build_dictionary(vec![None], vec![]), actual);
                }
                _ => panic!("test failure"),
            },
        );
    }

    #[test]
    fn test_slice_array_any_with_any_ranges() {
        let values_dictionary = build_dictionary(
            vec![Some(0), Some(0), Some(0), Some(0), None, Some(1)],
            vec![
                ValueOrRef::Array(
                    [
                        ValueOrRef::Integer(0),
                        ValueOrRef::Integer(1),
                        ValueOrRef::Integer(2),
                        ValueOrRef::Integer(3),
                    ]
                    .into(),
                ),
                ValueOrRef::Integer(0),
            ],
        );

        let range_start_values = build_dictionary(
            vec![None, None, Some(0), Some(1), None, Some(1)],
            vec![ValueOrRef::Integer(0), ValueOrRef::Integer(3)],
        );

        let range_length_values = build_dictionary(
            vec![None, Some(0), None, Some(1), None, Some(1)],
            vec![ValueOrRef::Integer(2), ValueOrRef::Integer(4)],
        );

        let slice_all_dictionary = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "values",
                    )),
                )]),
            )),
            Some(ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "range_start_values",
                    )),
                )]),
            ))),
            Some(ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "range_length_values",
                    )),
                )]),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([
                ("values".into(), values_dictionary.clone()),
                ("range_start_values".into(), range_start_values),
                ("range_length_values".into(), range_length_values.clone()),
            ])),
            ScalarExpression::Slice(slice_all_dictionary),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![Some(0), Some(1), Some(0), Some(2), None, None],
                            vec![
                                ValueOrRef::Array(
                                    [
                                        ValueOrRef::Integer(0),
                                        ValueOrRef::Integer(1),
                                        ValueOrRef::Integer(2),
                                        ValueOrRef::Integer(3),
                                    ]
                                    .into(),
                                ),
                                ValueOrRef::Array(
                                    [ValueOrRef::Integer(0), ValueOrRef::Integer(1),].into(),
                                ),
                                ValueOrRef::Array([ValueOrRef::Integer(3),].into(),),
                            ]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let range_length_empty = build_dictionary(vec![None], vec![]);

        let slice_invalid_range = SliceScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::Array(ArrayScalarExpression::new(
                QueryLocation::new_fake(),
                vec![],
            ))),
            Some(ScalarExpression::Static(StaticScalarExpression::Integer(
                IntegerScalarExpression::new(QueryLocation::new_fake(), 100),
            ))),
            Some(ScalarExpression::Source(SourceScalarExpression::new(
                QueryLocation::new_fake(),
                ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                    StaticScalarExpression::String(StringScalarExpression::new(
                        QueryLocation::new_fake(),
                        "range_length_values",
                    )),
                )]),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new().with_values(HashMap::from([(
                "range_length_values".into(),
                range_length_empty,
            )])),
            ScalarExpression::Slice(slice_invalid_range),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(build_dictionary(vec![None], vec![]), actual);
                }
                _ => panic!("test failure"),
            },
        );
    }

    fn valid_full_string_range_result(result: ResolvedScalarValue<'_, '_>) {
        match result {
            ResolvedScalarValue::Dictionary(actual) => {
                assert_eq!(
                    build_dictionary(
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
            _ => panic!("test failure"),
        }
    }

    fn valid_full_array_range_result(result: ResolvedScalarValue<'_, '_>) {
        match result {
            ResolvedScalarValue::Dictionary(actual) => {
                assert_eq!(
                    build_dictionary(
                        vec![None, Some(0), None, None],
                        vec![ValueOrRef::Array(
                            [
                                ValueOrRef::Integer(0),
                                ValueOrRef::Integer(1),
                                ValueOrRef::Integer(2),
                                ValueOrRef::Integer(3),
                            ]
                            .into()
                        ),]
                    ),
                    actual
                );
            }
            _ => panic!("test failure"),
        }
    }
}
