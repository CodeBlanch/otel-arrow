// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, scalars::execute_scalar_expression,
};

pub fn execute_length_scalar_expression<'a, 'pipeline, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    length_scalar_expression: &'pipeline LengthScalarExpression,
) -> Result<ResolvedScalarValue<'c>, ExpressionError>
where
    'a: 'c,
    'pipeline: 'c,
{
    let inner_value = execute_scalar_expression(
        execution_context,
        length_scalar_expression.get_inner_expression(),
    )?;

    inner_value.map_into(
        |single| {
            Ok(match single.to_value() {
                Value::String(s) => {
                    ResolvedScalarValue::new_int(s.get_value().chars().count() as i64)
                }
                Value::Array(a) => ResolvedScalarValue::new_int(a.len() as i64),
                Value::Map(m) => ResolvedScalarValue::new_int(m.len() as i64),
                v => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        length_scalar_expression,
                        || {
                            format!(
                                "Cannot calculate the length of '{}' input",
                                v.get_value_type()
                            )
                        },
                    );
                    ResolvedScalarValue::new_null()
                }
            })
        },
        |dictionary| {
            Ok(ResolvedScalarValue::Dictionary(
                dictionary.transform_into_any(|v| {
                    Ok(match v.to_value() {
                        Value::String(s) => {
                            ValueOrRef::Integer(s.get_value().chars().count() as i64)
                        }
                        Value::Map(m) => ValueOrRef::Integer(m.len() as i64),
                        Value::Array(a) => ValueOrRef::Integer(a.len() as i64),
                        v => {
                            execution_context.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                length_scalar_expression,
                                || {
                                    format!(
                                        "Cannot calculate the length of '{}' input",
                                        v.get_value_type()
                                    )
                                },
                            );
                            ValueOrRef::Null
                        }
                    })
                })?,
            ))
        },
        |_| {
            // what is length of table? a dictionary where each record points to a count of key\values?
            // that would make it equivalent to len(single_map)
            todo!()
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::test_helpers::*;

    use super::*;

    #[test]
    fn test_length_single() {
        let length_string = LengthScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::String(StringScalarExpression::new(
                QueryLocation::new_fake(),
                "hello world",
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::new()),
            ScalarExpression::Length(length_string),
            |r| match r.unwrap() {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(Value::Integer(&11), actual.to_value());
                }
                _ => panic!("test failure"),
            },
        );

        let length_array = LengthScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::Array(ArrayScalarExpression::new(
                QueryLocation::new_fake(),
                vec![],
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::new()),
            ScalarExpression::Length(length_array),
            |r| match r.unwrap() {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(Value::Integer(&0), actual.to_value());
                }
                _ => panic!("test failure"),
            },
        );

        let length_map = LengthScalarExpression::new(
            QueryLocation::new_fake(),
            ScalarExpression::Static(StaticScalarExpression::Map(MapScalarExpression::new(
                QueryLocation::new_fake(),
                HashMap::new(),
            ))),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::new()),
            ScalarExpression::Length(length_map),
            |r| match r.unwrap() {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(Value::Integer(&0), actual.to_value());
                }
                _ => panic!("test failure"),
            },
        );
    }

    #[test]
    fn test_length_dictionary() {
        let array = ArrayScalarExpression::new(QueryLocation::new_fake(), vec![]);
        let map = MapScalarExpression::new(QueryLocation::new_fake(), HashMap::new());

        let values_dictionary = build_indexset_dictionary(
            vec![Some(0), Some(0), None, Some(1), Some(2), Some(3)],
            vec![
                ValueOrRef::String(StringValueOrRef::new_owned("hello world".into())),
                ValueOrRef::Array(ArrayValueOrRef::Ref(&array)),
                ValueOrRef::Map(MapValueOrRef::Ref(&map)),
                ValueOrRef::Integer(0),
            ],
        );

        let length = LengthScalarExpression::new(
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
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Length(length),
            |r| match r.unwrap() {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_indexset_dictionary(
                            vec![Some(0), Some(0), None, Some(1), Some(1), None],
                            vec![ValueOrRef::Integer(11), ValueOrRef::Integer(0),]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );
    }
}
