// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{execution_context::ExecutionContext, resolved_value::*, selection::select, *};

pub fn execute_variable_scalar_expression<'a, 'pipeline, TRecords: ColumnarRecords<'pipeline>>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    variable_scalar_expression: &'pipeline VariableScalarExpression,
) -> ResolvedScalarValue<'pipeline, 'a> {
    let variable_name = variable_scalar_expression.get_name().get_value();

    if let Some(variable) = execution_context
        .get_variables()
        .get_global_or_local_variable(variable_name)
    {
        execution_context.add_diagnostic_if_enabled(
            ColumnarEngineDiagnosticLevel::Verbose,
            variable_scalar_expression,
            || format!("Resolved variable with name '{variable_name}'"),
        );

        select(
            execution_context,
            ResolvedScalarValue::Dictionary(variable.clone()),
            variable_scalar_expression
                .get_value_accessor()
                .get_selectors(),
        )
    } else {
        execution_context.add_diagnostic_if_enabled(
            ColumnarEngineDiagnosticLevel::Warn,
            variable_scalar_expression,
            || format!("Variable matching name '{variable_name}' could not be found"),
        );
        ResolvedScalarValue::new_null()
    }
}

#[cfg(test)]
mod tests {
    use ahash::AHashMap;
    use arrow::datatypes::DataType;

    use crate::test_helpers::*;

    use super::*;

    fn build_var1_values() -> Dictionary<'static> {
        build_dictionary(
            vec![Some(0), Some(0), None, Some(1), Some(2), Some(3)],
            vec![
                ValueOrRef::String(StringValueOrRef::new_owned("hello world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodbye world".into())),
                ValueOrRef::Map(MapValueOrRef::from([(
                    "key1".into(),
                    ValueOrRef::Integer(18),
                )])),
                ValueOrRef::Integer(0),
            ],
        )
    }

    fn build_var2_values() -> Dictionary<'static> {
        build_dictionary(
            vec![Some(0), Some(0), None, Some(1)],
            vec![
                ValueOrRef::Array(ArrayValueOrRef::from([
                    ValueOrRef::Integer(0),
                    ValueOrRef::Integer(1),
                    ValueOrRef::Integer(2),
                ])),
                ValueOrRef::Integer(0),
            ],
        )
    }

    fn build_test_variables() -> AHashMap<Box<str>, Dictionary<'static>> {
        AHashMap::from([
            ("var1".into(), build_var1_values()),
            ("var2".into(), build_var2_values()),
        ])
    }

    #[test]
    fn test_select_from_variable_using_single_string() {
        let select_valid_var = VariableScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "var1"),
            ValueAccessor::new(),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_global_variables(build_test_variables()),
            ScalarExpression::Variable(select_valid_var),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(build_var1_values(), actual)
                }
                _ => panic!("test failure"),
            },
        );

        let select_invalid_var = VariableScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "invalid"),
            ValueAccessor::new(),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_global_variables(build_test_variables()),
            ScalarExpression::Variable(select_invalid_var),
            |r| {
                matches!(r, ResolvedScalarValue::Single(ValueOrRef::Null));
            },
        );

        let select_sub_key_valid = VariableScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "var1"),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::String(StringScalarExpression::new(
                    QueryLocation::new_fake(),
                    "key1",
                )),
            )]),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_global_variables(build_test_variables()),
            ScalarExpression::Variable(select_sub_key_valid),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![None, None, None, None, Some(0), None],
                            vec![ValueOrRef::Integer(18)]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let select_sub_key_invalid = VariableScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "var1"),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::String(StringScalarExpression::new(
                    QueryLocation::new_fake(),
                    "invalid",
                )),
            )]),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_global_variables(build_test_variables()),
            ScalarExpression::Variable(select_sub_key_invalid),
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
    fn test_select_from_variable_using_single_integer() {
        let select_sub_index = VariableScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "var2"),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::Integer(IntegerScalarExpression::new(
                    QueryLocation::new_fake(),
                    0,
                )),
            )]),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_global_variables(build_test_variables()),
            ScalarExpression::Variable(select_sub_index),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![Some(0), Some(0), None, None],
                            vec![ValueOrRef::Integer(0)]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let select_sub_index_negative = VariableScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "var2"),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::Integer(IntegerScalarExpression::new(
                    QueryLocation::new_fake(),
                    -1,
                )),
            )]),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_global_variables(build_test_variables()),
            ScalarExpression::Variable(select_sub_index_negative),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![Some(0), Some(0), None, None],
                            vec![ValueOrRef::Integer(2)]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let select_sub_index_invalid = VariableScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "var2"),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::Integer(IntegerScalarExpression::new(
                    QueryLocation::new_fake(),
                    100,
                )),
            )]),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_global_variables(build_test_variables()),
            ScalarExpression::Variable(select_sub_index_invalid),
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
}
