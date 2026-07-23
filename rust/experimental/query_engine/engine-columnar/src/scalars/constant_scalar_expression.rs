// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{execution_context::ExecutionContext, resolved_value::*, selection::select, *};

pub fn execute_constant_scalar_expression<'a, 'pipeline, TRecords: ColumnarRecords<'pipeline>>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    constant_scalar_expression: &'pipeline ReferenceConstantScalarExpression,
) -> ResolvedScalarValue<'pipeline, 'a> {
    let constant_id = constant_scalar_expression.get_constant_id();

    let constant = execution_context
        .get_pipeline()
        .get_constant(constant_id)
        .unwrap_or_else(|| panic!("Constant for id '{constant_id}' was not found on pipeline"));

    if execution_context.is_diagnostic_level_enabled(ColumnarEngineDiagnosticLevel::Verbose) {
        let (line, column) = constant.get_query_location().get_line_and_column_numbers();
        execution_context.add_diagnostic(ColumnarEngineDiagnostic::new(
            ColumnarEngineDiagnosticLevel::Verbose,
            constant_scalar_expression,
            format!("Resolved '{}' constant with id '{constant_id}' defined on line {line} at column {column}", constant.get_value_type()),
        ));
    }

    select(
        execution_context,
        ResolvedScalarValue::Single(constant.to_value().into()),
        constant_scalar_expression
            .get_value_accessor()
            .get_selectors(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::test_helpers::*;

    use super::*;

    #[test]
    fn test_select_from_constants() {
        let pipeline = PipelineExpressionBuilder::new("")
            .with_constants(vec![
                StaticScalarExpression::Integer(IntegerScalarExpression::new(
                    QueryLocation::new_fake(),
                    18,
                )),
                StaticScalarExpression::Array(ArrayScalarExpression::new(
                    QueryLocation::new_fake(),
                    vec![StaticScalarExpression::Integer(
                        IntegerScalarExpression::new(QueryLocation::new_fake(), 99),
                    )],
                )),
                StaticScalarExpression::Map(MapScalarExpression::new(
                    QueryLocation::new_fake(),
                    HashMap::from([(
                        "key1".into(),
                        StaticScalarExpression::Integer(IntegerScalarExpression::new(
                            QueryLocation::new_fake(),
                            100,
                        )),
                    )]),
                )),
            ])
            .build()
            .expect("valid pipeline");

        let select_valid_constant = ReferenceConstantScalarExpression::new(
            QueryLocation::new_fake(),
            ValueType::Integer,
            0,
            ValueAccessor::new(),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_pipeline(&pipeline),
            ScalarExpression::Constant(select_valid_constant),
            |r| match r {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(ValueOrRef::Integer(18), actual)
                }
                _ => panic!("test failure"),
            },
        );

        let select_constant_with_int_path = ReferenceConstantScalarExpression::new(
            QueryLocation::new_fake(),
            ValueType::Integer,
            1,
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::Integer(IntegerScalarExpression::new(
                    QueryLocation::new_fake(),
                    0,
                )),
            )]),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_pipeline(&pipeline),
            ScalarExpression::Constant(select_constant_with_int_path),
            |r| match r {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(ValueOrRef::Integer(99), actual)
                }
                _ => panic!("test failure"),
            },
        );

        let select_constant_with_string_path = ReferenceConstantScalarExpression::new(
            QueryLocation::new_fake(),
            ValueType::Integer,
            2,
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::String(StringScalarExpression::new(
                    QueryLocation::new_fake(),
                    "key1",
                )),
            )]),
        );

        run_scalar_expression_test_with_state(
            ExecutionContextState::new().with_pipeline(&pipeline),
            ScalarExpression::Constant(select_constant_with_string_path),
            |r| match r {
                ResolvedScalarValue::Single(actual) => {
                    assert_eq!(ValueOrRef::Integer(100), actual)
                }
                _ => panic!("test failure"),
            },
        );
    }
}
