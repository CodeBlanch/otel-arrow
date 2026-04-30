// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, selection::select_from_record_table, *,
};

pub fn execute_attached_scalar_expression<'a, 'pipeline, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    attached_scalar_expression: &'pipeline AttachedScalarExpression,
) -> ResolvedScalarValue<'c>
where
    'a: 'c,
    'pipeline: 'c,
{
    let record = match execution_context.get_records() {
        Some(r) => r,
        None => {
            execution_context.add_diagnostic_if_enabled(
                ColumnarEngineDiagnosticLevel::Warn,
                attached_scalar_expression,
                || "Attached data could not be found".into(),
            );
            return ResolvedScalarValue::new_null();
        }
    };

    let name = attached_scalar_expression.get_name().get_value();

    let attached_record = match record.get_attached_records(name) {
        Some(a) => a,
        None => {
            execution_context.add_diagnostic_if_enabled(
                ColumnarEngineDiagnosticLevel::Warn,
                attached_scalar_expression,
                || format!("Attached record matching name '{name}' could not be found"),
            );
            return ResolvedScalarValue::new_null();
        }
    };

    let key_data_type = record.get_key_data_type();

    select_from_record_table(
        execution_context,
        attached_scalar_expression,
        key_data_type,
        attached_record,
        attached_scalar_expression
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
    fn test_select_from_attached_table() {
        let values_dictionary = build_indexset_dictionary(
            vec![Some(0), Some(0)],
            vec![ValueOrRef::String(StringValueOrRef::new_owned(
                "hello world".into(),
            ))],
        );

        let select_valid_attached_data = AttachedScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "data"),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::String(StringScalarExpression::new(
                    QueryLocation::new_fake(),
                    "values",
                )),
            )]),
        );

        run_scalar_expression_test(
            TestRecords::with_attached_records(
                HashMap::new(),
                HashMap::from([(
                    "data".into(),
                    TestRecords::new(HashMap::from([(
                        "values".into(),
                        values_dictionary.clone(),
                    )])),
                )]),
            ),
            ScalarExpression::Attached(select_valid_attached_data),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => assert_eq!(values_dictionary, actual),
                _ => panic!("test failure"),
            },
        );

        let select_invalid_attached_data = AttachedScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "invalid"),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::String(StringScalarExpression::new(
                    QueryLocation::new_fake(),
                    "unknown",
                )),
            )]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::new()),
            ScalarExpression::Attached(select_invalid_attached_data),
            |r| {
                matches!(r, ResolvedScalarValue::Single(ValueOrRef::Null));
            },
        );

        let select_root = AttachedScalarExpression::new(
            QueryLocation::new_fake(),
            StringScalarExpression::new(QueryLocation::new_fake(), "data"),
            ValueAccessor::new(),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::new()),
            ScalarExpression::Attached(select_root),
            |r| {
                matches!(r, ResolvedScalarValue::Table(_));
            },
        );
    }
}
