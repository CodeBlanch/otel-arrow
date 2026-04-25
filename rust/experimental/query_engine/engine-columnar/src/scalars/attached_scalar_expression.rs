// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, selection::select_from_record_table, *,
};

pub fn execute_attached_scalar_expression<'a, 'pipeline, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    attached_scalar_expression: &'pipeline AttachedScalarExpression,
) -> Result<ResolvedScalarValue<'c>, ExpressionError>
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
            return Ok(ResolvedScalarValue::new_null());
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
            return Ok(ResolvedScalarValue::new_null());
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
