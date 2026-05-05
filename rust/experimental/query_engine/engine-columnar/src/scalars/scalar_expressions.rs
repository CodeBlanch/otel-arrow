// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{
    execution_context::ExecutionContext,
    resolved_value::*,
    scalars::{
        attached_scalar_expression::execute_attached_scalar_expression,
        length_scalar_expression::execute_length_scalar_expression,
        slice_scalar_expression::execute_slice_scalar_expression,
        source_scalar_expression::execute_source_scalar_expression,
    },
    *,
};

pub fn execute_scalar_expression<'a, 'pipeline, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    scalar_expression: &'pipeline ScalarExpression,
) -> ResolvedScalarValue<'pipeline, 'a> {
    let value = match scalar_expression {
        ScalarExpression::Argument(_) => todo!(),
        ScalarExpression::Attached(a) => execute_attached_scalar_expression(execution_context, a),
        ScalarExpression::Case(_) => todo!(),
        ScalarExpression::Coalesce(_) => todo!(),
        ScalarExpression::Collection(_) => todo!(),
        ScalarExpression::Conditional(_) => todo!(),
        ScalarExpression::Constant(_) => todo!(),
        ScalarExpression::Convert(_) => todo!(),
        ScalarExpression::GetType(_) => todo!(),
        ScalarExpression::InvokeFunction(_) => todo!(),
        ScalarExpression::Length(l) => execute_length_scalar_expression(execution_context, l),
        ScalarExpression::Logical(_) => todo!(),
        ScalarExpression::Math(_) => todo!(),
        ScalarExpression::Parse(_) => todo!(),
        ScalarExpression::Select(_) => todo!(),
        ScalarExpression::Slice(s) => execute_slice_scalar_expression(execution_context, s),
        ScalarExpression::Source(s) => execute_source_scalar_expression(execution_context, s),
        ScalarExpression::Static(s) => ResolvedScalarValue::new_from_value(s.to_value()),
        ScalarExpression::Temporal(_) => todo!(),
        ScalarExpression::Text(_) => todo!(),
        ScalarExpression::Variable(_) => todo!(),
    };

    execution_context.add_diagnostic_if_enabled(
        ColumnarEngineDiagnosticLevel::Verbose,
        scalar_expression,
        || format!("Evaluated as: {value}"),
    );

    value
}
