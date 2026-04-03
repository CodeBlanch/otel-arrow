// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, scalars::execute_scalar_expression,
};

pub fn execute_length_scalar_expression<'a, 'pipeline, 'record, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    length_scalar_expression: &'pipeline LengthScalarExpression,
) -> Result<ResolvedValue<'c>, ExpressionError>
where
    'a: 'c,
    'pipeline: 'c,
    'record: 'c,
{
    let inner_value = execute_scalar_expression(
        execution_context,
        length_scalar_expression.get_inner_expression(),
    )?;

    inner_value.map_into(
        |single| {
            Ok(match single.to_value() {
                Value::String(s) => ResolvedValue::Single(ResolvedSingleValue::Owned(
                    OwnedValue::Integer(s.get_value().chars().count() as i64),
                )),
                Value::Array(a) => ResolvedValue::Single(ResolvedSingleValue::Owned(
                    OwnedValue::Integer(a.len() as i64),
                )),
                Value::Map(m) => ResolvedValue::Single(ResolvedSingleValue::Owned(
                    OwnedValue::Integer(m.len() as i64),
                )),
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
                    ResolvedValue::Single(ResolvedSingleValue::Owned(OwnedValue::Null))
                }
            })
        },
        |dictionary| {
            Ok(ResolvedValue::Dictionary(
                dictionary.into_len_dictionary(
                    &execution_context
                        .create_diagnostic_receiver_for_expression(length_scalar_expression),
                )?,
            ))
        },
        |_| todo!(),
    )
}
