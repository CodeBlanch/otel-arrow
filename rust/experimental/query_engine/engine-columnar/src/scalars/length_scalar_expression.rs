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
                    Ok(match v {
                        Some(ValueOrRef::String(s)) => {
                            Some(ValueOrRef::Integer(s.get_value().chars().count() as i64))
                        }
                        // todo: Map
                        // todo: Array
                        Some(v) => {
                            execution_context.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                length_scalar_expression,
                                || {
                                    format!(
                                        "Cannot calculate the length of '{}' input",
                                        v.to_value().get_value_type()
                                    )
                                },
                            );
                            None
                        }
                        _ => None,
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
        todo!()
    }

    #[test]
    fn test_length_dictionary() {
        todo!()
    }
}
