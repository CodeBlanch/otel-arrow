// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, scalars::execute_scalar_expression,
    slice::*, *,
};

pub fn execute_slice_scalar_expression<'a, 'pipeline, 'record, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    slice_scalar_expression: &'pipeline SliceScalarExpression,
) -> Result<ResolvedValue<'c>, ExpressionError>
where
    'a: 'c,
    'pipeline: 'c,
    'record: 'c,
{
    let inner_value =
        execute_scalar_expression(execution_context, slice_scalar_expression.get_source())?;

    let range_start_inclusive_expression = slice_scalar_expression.get_range_start_inclusive();
    let range_start_inclusive = match range_start_inclusive_expression {
        Some(start) => execute_scalar_expression(execution_context, start)?,
        None => ResolvedValue::Single(ResolvedSingleValue::Owned(OwnedValue::Integer(0))),
    };

    let range_end_exclusive_expression = slice_scalar_expression.get_range_end_exclusive();
    let range_end_exclusive = match range_end_exclusive_expression {
        Some(end) => execute_scalar_expression(execution_context, end)?,
        None => ResolvedValue::Single(ResolvedSingleValue::Owned(OwnedValue::Null)),
    };

    Ok(
        match (inner_value, range_start_inclusive, range_end_exclusive) {
            (
                ResolvedValue::Single(inner_value_single),
                ResolvedValue::Single(range_start_inclusive_single),
                ResolvedValue::Single(range_end_exclusive_single),
            ) => {
                match TryInto::<StringValueOrRef>::try_into(inner_value_single) {
                    Ok(string_value) => {
                        let range_start_inclusive = match range_start_inclusive_expression {
                            Some(start) => SliceScalarExpression::validate_resolved_range_value(
                                start.get_query_location(),
                                "start",
                                range_start_inclusive_single.to_value(),
                            )?,
                            None => 0,
                        };

                        let range_end_exclusive = match range_end_exclusive_expression {
                            Some(end) => {
                                Some(SliceScalarExpression::validate_resolved_range_value(
                                    end.get_query_location(),
                                    "end",
                                    range_end_exclusive_single.to_value(),
                                )?)
                            }
                            None => None,
                        };

                        let range_end_exclusive = SliceScalarExpression::validate_slice_range(
                            slice_scalar_expression.get_query_location(),
                            "String",
                            string_value.get_value().chars().count(),
                            range_start_inclusive,
                            range_end_exclusive,
                        )?;

                        ResolvedValue::Single(ResolvedSingleValue::Slice(Slice::String(
                            StringSlice::from_char_range(
                                string_value,
                                range_start_inclusive,
                                range_end_exclusive,
                            ),
                        )))
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
                        ResolvedValue::Single(ResolvedSingleValue::Owned(OwnedValue::Null))
                    }
                }
            }
            (inner_value, range_start_inclusive, range_end_exclusive) => {
                inner_value.map_into(|single| todo!(), |dictionary| todo!(), |table| todo!())?
            }
        },
    )
}
