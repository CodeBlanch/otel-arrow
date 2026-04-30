// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{execution_context::ExecutionContext, *};

pub fn execute_set_transform_expression<'a, 'pipeline, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    set_transform_expression: &'pipeline SetTransformExpression,
) -> Result<(), ExpressionError> {
    todo!()
}
