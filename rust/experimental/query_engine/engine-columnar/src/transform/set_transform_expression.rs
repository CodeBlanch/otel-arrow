// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{execution_context::ExecutionContext, scalars::execute_scalar_expression, *};

pub fn execute_set_transform_expression<'pipeline, TRecords: ColumnarRecords>(
    execution_context: &mut ExecutionContext<'_, 'pipeline, TRecords>,
    set_transform_expression: &'pipeline SetTransformExpression,
) {
    let _source =
        execute_scalar_expression(execution_context, set_transform_expression.get_source());

    /*match set_transform_expression.get_destination() {
        MutableValueExpression::Source(s) => {
            let selectors = s.get_value_accessor().get_selectors();

            let mut captured_selectors = match selectors.iter().size_hint() {
                (_, Some(l)) => Vec::with_capacity(l),
                _ => Vec::new(),
            };

            for selector in selectors {
                let selector_value = execute_scalar_expression(execution_context, selector);

                captured_selectors.push((selector, selector_value.into_owned()));
            }

            let root = match execution_context.take_records() {
                Some(r) => r,
                None => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        set_transform_expression,
                        || "Source could not be found".into(),
                    );
                    return;
                }
            };

            let mut current = ResolvedScalarValue::Table(&mut root);

            for (selector_expression, selector) in captured_selectors {
                let next = selector.map_into_with_state(
                    current,
                    |_current, s| match s {
                        ValueOrRef::String(key) => {
                            todo!()
                        }
                        ValueOrRef::Array(index) => {
                            todo!()
                        }
                        v => {
                            let value_type = v.get_value_type();
                            execution_context.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                selector_expression,
                                || format!("Unexpected scalar expression with '{value_type}' value type encountered in accessor expression"),
                            );
                            //None
                            todo!()
                        }
                    },
                    |current, dictionary| {
                        todo!()
                    },
                    |current, table| {
                        todo!()
                    });

                todo!()
            }

            execution_context.set_records(root);

            execution_context.add_diagnostic_if_enabled(
                ColumnarEngineDiagnosticLevel::Warn,
                set_transform_expression,
                || "Cannot set root map".into(),
            );
        }
        MutableValueExpression::Variable(_) => todo!(),
        MutableValueExpression::Argument(_) => todo!(),
    }*/
    todo!()
}

/*enum ResolvedScalarValue<'a> {
    Table(&'a mut dyn RecordTable),
}*/
