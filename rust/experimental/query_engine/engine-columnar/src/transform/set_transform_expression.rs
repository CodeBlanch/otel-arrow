// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use arrow::{array::RecordBatch, datatypes::DataType};
use data_engine_expressions::*;

use crate::{execution_context::ExecutionContext, scalars::execute_scalar_expression, *};

pub enum SingleOrDictionaryValue<'a> {
    Single(ValueOrRef<'a>),
    Dictionary(Dictionary<'a>)
}

impl<'a> SingleOrDictionaryValue<'a> {
    pub fn into_dictionary(
        self,
        key_data_type: DataType,
        key_count: usize,
    ) -> Dictionary<'a> {
        match self {
            SingleOrDictionaryValue::Single(v) => Dictionary::new_scalar_with_data_type(key_data_type, key_count, v),
            SingleOrDictionaryValue::Dictionary(d) => d,
        }
    }
}

pub fn execute_set_transform_expression<
    'pipeline,
    TRecords: ColumnarRecords,
>(
    execution_context: &ExecutionContext<'_, 'pipeline, TRecords>,
    set_transform_expression: &'pipeline SetTransformExpression,
    value: SingleOrDictionaryValue<'pipeline>
) -> Result<(), ()> {
    match set_transform_expression.get_destination() {
        MutableValueExpression::Source(s) => {
            if execution_context.get_records().is_none() {
                execution_context.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    set_transform_expression,
                    || "Source could not be found".into(),
                );
                return Err(());
            }

            let selectors = s.get_value_accessor().get_selectors();

            let mut path = match selectors.iter().size_hint() {
                (_, Some(len)) => Vec::with_capacity(len),
                _ => Vec::new(),
            };

            for selector in selectors {
                let ret = execute_scalar_expression(execution_context, selector).map_into(
                    |single| match single {
                        ValueOrRef::String(key) => {
                            path.push(SelectionPath::Key(key));
                            Ok(())
                        }
                        ValueOrRef::Array(index) => {
                            path.push(SelectionPath::Index(index));
                            Ok(())
                        }
                        v => {
                            execution_context.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                selector,
                                || format!("Unexpected scalar expression with '{}' value type encountered in accessor expression", v.get_value_type()),
                            );
                            Err(())
                        }
                    },
                    |dictionary| {
                        todo!()
                    },
                    |table| {
                        todo!()
                    });

                if ret.is_err() {
                    return Err(());
                }
            }

            /*match factory.set(&mut root, &path, value.into_dictionary(count, data_type)) {
                Ok(v) => Ok(v),
                Err(e) => {
                    // todo: Log
                    execution_context.set_records(root);
                    Err(())
                }
            }*/

            todo!()

            /*execution_context.add_diagnostic_if_enabled(
                ColumnarEngineDiagnosticLevel::Warn,
                set_transform_expression,
                || "Cannot set root map".into(),
            );*/
        }
        MutableValueExpression::Variable(_) => todo!(),
        MutableValueExpression::Argument(_) => todo!(),
    }
}
