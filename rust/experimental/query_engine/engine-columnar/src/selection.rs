// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use ahash::RandomState;
use arrow::datatypes::*;
use data_engine_expressions::*;
use indexmap::IndexSet;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, scalars::execute_scalar_expression, *,
};

pub fn select_from_record_table<'a, 'pipeline, TRecords: ColumnarRecords>(
    execution_context: &ExecutionContext<'a, 'pipeline, TRecords>,
    key_data_type: DataType,
    root: &'a dyn RecordTable,
    selectors: &'pipeline [ScalarExpression],
) -> ResolvedScalarValue<'pipeline, 'a> {
    let mut current = ResolvedScalarValue::Table(root);

    for selector in selectors {
        let next = execute_scalar_expression(execution_context, selector).map_into_with_state(
            current,
            |current, s| match s {
                ValueOrRef::String(key) => match current {
                    ResolvedScalarValue::Table(t) => match t.get_values(key.get_value()) {
                        Some(RecordTableValue::Table(t)) => Some(ResolvedScalarValue::Table(t)),
                        Some(RecordTableValue::Dictionary(d)) => Some(ResolvedScalarValue::Dictionary(d.into())),
                        None => None,
                    }
                    ResolvedScalarValue::Dictionary(d) => {
                        Some(ResolvedScalarValue::Dictionary(d.transform_into_any(|v| {
                            if let ValueOrRef::Map(m) = v {
                                match m {
                                    MapValueOrRef::Ref(m) => m.get(key.get_value()).map_or(ValueOrRef::Null, |v| v.to_value().into()),
                                    MapValueOrRef::Owned(m) => m.get_values().get(key.get_value()).map_or(ValueOrRef::Null, |v| v.clone())
                                }
                            } else {
                                execution_context.add_diagnostic_if_enabled(
                                    ColumnarEngineDiagnosticLevel::Warn,
                                    selector,
                                    || format!("Could not search for map key '{}' specified in accessor expression because current node is a '{}' value", key.get_value(), v.get_value_type()),
                                );
                                ValueOrRef::Null
                            }
                        })))
                    }
                    ResolvedScalarValue::Single(_) => unreachable!("single should never be returned from a source selector"),
                },
                ValueOrRef::Integer(index) => match current {
                    ResolvedScalarValue::Dictionary(d) => {
                        Some(ResolvedScalarValue::Dictionary(d.transform_into_any(|v| {
                            if let ValueOrRef::Array(a) = v {
                                let len = a.len();
                                let mut index = index.get_value();
                                if index < 0 {
                                    index += len as i64;
                                }
                                if index < 0 || index >= len as i64 {
                                    execution_context.add_diagnostic_if_enabled(
                                        ColumnarEngineDiagnosticLevel::Warn,
                                        selector,
                                        || format!("Array index '{index}' specified in accessor expression is invalid"),
                                    );
                                    ValueOrRef::Null
                                } else {
                                    a.get(index as usize)
                                }
                            } else {
                                execution_context.add_diagnostic_if_enabled(
                                    ColumnarEngineDiagnosticLevel::Warn,
                                    selector,
                                    || format!("Could not search for array index '{}' specified in accessor expression because current node is a '{}' value", index.get_value(), v.get_value_type()),
                                );
                                ValueOrRef::Null
                            }
                        })))
                    }
                    ResolvedScalarValue::Single(_) => unreachable!("single should never be returned from a source selector"),
                    ResolvedScalarValue::Table(_) => {
                        execution_context.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Warn,
                            selector,
                            || format!("Could not search for array index '{}' specified in accessor expression because current node is a 'Map' value", index.get_value()),
                        );
                        None
                    }
                }
                v => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        selector,
                        || format!("Unexpected scalar expression with '{}' value type encountered in accessor expression", v.get_value_type()),
                    );
                    None
                }
            },
            |current, dictionary| {
                Some(match key_data_type {
                    DataType::UInt8 => select_using_dictionary::<UInt8Type, TRecords>(execution_context, &current, selector, dictionary),
                    DataType::UInt16 => select_using_dictionary::<UInt16Type, TRecords>(execution_context, &current, selector, dictionary),
                    DataType::UInt32 => select_using_dictionary::<UInt32Type, TRecords>(execution_context, &current, selector, dictionary),
                    DataType::UInt64 => select_using_dictionary::<UInt64Type, TRecords>(execution_context, &current, selector, dictionary),

                    DataType::Int8 => select_using_dictionary::<Int8Type, TRecords>(execution_context, &current, selector, dictionary),
                    DataType::Int16 => select_using_dictionary::<Int16Type, TRecords>(execution_context, &current, selector, dictionary),
                    DataType::Int32 => select_using_dictionary::<Int32Type, TRecords>(execution_context, &current, selector, dictionary),
                    DataType::Int64 => select_using_dictionary::<Int64Type, TRecords>(execution_context, &current, selector, dictionary),

                    _ => panic!("Key type is not supported"),
                })
            },
            |_, _| {
                execution_context.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    selector,
                    || "Unexpected scalar expression with Map value type encountered in accessor expression".into(),
                );
                None
            }
        );

        match next {
            None => {
                current = ResolvedScalarValue::new_null();
                break;
            }
            Some(v) => current = v,
        }
    }

    current
}

pub fn capture_selector_values<'pipeline, TRecords: ColumnarRecords>(
    execution_context: &ExecutionContext<'_, 'pipeline, TRecords>,
    selectors: &'pipeline [ScalarExpression],
) -> Result<Vec<SelectionPath<'pipeline>>, ()> {
    let mut path = match selectors.iter().size_hint() {
        (_, Some(len)) => Vec::with_capacity(len),
        _ => Vec::new(),
    };

    for selector in selectors {
        let ret = execute_scalar_expression(execution_context, selector).map_into_with_state(
            &mut path,
            |path, single| match single {
                ValueOrRef::String(key) => {
                    path.push(SelectionPath::Key{ expression: selector, value: key });
                    Ok(())
                }
                ValueOrRef::Array(index) => {
                    path.push(SelectionPath::Index{ expression: selector, value: index });
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
            |path, dictionary| {
                path.push(SelectionPath::Dictionary{ expression: selector, value: dictionary });
                Ok(())
            },
            |_, _| {
                execution_context.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    selector,
                    || "Unexpected scalar expression with Map value type encountered in accessor expression".into(),
                );
                Err(())
            });

        if ret.is_err() {
            return Err(());
        }
    }

    Ok(path)
}

fn select_using_dictionary<'a, 'pipeline, K: ArrowDictionaryKeyType, TRecords: ColumnarRecords>(
    execution_context: &ExecutionContext<'a, 'pipeline, TRecords>,
    source: &ResolvedScalarValue<'pipeline, 'a>,
    selector_expression: &'pipeline dyn Expression,
    selector: Dictionary<'pipeline>,
) -> ResolvedScalarValue<'pipeline, 'a> {
    let key_count = selector.len();

    let mut key_builder = DictionaryKeyArrayBuilder::<K>::new(key_count);
    let mut key_writer = key_builder.get_writer();

    let mut values = IndexSet::with_hasher(RandomState::new());

    for key_index in 0..key_count {
        match selector.get_value(key_index).to_value() {
            Value::String(key) => {
                let value = match source {
                    ResolvedScalarValue::Table(t) => match t.get_values(key.get_value()) {
                        Some(RecordTableValue::Dictionary(d)) => d.get_value(key_index),
                        Some(RecordTableValue::Table(_)) => {
                            todo!("table returning a table for a key is not currently supported")
                        }
                        None => ValueOrRef::Null,
                    },
                    ResolvedScalarValue::Dictionary(d) => match d.get_value(key_index) {
                        ValueOrRef::Map(m) => match m {
                            MapValueOrRef::Ref(m) => m
                                .get(key.get_value())
                                .map_or(ValueOrRef::Null, |v| v.to_value().into()),
                            MapValueOrRef::Owned(m) => m
                                .get_values()
                                .get(key.get_value())
                                .map_or(ValueOrRef::Null, |v| v.clone()),
                        },
                        v => {
                            execution_context.add_diagnostic_if_enabled(
                                    ColumnarEngineDiagnosticLevel::Warn,
                                    selector_expression,
                                    || format!("Could not search for map key '{}' specified in accessor expression because current node is a '{}' value", key.get_value(), v.get_value_type()),
                                );
                            ValueOrRef::Null
                        }
                    },
                    ResolvedScalarValue::Single(_) => {
                        unreachable!("single should never be returned from a source selector")
                    }
                };
                if let ValueOrRef::Null = value {
                    unsafe { key_writer.set_null(key_index) }
                } else {
                    let (index, _) = values.insert_full(value);
                    unsafe { key_writer.set_value_index(key_index, index) };
                }
            }
            Value::Integer(index) => {
                let value = match source {
                    ResolvedScalarValue::Table(_) => {
                        execution_context.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Warn,
                            selector_expression,
                            || format!("Could not search for array index '{}' specified in accessor expression because current node is a 'Map' value", index.get_value()),
                        );
                        ValueOrRef::Null
                    }
                    ResolvedScalarValue::Dictionary(d) => match d.get_value(key_index) {
                        ValueOrRef::Array(a) => {
                            let len = a.len();
                            let mut index = index.get_value();
                            if index < 0 {
                                index += len as i64;
                            }
                            if index < 0 || index >= len as i64 {
                                execution_context.add_diagnostic_if_enabled(
                                        ColumnarEngineDiagnosticLevel::Warn,
                                        selector_expression,
                                        || format!("Array index '{index}' specified in accessor expression is invalid"),
                                    );
                                ValueOrRef::Null
                            } else {
                                a.get(index as usize)
                            }
                        }
                        v => {
                            execution_context.add_diagnostic_if_enabled(
                                    ColumnarEngineDiagnosticLevel::Warn,
                                    selector_expression,
                                    || format!("Could not search for array index '{}' specified in accessor expression because current node is a '{}' value", index.get_value(), v.get_value_type()),
                                );
                            ValueOrRef::Null
                        }
                    },
                    ResolvedScalarValue::Single(_) => {
                        unreachable!("single should never be returned from a source selector")
                    }
                };
                if let ValueOrRef::Null = value {
                    unsafe { key_writer.set_null(key_index) };
                } else {
                    let (index, _) = values.insert_full(value);
                    unsafe { key_writer.set_value_index(key_index, index) };
                }
            }
            v => {
                execution_context.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    selector_expression,
                    || format!("Unexpected scalar expression with '{}' value type encountered in accessor expression", v.get_value_type()),
                );
                unsafe { key_writer.set_null(key_index) };
            }
        }
    }

    ResolvedScalarValue::Dictionary(Dictionary::new(key_builder.finish().into(), values.into()))
}
