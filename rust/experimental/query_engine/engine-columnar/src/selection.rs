// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use ahash::RandomState;
use arrow::{array::*, buffer::MutableBuffer, datatypes::*};
use data_engine_expressions::*;
use indexmap::IndexSet;

use crate::{
    dictionary_transform::push_null, execution_context::ExecutionContext, resolved_value::*,
    scalars::execute_scalar_expression, *,
};

pub(crate) fn select_from_record_table<'a, 'pipeline, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    expression: &'pipeline dyn Expression,
    key_data_type: DataType,
    root: &'a dyn RecordTable,
    selectors: &'pipeline [ScalarExpression],
) -> Result<ResolvedScalarValue<'c>, ExpressionError>
where
    'a: 'c,
    'pipeline: 'c,
{
    let mut current = ResolvedScalarValue::Table(root);

    for selector in selectors {
        let next = execute_scalar_expression(execution_context, selector)?.map_into_with_state(
            current,
            |current, s| match s {
                ValueOrRef::String(key) => match current {
                    ResolvedScalarValue::Table(t) => Ok(t.get_values(key.get_value())),
                    ResolvedScalarValue::Dictionary(d) => {
                        Ok(Some(RecordTableValue::Dictionary(d.transform_into_any(|v| {
                            if let ValueOrRef::Map(m) = v {
                                match m {
                                    MapValueOrRef::Ref(m) => m.get(key.get_value()).map_or(ValueOrRef::Null, |v| v.to_value().into()),
                                    MapValueOrRef::Owned(m) => m.get_values().get(key.get_value()).map_or(ValueOrRef::Null, |v| v.clone())
                                }
                            } else {
                                execution_context.add_diagnostic_if_enabled(
                                    ColumnarEngineDiagnosticLevel::Warn,
                                    expression,
                                    || format!("Could not search for map key '{}' specified in accessor expression because current node is a '{}' value", key.get_value(), v.get_value_type()),
                                );
                                ValueOrRef::Null
                            }
                        }))))
                    }
                    ResolvedScalarValue::Single(_) => unreachable!("single should never be returned from a source selector"),
                },
                ValueOrRef::Integer(index) => match current {
                    ResolvedScalarValue::Dictionary(d) => {
                        Ok(Some(RecordTableValue::Dictionary(d.transform_into_any(|v| {
                            if let ValueOrRef::Array(a) = v {
                                let len = a.len();
                                let mut index = index.get_value();
                                if index < 0 {
                                    index += len as i64;
                                }
                                if index < 0 || index >= len as i64 {
                                    execution_context.add_diagnostic_if_enabled(
                                        ColumnarEngineDiagnosticLevel::Warn,
                                        expression,
                                        || format!("Array index '{index}' specified in accessor expression is invalid"),
                                    );
                                    ValueOrRef::Null
                                } else {
                                    a.get(index as usize)
                                }
                            } else {
                                execution_context.add_diagnostic_if_enabled(
                                    ColumnarEngineDiagnosticLevel::Warn,
                                    expression,
                                    || format!("Could not search for array index '{}' specified in accessor expression because current node is a '{}' value", index.get_value(), v.get_value_type()),
                                );
                                ValueOrRef::Null
                            }
                        }))))
                    }
                    ResolvedScalarValue::Single(_) => unreachable!("single should never be returned from a source selector"),
                    ResolvedScalarValue::Table(_) => {
                        execution_context.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Warn,
                            expression,
                            || format!("Could not search for array index '{}' specified in accessor expression because current node is a 'Map' value", index.get_value()),
                        );
                        Ok(None)
                    }
                }
                v => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        expression,
                        || format!("Unexpected scalar expression with '{}' value type encountered in accessor expression", v.get_value_type()),
                    );
                    Ok(None)
                }
            },
            |current, dictionary| {
                Ok(Some(match key_data_type {
                    DataType::UInt8 => select_using_dictionary::<UInt8Type, TRecords>(execution_context, expression, &current, dictionary),
                    DataType::UInt16 => select_using_dictionary::<UInt16Type, TRecords>(execution_context, expression, &current, dictionary),
                    DataType::UInt32 => select_using_dictionary::<UInt32Type, TRecords>(execution_context, expression, &current, dictionary),
                    DataType::UInt64 => select_using_dictionary::<UInt64Type, TRecords>(execution_context, expression, &current, dictionary),

                    DataType::Int8 => select_using_dictionary::<Int8Type, TRecords>(execution_context, expression, &current, dictionary),
                    DataType::Int16 => select_using_dictionary::<Int16Type, TRecords>(execution_context, expression, &current, dictionary),
                    DataType::Int32 => select_using_dictionary::<Int32Type, TRecords>(execution_context, expression, &current, dictionary),
                    DataType::Int64 => select_using_dictionary::<Int64Type, TRecords>(execution_context, expression, &current, dictionary),

                    _ => panic!("Key type is not supported"),
                }))
            },
            |_, _| {
                execution_context.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    expression,
                    || "Unexpected scalar expression with Map value type encountered in accessor expression".into(),
                );
                Ok(None)
            }
        )?;

        match next {
            None => {
                current = ResolvedScalarValue::new_null();
                break;
            }
            Some(v) => match v {
                RecordTableValue::Table(t) => current = ResolvedScalarValue::Table(t),
                RecordTableValue::Dictionary(d) => current = ResolvedScalarValue::Dictionary(d),
            },
        }
    }

    Ok(current)
}

fn select_using_dictionary<'a, 'pipeline, K: ArrowDictionaryKeyType, TRecords: ColumnarRecords>(
    execution_context: &ExecutionContext<'a, 'pipeline, TRecords>,
    expression: &'pipeline dyn Expression,
    source: &ResolvedScalarValue<'a>,
    selector: Dictionary<'a>,
) -> RecordTableValue<'a> {
    let key_count = selector.len();

    let mut key_buffer = MutableBuffer::from_len_zeroed(size_of::<K::Native>() * key_count);
    let key_builder = key_buffer.typed_data_mut::<K::Native>().as_mut_ptr();

    let key_bit_length = arrow::util::bit_util::ceil(key_count, 8);
    let mut null_buffer = None;

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
                                    expression,
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
                    push_null(&mut null_buffer, key_index, key_bit_length);
                } else {
                    let (index, _) = values.insert_full(value);
                    unsafe {
                        *key_builder.add(key_index) =
                            <K as ArrowPrimitiveType>::Native::from_usize(index).unwrap()
                    };
                }
            }
            Value::Integer(index) => {
                let value = match source {
                    ResolvedScalarValue::Table(_) => {
                        execution_context.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Warn,
                            expression,
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
                                        expression,
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
                                    expression,
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
                    push_null(&mut null_buffer, key_index, key_bit_length);
                } else {
                    let (index, _) = values.insert_full(value);
                    unsafe {
                        *key_builder.add(key_index) =
                            <K as ArrowPrimitiveType>::Native::from_usize(index).unwrap()
                    };
                }
            }
            v => {
                execution_context.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    expression,
                    || format!("Unexpected scalar expression with '{}' value type encountered in accessor expression", v.get_value_type()),
                );
                push_null(&mut null_buffer, key_index, key_bit_length);
            }
        }
    }

    RecordTableValue::Dictionary(Dictionary::new(
        PrimitiveArray::<K>::new(
            key_buffer.into(),
            null_buffer.and_then(|v| NullBufferBuilder::new_from_buffer(v, key_count).finish()),
        )
        .into(),
        values.into(),
    ))
}
