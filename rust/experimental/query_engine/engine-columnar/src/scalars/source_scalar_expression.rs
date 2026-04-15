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

pub fn execute_source_scalar_expression<'a, 'pipeline, 'c, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    source_scalar_expression: &'pipeline SourceScalarExpression,
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
                source_scalar_expression,
                || "Source could not be found".into(),
            );
            return Ok(ResolvedScalarValue::new_null());
        }
    };

    let key_data_type = record.get_key_data_type();

    let mut current = ResolvedScalarValue::Table(record);

    for selector in source_scalar_expression
        .get_value_accessor()
        .get_selectors()
    {
        let next = execute_scalar_expression(execution_context, selector)?.map_into_with_state(
            current,
            |current, s| match s.to_value() {
                Value::String(single) => match current {
                    ResolvedScalarValue::Table(t) => Ok(t.get_values(single.get_value())),
                    ResolvedScalarValue::Dictionary(d) => {
                        Ok(Some(RecordTableValue::Dictionary(d.transform_into_any(|v| {
                            Ok(v.and_then(|v| {
                                if let ValueOrRef::Map(m) = v {
                                    match m {
                                        MapValueOrRef::Ref(m) => {
                                            m.get(single.get_value()).and_then(|v| {
                                                TryInto::<ValueOrRef>::try_into(v.to_value()).ok()
                                            })
                                        }
                                        MapValueOrRef::Owned(mut o) => {
                                            o.get_values_mut().remove(single.get_value())
                                        }
                                    }
                                } else {
                                    execution_context.add_diagnostic_if_enabled(
                                        ColumnarEngineDiagnosticLevel::Warn,
                                        source_scalar_expression,
                                        || format!("Could not search for map key '{}' specified in accessor expression because current node is a '{}' value", single.get_value(), v.get_value_type()),
                                    );
                                    None
                                }
                            }))
                        })?)))
                    }
                    ResolvedScalarValue::Single(_) => unreachable!("single should never be returned from a source selector"),
                },
                // todo: integer support for arrays
                v => {
                    execution_context.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Warn,
                        source_scalar_expression,
                        || format!("Unexpected scalar expression with '{}' value type encountered in accessor expression", v.get_value_type()),
                    );
                    Ok(None)
                }
            },
            |current, dictionary| {
                Ok(Some(match key_data_type {
                    DataType::UInt8 => select_using_dictionary::<UInt8Type>(&current, dictionary),
                    DataType::UInt16 => select_using_dictionary::<UInt16Type>(&current, dictionary),
                    DataType::UInt32 => select_using_dictionary::<UInt32Type>(&current, dictionary),
                    DataType::UInt64 => select_using_dictionary::<UInt64Type>(&current, dictionary),

                    DataType::Int8 => select_using_dictionary::<Int8Type>(&current, dictionary),
                    DataType::Int16 => select_using_dictionary::<Int16Type>(&current, dictionary),
                    DataType::Int32 => select_using_dictionary::<Int32Type>(&current, dictionary),
                    DataType::Int64 => select_using_dictionary::<Int64Type>(&current, dictionary),

                    _ => panic!("Key type is not supported"),
                }))
            },
            |_, _| {
                execution_context.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    source_scalar_expression,
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

fn select_using_dictionary<'a, K: ArrowDictionaryKeyType>(
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
        match selector
            .get_value(key_index)
            .as_ref()
            .map_or(Value::Null, |v| v.to_value())
        {
            Value::String(selector_value_string) => {
                let value = match source {
                    ResolvedScalarValue::Table(t) => {
                        if let Some(RecordTableValue::Dictionary(d)) =
                            t.get_values(selector_value_string.get_value())
                        {
                            d.get_value(key_index)
                        } else {
                            todo!()
                        }
                    }
                    // todo: Support single map (single_map[selector_value_string])
                    // todo: Support dictionary of maps (dictionary[key_index][selector_value_string])
                    _ => todo!(),
                };
                if let Some(v) = value {
                    let (index, _) = values.insert_full(v);
                    unsafe {
                        *key_builder.add(key_index) =
                            <K as ArrowPrimitiveType>::Native::from_usize(index).unwrap()
                    };
                } else {
                    push_null(&mut null_buffer, key_index, key_bit_length);
                }
            }
            // todo: support integer with arrays
            _ => {
                push_null(&mut null_buffer, key_index, key_bit_length);
                todo!()
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::test_helpers::*;

    use super::*;

    #[test]
    fn test_select_from_source_table_using_single_string() {
        let values_dictionary = build_indexset_dictionary(
            vec![Some(0), Some(0), None, Some(1), Some(2), Some(3)],
            vec![
                ValueOrRef::String(StringValueOrRef::new_owned("hello world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodbye world".into())),
                ValueOrRef::Map(MapValueOrRef::from([(
                    "key1".into(),
                    ValueOrRef::Integer(18),
                )])),
                ValueOrRef::Integer(0),
            ],
        );

        let select_valid_key = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::String(StringScalarExpression::new(
                    QueryLocation::new_fake(),
                    "values",
                )),
            )]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Source(select_valid_key),
            |r| match r.unwrap() {
                ResolvedScalarValue::Dictionary(actual) => assert_eq!(values_dictionary, actual),
                _ => panic!("test failure"),
            },
        );

        let select_invalid_key = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::String(StringScalarExpression::new(
                    QueryLocation::new_fake(),
                    "unknown",
                )),
            )]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::new()),
            ScalarExpression::Source(select_invalid_key),
            |r| {
                matches!(
                    r,
                    Ok(ResolvedScalarValue::Single(ResolvedSingleValue::Null))
                );
            },
        );

        let select_root =
            SourceScalarExpression::new(QueryLocation::new_fake(), ValueAccessor::new());

        run_scalar_expression_test(
            TestRecords::new(HashMap::new()),
            ScalarExpression::Source(select_root),
            |r| {
                matches!(r, Ok(ResolvedScalarValue::Table(_)));
            },
        );

        let select_sub_key = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "values"),
                )),
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "key1"),
                )),
            ]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Source(select_sub_key),
            |r| match r.unwrap() {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_indexset_dictionary(
                            vec![None, None, None, None, Some(0), None],
                            vec![ValueOrRef::Integer(18)]
                        ),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );
    }

    #[test]
    fn test_select_from_source_table_using_dictionary() {
        todo!()
    }
}
