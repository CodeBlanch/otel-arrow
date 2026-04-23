// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::cell::OnceCell;

use arrow::{
    array::*,
    buffer::{BooleanBuffer, MutableBuffer, NullBuffer},
    datatypes::*,
};
use data_engine_expressions::*;

use crate::*;

impl<'a> Dictionary<'a> {
    pub(crate) fn transform_into_boolean<FTransform>(
        self,
        mut transform: FTransform,
    ) -> Result<BooleanArray, ExpressionError>
    where
        FTransform: FnMut(ValueOrRef<'_>) -> Result<Option<bool>, ExpressionError>,
    {
        let (keys, values) = self.into_parts();

        match keys.values() {
            DictionaryKeyArrayValues::Array(key_array) => match key_array.data_type() {
                DataType::Int8 => {
                    transform_boolean_typed(key_array.as_primitive::<Int8Type>(), values, transform)
                }
                DataType::Int16 => transform_boolean_typed(
                    key_array.as_primitive::<Int16Type>(),
                    values,
                    transform,
                ),
                DataType::Int32 => transform_boolean_typed(
                    key_array.as_primitive::<Int32Type>(),
                    values,
                    transform,
                ),
                DataType::Int64 => transform_boolean_typed(
                    key_array.as_primitive::<Int64Type>(),
                    values,
                    transform,
                ),

                DataType::UInt8 => transform_boolean_typed(
                    key_array.as_primitive::<UInt8Type>(),
                    values,
                    transform,
                ),
                DataType::UInt16 => transform_boolean_typed(
                    key_array.as_primitive::<UInt16Type>(),
                    values,
                    transform,
                ),
                DataType::UInt32 => transform_boolean_typed(
                    key_array.as_primitive::<UInt32Type>(),
                    values,
                    transform,
                ),
                DataType::UInt64 => transform_boolean_typed(
                    key_array.as_primitive::<UInt64Type>(),
                    values,
                    transform,
                ),

                d => panic!("Unexpected dictionary key type '{d}' encountered"),
            },
            DictionaryKeyArrayValues::UniqueValues {
                data_type: _,
                length,
            } => {
                let key_bit_length = arrow::util::bit_util::ceil(length, 8);

                let mut key_buffer = MutableBuffer::from_len_zeroed(key_bit_length);
                let key_builder = key_buffer.typed_data_mut::<u8>().as_mut_ptr();

                let mut null_buffer = None;

                let transformered_values = values.transform_into_vec(&mut transform)?;

                assert!(transformered_values.len() == length);

                for key_index in 0..length {
                    if let Some(v) = unsafe { transformered_values.get_unchecked(key_index) } {
                        if *v {
                            unsafe { arrow::util::bit_util::set_bit_raw(key_builder, key_index) };
                        }
                    } else {
                        push_null(&mut null_buffer, key_index, key_bit_length);
                    }
                }

                Ok(BooleanArray::new(
                    BooleanBufferBuilder::new_from_buffer(key_buffer, length).finish(),
                    null_buffer
                        .and_then(|v| NullBufferBuilder::new_from_buffer(v, length).finish()),
                ))
            }
            DictionaryKeyArrayValues::SingleValue {
                data_type: _,
                length,
                value_index,
            } => {
                let (key_buffer, null_buffer) = match value_index {
                    None => (
                        BooleanBuffer::new_unset(length),
                        Some(NullBuffer::new_null(length)),
                    ),
                    Some(value_index) => match transform(values.get_value_at(value_index))? {
                        None => (
                            BooleanBuffer::new_unset(length),
                            Some(NullBuffer::new_null(length)),
                        ),
                        Some(v) => (
                            if v {
                                BooleanBuffer::new_set(length)
                            } else {
                                BooleanBuffer::new_unset(length)
                            },
                            None,
                        ),
                    },
                };

                Ok(BooleanArray::new(key_buffer, null_buffer))
            }
        }
    }

    pub(crate) fn transform_into_any<FTransform>(
        self,
        mut transform: FTransform,
    ) -> Result<Dictionary<'a>, ExpressionError>
    where
        FTransform: FnMut(ValueOrRef<'a>) -> Result<ValueOrRef<'a>, ExpressionError>,
    {
        let (keys, values) = self.into_parts();

        match keys.values() {
            DictionaryKeyArrayValues::Array(key_array) => match key_array.data_type() {
                DataType::Int8 => {
                    transform_any_typed(key_array.as_primitive::<Int8Type>(), values, transform)
                }
                DataType::Int16 => {
                    transform_any_typed(key_array.as_primitive::<Int16Type>(), values, transform)
                }
                DataType::Int32 => {
                    transform_any_typed(key_array.as_primitive::<Int32Type>(), values, transform)
                }
                DataType::Int64 => {
                    transform_any_typed(key_array.as_primitive::<Int64Type>(), values, transform)
                }

                DataType::UInt8 => {
                    transform_any_typed(key_array.as_primitive::<UInt8Type>(), values, transform)
                }
                DataType::UInt16 => {
                    transform_any_typed(key_array.as_primitive::<UInt16Type>(), values, transform)
                }
                DataType::UInt32 => {
                    transform_any_typed(key_array.as_primitive::<UInt32Type>(), values, transform)
                }
                DataType::UInt64 => {
                    transform_any_typed(key_array.as_primitive::<UInt64Type>(), values, transform)
                }

                d => panic!("Unexpected dictionary key type '{d}' encountered"),
            },
            DictionaryKeyArrayValues::UniqueValues { data_type, length } => match data_type {
                DataType::Int8 => {
                    transform_any_typed_keyless::<Int8Type, _>(length, values, transform)
                }
                DataType::Int16 => {
                    transform_any_typed_keyless::<Int16Type, _>(length, values, transform)
                }
                DataType::Int32 => {
                    transform_any_typed_keyless::<Int32Type, _>(length, values, transform)
                }
                DataType::Int64 => {
                    transform_any_typed_keyless::<Int64Type, _>(length, values, transform)
                }

                DataType::UInt8 => {
                    transform_any_typed_keyless::<UInt8Type, _>(length, values, transform)
                }
                DataType::UInt16 => {
                    transform_any_typed_keyless::<UInt16Type, _>(length, values, transform)
                }
                DataType::UInt32 => {
                    transform_any_typed_keyless::<UInt32Type, _>(length, values, transform)
                }
                DataType::UInt64 => {
                    transform_any_typed_keyless::<UInt64Type, _>(length, values, transform)
                }

                d => panic!("Unexpected dictionary key type '{d}' encountered"),
            },
            DictionaryKeyArrayValues::SingleValue {
                data_type,
                length,
                value_index,
            } => match value_index {
                None => Ok(Dictionary::new(keys, values)),
                Some(value_index) => match transform(values.get_value_at(value_index))? {
                    ValueOrRef::Null => Ok(Dictionary::new_null_with_data_type(length, data_type)),
                    v => Ok(Dictionary::new_scalar_with_data_type(length, v, data_type)),
                },
            },
        }
    }
}

fn transform_boolean_typed<K: ArrowDictionaryKeyType, FTransform>(
    keys: &PrimitiveArray<K>,
    values: DictionaryValueArray<'_>,
    mut transform: FTransform,
) -> Result<BooleanArray, ExpressionError>
where
    FTransform: FnMut(ValueOrRef<'_>) -> Result<Option<bool>, ExpressionError>,
{
    let key_length = keys.len();

    let key_bit_length = arrow::util::bit_util::ceil(key_length, 8);

    let mut key_buffer = MutableBuffer::from_len_zeroed(key_bit_length);
    let key_builder = key_buffer.typed_data_mut::<u8>().as_mut_ptr();

    let mut null_buffer = None;

    let transformered_values = values.transform_into_vec(&mut transform)?;

    if keys.is_nullable() {
        let mut null_value = OnceCell::new();
        for (key_index, value_index) in keys.iter().enumerate() {
            let v = if let Some(value_index) = value_index {
                transformered_values
                    .get(<K as ArrowPrimitiveType>::Native::as_usize(value_index))
                    .unwrap_or(&None)
            } else {
                match null_value.get_or_init(|| transform(ValueOrRef::Null)) {
                    Err(_) => return Err(null_value.take().unwrap().unwrap_err()),
                    Ok(v) => v,
                }
            };

            if let Some(v) = v {
                if *v {
                    unsafe { arrow::util::bit_util::set_bit_raw(key_builder, key_index) };
                }
            } else {
                push_null(&mut null_buffer, key_index, key_bit_length);
            }
        }
    } else {
        let values = keys.values().as_ptr();

        for key_index in 0..key_length {
            let value_index = unsafe { *values.add(key_index) };
            if let Some(v) = transformered_values
                .get(<K as ArrowPrimitiveType>::Native::as_usize(value_index))
                .unwrap_or(&None)
            {
                if *v {
                    unsafe { arrow::util::bit_util::set_bit_raw(key_builder, key_index) };
                }
            } else {
                push_null(&mut null_buffer, key_index, key_bit_length);
            }
        }
    }

    Ok(BooleanArray::new(
        BooleanBufferBuilder::new_from_buffer(key_buffer, key_length).finish(),
        null_buffer.and_then(|v| NullBufferBuilder::new_from_buffer(v, key_length).finish()),
    ))
}

pub(crate) fn push_null(
    null_buffer: &mut Option<MutableBuffer>,
    index: usize,
    key_bit_length: usize,
) {
    if let Some(buffer) = null_buffer {
        let ptr = buffer.typed_data_mut::<u8>().as_mut_ptr();

        let i = index / 8;
        let b = 1 << (index % 8);

        unsafe { *ptr.add(i) &= !b };
    } else {
        let mut buffer = MutableBuffer::new(key_bit_length).with_bitset(key_bit_length, true);

        let ptr = buffer.typed_data_mut::<u8>().as_mut_ptr();

        let i = index / 8;
        let b = 1 << (index % 8);

        unsafe { *ptr.add(i) &= !b };

        *null_buffer = Some(buffer);
    }
}

fn transform_any_typed<'a, K: ArrowDictionaryKeyType, FTransform>(
    keys: &PrimitiveArray<K>,
    values: DictionaryValueArray<'a>,
    mut transform: FTransform,
) -> Result<Dictionary<'a>, ExpressionError>
where
    FTransform: FnMut(ValueOrRef<'a>) -> Result<ValueOrRef<'a>, ExpressionError>,
{
    let key_length = keys.len();
    let key_bit_length = arrow::util::bit_util::ceil(key_length, 8);

    let mut key_buffer = MutableBuffer::from_len_zeroed(size_of::<K::Native>() * key_length);
    let key_builder = key_buffer.typed_data_mut::<K::Native>().as_mut_ptr();
    let mut null_buffer = None;

    let (mut transformed_values, value_index_lookup) = values.transform_into_set(&mut |v| {
        Ok(match transform(v)? {
            ValueOrRef::Null => None,
            v => Some(v),
        })
    })?;

    let mut null_index = None;

    for (key_index, value_index) in keys.into_iter().enumerate() {
        if let Some(value_index) = value_index.map(<K as ArrowPrimitiveType>::Native::as_usize)
            && let Some(Some(transformed_value_index)) = value_index_lookup.get(&value_index)
        {
            unsafe {
                *key_builder.add(key_index) =
                    <K as ArrowPrimitiveType>::Native::from_usize(*transformed_value_index).unwrap()
            };
            continue;
        }

        let (has_value_index, value_index) = match null_index {
            Some(v) => v,
            None => {
                let v = match transform(ValueOrRef::Null)? {
                    ValueOrRef::Null => (
                        false,
                        <K as ArrowPrimitiveType>::Native::from_usize(0).unwrap(),
                    ),
                    v => {
                        let (index, _) = transformed_values.insert_full(v);
                        (
                            true,
                            <K as ArrowPrimitiveType>::Native::from_usize(index).unwrap(),
                        )
                    }
                };
                null_index = Some(v);
                v
            }
        };

        if has_value_index {
            unsafe { *key_builder.add(key_index) = value_index };
            continue;
        }

        push_null(&mut null_buffer, key_index, key_bit_length);
    }

    Ok(Dictionary::new(
        PrimitiveArray::<K>::new(
            key_buffer.into(),
            null_buffer.and_then(|v| NullBufferBuilder::new_from_buffer(v, key_length).finish()),
        )
        .into(),
        transformed_values.into(),
    ))
}

fn transform_any_typed_keyless<'a, K: ArrowDictionaryKeyType, FTransform>(
    key_length: usize,
    values: DictionaryValueArray<'a>,
    mut transform: FTransform,
) -> Result<Dictionary<'a>, ExpressionError>
where
    FTransform: FnMut(ValueOrRef<'a>) -> Result<ValueOrRef<'a>, ExpressionError>,
{
    let key_bit_length = arrow::util::bit_util::ceil(key_length, 8);

    let mut key_buffer = MutableBuffer::from_len_zeroed(size_of::<K::Native>() * key_length);
    let key_builder = key_buffer.typed_data_mut::<K::Native>().as_mut_ptr();
    let mut null_buffer = None;

    let (mut transformed_values, value_index_lookup) = values.transform_into_set(&mut |v| {
        Ok(match transform(v)? {
            ValueOrRef::Null => None,
            v => Some(v),
        })
    })?;

    let mut null_index = None;

    for key_index in 0..key_length {
        if let Some(Some(transformed_value_index)) = value_index_lookup.get(&key_index) {
            unsafe {
                *key_builder.add(key_index) =
                    <K as ArrowPrimitiveType>::Native::from_usize(*transformed_value_index).unwrap()
            };
            continue;
        }

        let (has_value_index, value_index) = match null_index {
            Some(v) => v,
            None => {
                let v = match transform(ValueOrRef::Null)? {
                    ValueOrRef::Null => (
                        false,
                        <K as ArrowPrimitiveType>::Native::from_usize(0).unwrap(),
                    ),
                    v => {
                        let (index, _) = transformed_values.insert_full(v);
                        (
                            true,
                            <K as ArrowPrimitiveType>::Native::from_usize(index).unwrap(),
                        )
                    }
                };
                null_index = Some(v);
                v
            }
        };

        if has_value_index {
            unsafe { *key_builder.add(key_index) = value_index };
            continue;
        }

        push_null(&mut null_buffer, key_index, key_bit_length);
    }

    Ok(Dictionary::new(
        PrimitiveArray::<K>::new(
            key_buffer.into(),
            null_buffer.and_then(|v| NullBufferBuilder::new_from_buffer(v, key_length).finish()),
        )
        .into(),
        transformed_values.into(),
    ))
}
