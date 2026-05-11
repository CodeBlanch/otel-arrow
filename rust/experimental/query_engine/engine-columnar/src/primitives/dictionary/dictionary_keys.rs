// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use ahash::AHashMap;
use arrow::{
    array::*,
    buffer::{MutableBuffer, NullBuffer},
    datatypes::*,
};

use crate::dictionary_transform::push_null;

#[derive(Debug, Clone, PartialEq)]
pub enum DictionaryKeyArray {
    KeyArray(Arc<dyn Array>),
    BooleanArray {
        data_type: DataType,
        values: Arc<dyn Array>,
    },
    UniqueValues {
        data_type: DataType,
        length: usize,
    },
    SingleValue {
        data_type: DataType,
        length: usize,
        value_index: Option<usize>,
    },
}

impl DictionaryKeyArray {
    pub fn len(&self) -> usize {
        match self {
            DictionaryKeyArray::KeyArray(a) => a.len(),
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => values.len(),
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length,
            } => *length,
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length,
                value_index: _,
            } => *length,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DictionaryKeyArray::KeyArray(a) => a.is_empty(),
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => values.is_empty(),
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length,
            } => *length == 0,
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length,
                value_index: _,
            } => *length == 0,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            DictionaryKeyArray::KeyArray(a) => a.data_type().clone(),
            DictionaryKeyArray::BooleanArray {
                data_type,
                values: _,
            } => data_type.clone(),
            DictionaryKeyArray::UniqueValues {
                data_type,
                length: _,
            } => data_type.clone(),
            DictionaryKeyArray::SingleValue {
                data_type,
                length: _,
                value_index: _,
            } => data_type.clone(),
        }
    }

    pub fn get_value_index_for_key_index(&self, index: usize) -> Option<usize> {
        match self {
            DictionaryKeyArray::KeyArray(a) => get_key_array_value_index_for_key_index(a, index),
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => get_bool_array_value_index_for_key_index(values.as_boolean(), index),
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length,
            } => {
                if index > *length {
                    None
                } else {
                    Some(index)
                }
            }
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length,
                value_index,
            } => {
                if index > *length {
                    None
                } else {
                    *value_index
                }
            }
        }
    }

    pub fn transform_into_key_array<KOutput: ArrowDictionaryKeyType>(
        self,
        value_index_lookup: Option<AHashMap<usize, Option<usize>>>,
    ) -> PrimitiveArray<KOutput> {
        match self {
            DictionaryKeyArray::KeyArray(array) => match array.data_type() {
                DataType::UInt8 => transform_key_array_into_key_array(
                    array.as_primitive::<UInt8Type>(),
                    value_index_lookup,
                ),
                DataType::UInt16 => transform_key_array_into_key_array(
                    array.as_primitive::<UInt16Type>(),
                    value_index_lookup,
                ),
                DataType::UInt32 => transform_key_array_into_key_array(
                    array.as_primitive::<UInt32Type>(),
                    value_index_lookup,
                ),
                DataType::UInt64 => transform_key_array_into_key_array(
                    array.as_primitive::<UInt64Type>(),
                    value_index_lookup,
                ),

                DataType::Int8 => transform_key_array_into_key_array(
                    array.as_primitive::<Int8Type>(),
                    value_index_lookup,
                ),
                DataType::Int16 => transform_key_array_into_key_array(
                    array.as_primitive::<Int16Type>(),
                    value_index_lookup,
                ),
                DataType::Int32 => transform_key_array_into_key_array(
                    array.as_primitive::<Int32Type>(),
                    value_index_lookup,
                ),
                DataType::Int64 => transform_key_array_into_key_array(
                    array.as_primitive::<Int64Type>(),
                    value_index_lookup,
                ),

                d => panic!("Key type '{d}' is not supported"),
            },
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => {
                let key_length = values.len();
                let key_bit_length = arrow::util::bit_util::ceil(key_length, 8);

                let mut key_buffer =
                    MutableBuffer::from_len_zeroed(size_of::<KOutput::Native>() * key_length);
                let key_builder = key_buffer.typed_data_mut::<KOutput::Native>().as_mut_ptr();
                let mut null_buffer = None;

                let true_value_index =
                    <KOutput as ArrowPrimitiveType>::Native::from_usize(1).unwrap();

                for (key_index, v) in values.as_boolean().into_iter().enumerate() {
                    if let Some(v) = v {
                        if v {
                            unsafe { *key_builder.add(key_index) = true_value_index };
                        }
                    } else {
                        push_null(&mut null_buffer, key_index, key_bit_length);
                    }
                }

                PrimitiveArray::<KOutput>::new(
                    key_buffer.into(),
                    null_buffer
                        .and_then(|v| NullBufferBuilder::new_from_buffer(v, key_length).finish()),
                )
            }
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length,
            } => {
                let mut key_buffer =
                    MutableBuffer::from_len_zeroed(size_of::<KOutput::Native>() * length);
                let key_builder = key_buffer.typed_data_mut::<KOutput::Native>().as_mut_ptr();

                for key_index in 0..length {
                    unsafe {
                        *key_builder.add(key_index) =
                            <KOutput as ArrowPrimitiveType>::Native::from_usize(key_index)
                                .expect("key index converted to output size")
                    };
                }

                PrimitiveArray::<KOutput>::new(key_buffer.into(), None)
            }
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length,
                value_index,
            } => {
                let mut key_buffer =
                    MutableBuffer::from_len_zeroed(size_of::<KOutput::Native>() * length);
                let mut null_buffer = None;

                if let Some(value_index) = value_index {
                    let key_builder = key_buffer.typed_data_mut::<KOutput::Native>().as_mut_ptr();
                    let value_index =
                        <KOutput as ArrowPrimitiveType>::Native::from_usize(value_index)
                            .expect("value index converted to output size");
                    for key_index in 0..length {
                        unsafe { *key_builder.add(key_index) = value_index };
                    }
                } else {
                    null_buffer = Some(NullBuffer::new_null(length));
                }

                PrimitiveArray::<KOutput>::new(key_buffer.into(), null_buffer)
            }
        }
    }
}

fn transform_key_array_into_key_array<
    KInput: ArrowDictionaryKeyType,
    KOutput: ArrowDictionaryKeyType,
>(
    keys: &PrimitiveArray<KInput>,
    value_index_lookup: Option<AHashMap<usize, Option<usize>>>,
) -> PrimitiveArray<KOutput> {
    let key_length = keys.len();
    let key_bit_length = arrow::util::bit_util::ceil(key_length, 8);

    let mut key_buffer = MutableBuffer::from_len_zeroed(size_of::<KOutput::Native>() * key_length);
    let key_builder = key_buffer.typed_data_mut::<KOutput::Native>().as_mut_ptr();
    let mut null_buffer = None;

    for (key_index, value_index) in keys.into_iter().enumerate() {
        if let Some(value_index) = value_index {
            if let Some(transformed_value_index) = value_index_lookup
                .as_ref()
                .and_then(|v| {
                    v.get(&<KInput as ArrowPrimitiveType>::Native::as_usize(
                        value_index,
                    ))
                })
                .and_then(|v| *v)
            {
                unsafe {
                    *key_builder.add(key_index) =
                        <KOutput as ArrowPrimitiveType>::Native::from_usize(transformed_value_index)
                            .expect("transformed value index converted to output size")
                };
                continue;
            }
        }

        push_null(&mut null_buffer, key_index, key_bit_length);
    }

    PrimitiveArray::<KOutput>::new(
        key_buffer.into(),
        null_buffer.and_then(|v| NullBufferBuilder::new_from_buffer(v, key_length).finish()),
    )
}

impl<T: ArrowDictionaryKeyType> From<PrimitiveArray<T>> for DictionaryKeyArray {
    fn from(value: PrimitiveArray<T>) -> DictionaryKeyArray {
        DictionaryKeyArray::KeyArray(Arc::new(value))
    }
}

impl From<&dyn Array> for DictionaryKeyArray {
    fn from(value: &dyn Array) -> DictionaryKeyArray {
        DictionaryKeyArray::KeyArray(value.slice(0, value.len()))
    }
}

impl<'a, T: ArrowPrimitiveType> From<&'a PrimitiveArray<T>> for DictionaryKeyArray {
    fn from(value: &'a PrimitiveArray<T>) -> DictionaryKeyArray {
        DictionaryKeyArray::KeyArray((value as &dyn Array).slice(0, value.len()))
    }
}

fn get_key_array_value_index_for_key_index(array: &dyn Array, key_index: usize) -> Option<usize> {
    if key_index > array.len() || array.is_null(key_index) {
        return None;
    }

    unsafe {
        Some(match array.data_type() {
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int8Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int16Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int32Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,

            DataType::UInt8 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt8Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::UInt16 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt16Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::UInt32 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::UInt64 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,

            d => panic!("Key type '{d}' is not supported"),
        })
    }
}

fn get_bool_array_value_index_for_key_index(
    array: &BooleanArray,
    key_index: usize,
) -> Option<usize> {
    if key_index > array.len() || array.is_null(key_index) {
        return None;
    }
    Some(match unsafe { array.value_unchecked(key_index) } {
        true => 1,
        false => 0,
    })
}
