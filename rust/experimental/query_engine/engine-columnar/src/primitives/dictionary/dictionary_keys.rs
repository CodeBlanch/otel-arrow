// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::{array::*, datatypes::*};

#[derive(Debug, Clone, PartialEq)]
pub enum DictionaryKeyArray<'a> {
    ArrayRef(&'a dyn Array),
    ArrayOwned(Arc<dyn Array>),
    BooleanRef(&'a BooleanArray),
    BooleanOwned(BooleanArray),
    None { data_type: DataType, length: usize },
}

impl DictionaryKeyArray<'_> {
    pub fn len(&self) -> usize {
        match self {
            DictionaryKeyArray::ArrayRef(a) => a.len(),
            DictionaryKeyArray::ArrayOwned(a) => a.len(),
            DictionaryKeyArray::BooleanRef(a) => a.len(),
            DictionaryKeyArray::BooleanOwned(a) => a.len(),
            DictionaryKeyArray::None {
                data_type: _,
                length,
            } => *length,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DictionaryKeyArray::ArrayRef(a) => a.is_empty(),
            DictionaryKeyArray::ArrayOwned(a) => a.is_empty(),
            DictionaryKeyArray::BooleanRef(a) => a.is_empty(),
            DictionaryKeyArray::BooleanOwned(a) => a.is_empty(),
            DictionaryKeyArray::None {
                data_type: _,
                length,
            } => *length == 0,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            DictionaryKeyArray::ArrayRef(a) => a.data_type().clone(),
            DictionaryKeyArray::ArrayOwned(a) => a.data_type().clone(),
            DictionaryKeyArray::BooleanRef(a) => a.data_type().clone(),
            DictionaryKeyArray::BooleanOwned(a) => a.data_type().clone(),
            DictionaryKeyArray::None {
                data_type,
                length: _,
            } => data_type.clone(),
        }
    }

    pub fn values(&self) -> DictionaryKeyArrayValues<'_> {
        match self {
            DictionaryKeyArray::ArrayRef(a) => DictionaryKeyArrayValues::Array(*a),
            DictionaryKeyArray::ArrayOwned(a) => DictionaryKeyArrayValues::Array(a.as_ref()),
            DictionaryKeyArray::BooleanRef(a) => DictionaryKeyArrayValues::Array(*a),
            DictionaryKeyArray::BooleanOwned(a) => DictionaryKeyArrayValues::Array(a),
            DictionaryKeyArray::None { data_type, length } => DictionaryKeyArrayValues::None {
                data_type: data_type.clone(),
                length: *length,
            },
        }
    }

    pub fn get_value_index_for_key_index(&self, index: usize) -> Option<usize> {
        match self {
            DictionaryKeyArray::ArrayRef(a) => get_key_array_value_index_for_key_index(*a, index),
            DictionaryKeyArray::ArrayOwned(a) => get_key_array_value_index_for_key_index(a, index),
            DictionaryKeyArray::BooleanRef(a) => get_bool_array_value_index_for_key_index(a, index),
            DictionaryKeyArray::BooleanOwned(a) => {
                get_bool_array_value_index_for_key_index(a, index)
            }
            DictionaryKeyArray::None {
                data_type: _,
                length,
            } => {
                if index > *length {
                    None
                } else {
                    Some(index)
                }
            }
        }
    }
}

pub enum DictionaryKeyArrayValues<'a> {
    Array(&'a dyn Array),
    None { data_type: DataType, length: usize },
}

impl<T: ArrowDictionaryKeyType> From<PrimitiveArray<T>> for DictionaryKeyArray<'_> {
    fn from(value: PrimitiveArray<T>) -> DictionaryKeyArray<'static> {
        DictionaryKeyArray::ArrayOwned(Arc::new(value))
    }
}

impl<'a, T: ArrowDictionaryKeyType> From<&'a PrimitiveArray<T>> for DictionaryKeyArray<'a> {
    fn from(value: &'a PrimitiveArray<T>) -> DictionaryKeyArray<'a> {
        DictionaryKeyArray::ArrayRef(value)
    }
}

fn get_key_array_value_index_for_key_index(array: &dyn Array, key_index: usize) -> Option<usize> {
    if array.is_null(key_index) || key_index > array.len() {
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

            _ => panic!("Key type is not supported"),
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
