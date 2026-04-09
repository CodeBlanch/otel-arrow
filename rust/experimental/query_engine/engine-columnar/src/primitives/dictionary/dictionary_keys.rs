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
}

impl DictionaryKeyArray<'_> {
    pub fn len(&self) -> usize {
        match self {
            DictionaryKeyArray::ArrayRef(a) => a.len(),
            DictionaryKeyArray::ArrayOwned(a) => a.len(),
            DictionaryKeyArray::BooleanRef(a) => a.len(),
            DictionaryKeyArray::BooleanOwned(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DictionaryKeyArray::ArrayRef(a) => a.is_empty(),
            DictionaryKeyArray::ArrayOwned(a) => a.is_empty(),
            DictionaryKeyArray::BooleanRef(a) => a.is_empty(),
            DictionaryKeyArray::BooleanOwned(a) => a.is_empty(),
        }
    }

    pub fn as_array(&self) -> &dyn Array {
        match self {
            DictionaryKeyArray::ArrayRef(a) => *a,
            DictionaryKeyArray::ArrayOwned(a) => a,
            DictionaryKeyArray::BooleanRef(a) => *a,
            DictionaryKeyArray::BooleanOwned(a) => a,
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
        }
    }

    pub fn create_builder(&self) -> Box<dyn DictionaryKeyArrayBuilder> {
        let key_count = self.len();

        let array = self.as_array();

        match array.data_type() {
            DataType::Int8 => Box::new(TypeDictionaryKeyArrayBuilder::<Int8Type>::new(key_count)),
            DataType::Int16 => Box::new(TypeDictionaryKeyArrayBuilder::<Int16Type>::new(key_count)),
            DataType::Int32 => Box::new(TypeDictionaryKeyArrayBuilder::<Int32Type>::new(key_count)),
            DataType::Int64 => Box::new(TypeDictionaryKeyArrayBuilder::<Int64Type>::new(key_count)),

            DataType::UInt8 => Box::new(TypeDictionaryKeyArrayBuilder::<UInt8Type>::new(key_count)),
            DataType::UInt16 => {
                Box::new(TypeDictionaryKeyArrayBuilder::<UInt16Type>::new(key_count))
            }
            DataType::UInt32 => {
                Box::new(TypeDictionaryKeyArrayBuilder::<UInt32Type>::new(key_count))
            }
            DataType::UInt64 => {
                Box::new(TypeDictionaryKeyArrayBuilder::<UInt64Type>::new(key_count))
            }

            _ => panic!("Unexpected dictionary key type"),
        }
    }
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

pub trait DictionaryKeyArrayBuilder {
    fn push_value_index(&mut self, value_index: usize);

    fn push_null(&mut self);

    fn finish(&mut self) -> DictionaryKeyArray<'static>;
}

struct TypeDictionaryKeyArrayBuilder<K: ArrowDictionaryKeyType> {
    builder: PrimitiveBuilder<K>,
}

impl<K: ArrowDictionaryKeyType> TypeDictionaryKeyArrayBuilder<K> {
    pub fn new(capacity: usize) -> TypeDictionaryKeyArrayBuilder<K> {
        Self {
            builder: PrimitiveBuilder::with_capacity(capacity),
        }
    }
}

impl<K: ArrowDictionaryKeyType> DictionaryKeyArrayBuilder for TypeDictionaryKeyArrayBuilder<K> {
    fn push_value_index(&mut self, value_index: usize) {
        self.builder
            .append_value(K::Native::from_usize(value_index).unwrap());
    }

    fn push_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> DictionaryKeyArray<'static> {
        PrimitiveBuilder::<K>::finish(&mut self.builder).into()
    }
}

fn get_key_array_value_index_for_key_index(array: &dyn Array, key_index: usize) -> Option<usize> {
    if array.is_null(key_index) {
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
    if array.is_null(key_index) {
        return None;
    }
    Some(match array.value(key_index) {
        true => 1,
        false => 0,
    })
}
