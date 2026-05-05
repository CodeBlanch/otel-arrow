// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Write};

use arrow::{array::*, datatypes::*};
use data_engine_expressions::*;

use crate::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Dictionary<'a> {
    keys: DictionaryKeyArray,
    values: DictionaryValueArray<'a>,
}

impl<'a> Dictionary<'a> {
    pub fn new(keys: DictionaryKeyArray, values: DictionaryValueArray<'a>) -> Dictionary<'a> {
        Self { keys, values }
    }

    pub fn from_array<K: ArrowDictionaryKeyType, V: ArrowPrimitiveType>(
        values: &PrimitiveArray<V>,
    ) -> Dictionary<'a> {
        Self {
            keys: DictionaryKeyArray::UniqueValues {
                data_type: K::DATA_TYPE,
                length: values.len(),
            },
            values: (values as &dyn Array).into(),
        }
    }

    pub fn new_scalar_with_data_type(
        count: usize,
        value: ValueOrRef<'a>,
        data_type: DataType,
    ) -> Dictionary<'a> {
        match data_type {
            DataType::Int8 => Self::new_scalar::<Int8Type>(count, value),
            DataType::Int16 => Self::new_scalar::<Int16Type>(count, value),
            DataType::Int32 => Self::new_scalar::<Int32Type>(count, value),
            DataType::Int64 => Self::new_scalar::<Int64Type>(count, value),

            DataType::UInt8 => Self::new_scalar::<UInt8Type>(count, value),
            DataType::UInt16 => Self::new_scalar::<UInt16Type>(count, value),
            DataType::UInt32 => Self::new_scalar::<UInt32Type>(count, value),
            DataType::UInt64 => Self::new_scalar::<UInt64Type>(count, value),

            d => panic!("Unexpected dictionary key type '{d}' encountered"),
        }
    }

    pub fn new_scalar<K: ArrowDictionaryKeyType>(
        count: usize,
        value: ValueOrRef<'a>,
    ) -> Dictionary<'a> {
        Dictionary::new(
            DictionaryKeyArray::SingleValue {
                data_type: K::DATA_TYPE,
                length: count,
                value_index: Some(0),
            },
            vec![value].into(),
        )
    }

    pub fn new_null_with_data_type(count: usize, data_type: DataType) -> Dictionary<'a> {
        match data_type {
            DataType::Int8 => Self::new_null::<Int8Type>(count),
            DataType::Int16 => Self::new_null::<Int16Type>(count),
            DataType::Int32 => Self::new_null::<Int32Type>(count),
            DataType::Int64 => Self::new_null::<Int64Type>(count),

            DataType::UInt8 => Self::new_null::<UInt8Type>(count),
            DataType::UInt16 => Self::new_null::<UInt16Type>(count),
            DataType::UInt32 => Self::new_null::<UInt32Type>(count),
            DataType::UInt64 => Self::new_null::<UInt64Type>(count),

            d => panic!("Unexpected dictionary key type '{d}' encountered"),
        }
    }

    pub fn new_null<K: ArrowDictionaryKeyType>(count: usize) -> Dictionary<'a> {
        Dictionary::new(
            DictionaryKeyArray::SingleValue {
                data_type: K::DATA_TYPE,
                length: count,
                value_index: None,
            },
            vec![].into(),
        )
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn keys(&self) -> &DictionaryKeyArray {
        &self.keys
    }

    pub fn values(&self) -> &DictionaryValueArray<'a> {
        &self.values
    }

    pub fn into_parts(self) -> (DictionaryKeyArray, DictionaryValueArray<'a>) {
        (self.keys, self.values)
    }

    pub fn get_value_index(&self, key_index: usize) -> Option<usize> {
        self.keys.get_value_index_for_key_index(key_index)
    }

    pub fn get_value(&self, key_index: usize) -> ValueOrRef<'a> {
        if let Some(value_index) = self.get_value_index(key_index) {
            return self.values.get_value_at(value_index);
        }

        ValueOrRef::Null
    }
}

impl Display for Dictionary<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('{')?;
        for key in 0..self.keys.len() {
            if key > 0 {
                f.write_char(',')?;
            }
            write!(f, "{key}:")?;
            fmt_value(self.get_value(key).to_value(), f)?;
        }
        f.write_char('}')
    }
}

impl<'a, T: ArrowDictionaryKeyType> From<&DictionaryArray<T>> for Dictionary<'a> {
    fn from(value: &DictionaryArray<T>) -> Self {
        Dictionary {
            keys: value.keys().into(),
            values: (value.values() as &dyn Array).into(),
        }
    }
}

impl<'a, 'b, K: ArrowDictionaryKeyType, V> From<TypedDictionaryArray<'b, K, V>> for Dictionary<'a>
where
    DictionaryValueArray<'a>: From<&'b V>,
{
    fn from(value: TypedDictionaryArray<'b, K, V>) -> Self {
        Dictionary {
            keys: value.keys().into(),
            values: value.values().into(),
        }
    }
}

impl From<RecordTableDictionary> for Dictionary<'static> {
    fn from(value: RecordTableDictionary) -> Self {
        let (keys, values) = value.into_parts();

        Dictionary {
            keys,
            values: values.into(),
        }
    }
}

impl From<RecordTableDictionaryValueArray> for DictionaryValueArray<'static> {
    fn from(value: RecordTableDictionaryValueArray) -> Self {
        match value {
            RecordTableDictionaryValueArray::Array(a) => DictionaryValueArray::Array(a),
            RecordTableDictionaryValueArray::Vec(v) => DictionaryValueArray::Vec(v),
            RecordTableDictionaryValueArray::Boolean => DictionaryValueArray::Boolean,
        }
    }
}
