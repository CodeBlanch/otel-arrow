// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Write};

use arrow::{
    array::*,
    buffer::{MutableBuffer, NullBuffer},
    datatypes::*,
};
use data_engine_expressions::*;

use crate::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Dictionary<'a> {
    keys: DictionaryKeyArray<'a>,
    values: DictionaryValueArray<'a>,
}

impl<'a> Dictionary<'a> {
    pub fn new(keys: DictionaryKeyArray<'a>, values: DictionaryValueArray<'a>) -> Dictionary<'a> {
        Self { keys, values }
    }

    pub fn from_array<K: ArrowDictionaryKeyType, V: ArrowPrimitiveType>(
        values: &'a PrimitiveArray<V>,
    ) -> Dictionary<'a> {
        Self {
            keys: DictionaryKeyArray::None {
                data_type: K::DATA_TYPE,
                length: values.len(),
            },
            values: values.into(),
        }
    }

    pub fn new_scalar<K: ArrowDictionaryKeyType>(
        count: usize,
        value: ValueOrRef<'a>,
    ) -> Dictionary<'a> {
        Dictionary::new(
            PrimitiveArray::<K>::new(
                MutableBuffer::from_len_zeroed(size_of::<K::Native>() * count).into(),
                None,
            )
            .into(),
            vec![value].into(),
        )
    }

    pub fn new_null<K: ArrowDictionaryKeyType>(count: usize) -> Dictionary<'a> {
        Dictionary::new(
            PrimitiveArray::<K>::new(
                MutableBuffer::from_len_zeroed(size_of::<K::Native>() * count).into(),
                Some(NullBuffer::new_null(count)),
            )
            .into(),
            vec![].into(),
        )
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn keys(&self) -> &DictionaryKeyArray<'a> {
        &self.keys
    }

    pub fn values(&self) -> &DictionaryValueArray<'a> {
        &self.values
    }

    pub fn into_parts(self) -> (DictionaryKeyArray<'a>, DictionaryValueArray<'a>) {
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

impl From<BooleanArray> for Dictionary<'_> {
    fn from(value: BooleanArray) -> Self {
        Dictionary {
            keys: DictionaryKeyArray::BooleanOwned(value),
            values: DictionaryValueArray::Boolean,
        }
    }
}

impl<'a> From<&'a BooleanArray> for Dictionary<'a> {
    fn from(value: &'a BooleanArray) -> Self {
        Dictionary {
            keys: DictionaryKeyArray::BooleanRef(value),
            values: DictionaryValueArray::Boolean,
        }
    }
}

impl<'a, T: ArrowDictionaryKeyType> From<&'a DictionaryArray<T>> for Dictionary<'a> {
    fn from(value: &'a DictionaryArray<T>) -> Self {
        Dictionary {
            keys: value.keys().into(),
            values: value.values().into(),
        }
    }
}

impl<'a, K: ArrowDictionaryKeyType, V> From<TypedDictionaryArray<'a, K, V>> for Dictionary<'a>
where
    DictionaryValueArray<'a>: From<&'a V>,
{
    fn from(value: TypedDictionaryArray<'a, K, V>) -> Self {
        Dictionary {
            keys: value.keys().into(),
            values: value.values().into(),
        }
    }
}
