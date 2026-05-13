// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Debug, Display},
    rc::Rc,
    sync::Arc,
};

use arrow::{array::*, datatypes::*};

use crate::{engine_diagnostic::ColumnarEngineDiagnosticLevel, *};

pub trait ColumnarRecordsFactory<const BATCH_SIZE: usize> {
    type Records<'a>: ColumnarRecords
    where
        Self: 'a;

    fn create<'a>(&self, batches: &'a [Option<RecordBatch>; BATCH_SIZE]) -> Self::Records<'a>;

    fn filter(
        &self,
        batches: &[Option<RecordBatch>; BATCH_SIZE],
        filter: &BooleanArray,
    ) -> [Option<RecordBatch>; BATCH_SIZE];

    fn set(
        &self,
        batches: &mut [Option<RecordBatch>; BATCH_SIZE],
        path: &[SelectionPath<'_>],
        value: Dictionary,
    ) -> Result<(), &'static str>;
}

pub enum SelectionPath<'a> {
    Key(StringValueOrRef<'a>),
    Index(ArrayValueOrRef<'a>),
    Dictionary(Dictionary<'a>),
}

pub trait ColumnarRecords: RecordTable
where
    Self: Sized,
{
    fn get_diagnostic_level(&self) -> Option<ColumnarEngineDiagnosticLevel>;

    fn get_key_data_type(&self) -> DataType;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() > 0
    }

    fn get_attached_records(&self, name: &str) -> Option<&dyn RecordTable>;
}

pub trait RecordTable: Display + Debug {
    //fn get_keys(&self) -> &[&str];

    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>>;
}

#[derive(Debug, Clone)]
pub enum RecordTableValue<'a> {
    Dictionary(RecordTableDictionary),
    Table(&'a dyn RecordTable),
}

#[derive(Debug, Clone)]
pub struct RecordTableDictionary {
    keys: DictionaryKeyArray,
    values: RecordTableDictionaryValueArray,
}

impl RecordTableDictionary {
    pub fn new(
        keys: DictionaryKeyArray,
        values: RecordTableDictionaryValueArray,
    ) -> RecordTableDictionary {
        Self { keys, values }
    }

    pub fn from_array<K: ArrowDictionaryKeyType, V: ArrowPrimitiveType>(
        values: &PrimitiveArray<V>,
    ) -> RecordTableDictionary {
        Self {
            keys: DictionaryKeyArray::UniqueValues {
                data_type: K::DATA_TYPE,
                length: values.len(),
            },
            values: (values as &dyn Array).into(),
        }
    }

    pub fn as_dictionary(&self) -> Dictionary<'static> {
        let values = match &self.values {
            RecordTableDictionaryValueArray::Array(a) => DictionaryValueArray::Array(a.clone()),
            RecordTableDictionaryValueArray::Vec(v) => DictionaryValueArray::Vec(v.clone()),
            RecordTableDictionaryValueArray::Boolean => DictionaryValueArray::Boolean,
        };

        Dictionary::new(self.keys.clone(), values)
    }

    pub fn into_parts(self) -> (DictionaryKeyArray, RecordTableDictionaryValueArray) {
        (self.keys, self.values)
    }

    pub fn get_value_index(&self, key_index: usize) -> Option<usize> {
        self.keys.get_value_index_for_key_index(key_index)
    }

    pub fn get_value(&self, key_index: usize) -> ValueOrRef<'static> {
        if let Some(value_index) = self.get_value_index(key_index) {
            return self.values.get_value_at(value_index);
        }

        ValueOrRef::Null
    }
}

#[derive(Debug, Clone)]
pub enum RecordTableDictionaryValueArray {
    Array(Arc<dyn Array>),
    Vec(Rc<Vec<ValueOrRef<'static>>>),
    Boolean,
}

impl RecordTableDictionaryValueArray {
    pub fn get_value_at(&self, index: usize) -> ValueOrRef<'static> {
        match self {
            RecordTableDictionaryValueArray::Array(a) => get_value_from_array(a, index),
            RecordTableDictionaryValueArray::Vec(a) => {
                a.get(index).cloned().unwrap_or(ValueOrRef::Null)
            }
            RecordTableDictionaryValueArray::Boolean => ValueOrRef::Boolean(index != 0),
        }
    }
}

impl<T: ArrowDictionaryKeyType> From<&DictionaryArray<T>> for RecordTableDictionary {
    fn from(value: &DictionaryArray<T>) -> Self {
        RecordTableDictionary {
            keys: value.keys().into(),
            values: (value.values() as &dyn Array).into(),
        }
    }
}

impl<'a, K: ArrowDictionaryKeyType, V> From<TypedDictionaryArray<'a, K, V>>
    for RecordTableDictionary
where
    RecordTableDictionaryValueArray: From<&'a V>,
{
    fn from(value: TypedDictionaryArray<'a, K, V>) -> Self {
        RecordTableDictionary {
            keys: value.keys().into(),
            values: value.values().into(),
        }
    }
}

impl<T: Array> From<&T> for RecordTableDictionaryValueArray {
    fn from(value: &T) -> RecordTableDictionaryValueArray {
        RecordTableDictionaryValueArray::Array((value as &dyn Array).slice(0, value.len()))
    }
}

impl From<&dyn Array> for RecordTableDictionaryValueArray {
    fn from(value: &dyn Array) -> RecordTableDictionaryValueArray {
        RecordTableDictionaryValueArray::Array(value.slice(0, value.len()))
    }
}
