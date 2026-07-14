// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt::{Display, Write},
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use data_engine_expressions::*;

use crate::*;

#[derive(Debug)]
pub(crate) enum ResolvedScalarValue<'a, 'b>
where
    'a: 'b,
{
    Single(ValueOrRef<'a>),
    Dictionary(Dictionary<'a>),
    Table(&'b dyn RecordTable<'a>),
}

impl<'a, 'b> ResolvedScalarValue<'a, 'b> {
    pub fn new_null() -> ResolvedScalarValue<'a, 'b> {
        ResolvedScalarValue::Single(ValueOrRef::Null)
    }

    pub fn new_int(value: i64) -> ResolvedScalarValue<'a, 'b> {
        ResolvedScalarValue::Single(ValueOrRef::Integer(value))
    }

    pub fn new_from_value(value: Value<'a>) -> ResolvedScalarValue<'a, 'b> {
        ResolvedScalarValue::Single(match value {
            Value::Array(a) => ValueOrRef::Array(ArrayValueOrRef::Ref(a)),
            Value::Boolean(b) => ValueOrRef::Boolean(b.get_value()),
            Value::DateTime(d) => ValueOrRef::DateTime(d.get_value()),
            Value::Double(d) => ValueOrRef::Double(d.get_value()),
            Value::Integer(i) => ValueOrRef::Integer(i.get_value()),
            Value::Map(m) => ValueOrRef::Map(MapValueOrRef::Ref(m)),
            Value::Null => ValueOrRef::Null,
            Value::Regex(r) => ValueOrRef::Regex(RegexValueOrRef::Ref(r.get_value())),
            Value::String(s) => ValueOrRef::String(StringValueOrRef::Ref(s.get_value())),
            Value::TimeSpan(t) => ValueOrRef::TimeSpan(t.get_value()),
        })
    }

    pub fn map_into<FSingle, FDictionary, FTable, FRet>(
        self,
        when_single: FSingle,
        when_dictionary: FDictionary,
        when_table: FTable,
    ) -> FRet
    where
        FSingle: FnOnce(ValueOrRef<'a>) -> FRet,
        FDictionary: FnOnce(Dictionary<'a>) -> FRet,
        FTable: FnOnce(&'b dyn RecordTable) -> FRet,
    {
        match self {
            ResolvedScalarValue::Single(single) => when_single(single),
            ResolvedScalarValue::Dictionary(dictionary) => when_dictionary(dictionary),
            ResolvedScalarValue::Table(table) => when_table(table),
        }
    }

    pub fn map_into_with_state<TState, FSingle, FDictionary, FTable, FRet>(
        self,
        state: TState,
        when_single: FSingle,
        when_dictionary: FDictionary,
        when_table: FTable,
    ) -> FRet
    where
        FSingle: FnOnce(TState, ValueOrRef<'a>) -> FRet,
        FDictionary: FnOnce(TState, Dictionary<'a>) -> FRet,
        FTable: FnOnce(TState, &'b dyn RecordTable) -> FRet,
    {
        match self {
            ResolvedScalarValue::Single(single) => when_single(state, single),
            ResolvedScalarValue::Dictionary(dictionary) => when_dictionary(state, dictionary),
            ResolvedScalarValue::Table(table) => when_table(state, table),
        }
    }

    pub fn try_into_dictionary(
        self,
        key_type: DataType,
        key_count: usize,
    ) -> Result<Dictionary<'a>, ()> {
        match self {
            ResolvedScalarValue::Single(s) => Ok(match s {
                ValueOrRef::Null => match key_type {
                    DataType::Int8 => Dictionary::new_null::<Int8Type>(key_count),
                    DataType::Int16 => Dictionary::new_null::<Int16Type>(key_count),
                    DataType::Int32 => Dictionary::new_null::<Int32Type>(key_count),
                    DataType::Int64 => Dictionary::new_null::<Int64Type>(key_count),
                    DataType::UInt8 => Dictionary::new_null::<UInt8Type>(key_count),
                    DataType::UInt16 => Dictionary::new_null::<UInt16Type>(key_count),
                    DataType::UInt32 => Dictionary::new_null::<UInt32Type>(key_count),
                    DataType::UInt64 => Dictionary::new_null::<UInt64Type>(key_count),
                    d => panic!("Unexpected dictionary key type '{d}' encountered"),
                },
                value => match key_type {
                    DataType::Int8 => Dictionary::new_scalar::<Int8Type>(key_count, value),
                    DataType::Int16 => Dictionary::new_scalar::<Int16Type>(key_count, value),
                    DataType::Int32 => Dictionary::new_scalar::<Int32Type>(key_count, value),
                    DataType::Int64 => Dictionary::new_scalar::<Int64Type>(key_count, value),
                    DataType::UInt8 => Dictionary::new_scalar::<UInt8Type>(key_count, value),
                    DataType::UInt16 => Dictionary::new_scalar::<UInt16Type>(key_count, value),
                    DataType::UInt32 => Dictionary::new_scalar::<UInt32Type>(key_count, value),
                    DataType::UInt64 => Dictionary::new_scalar::<UInt64Type>(key_count, value),
                    d => panic!("Unexpected dictionary key type '{d}' encountered"),
                },
            }),
            ResolvedScalarValue::Dictionary(d) => Ok(d),
            ResolvedScalarValue::Table(_) => Err(()),
        }
    }
}

impl ResolvedScalarValue<'_, '_> {
    pub fn try_get_key_info(
        values: &[&ResolvedScalarValue<'_, '_>],
    ) -> Result<(usize, DataType), ()> {
        for value in values {
            if let ResolvedScalarValue::Dictionary(d) = value {
                let keys = d.keys();
                return Ok((keys.len(), keys.data_type()));
            }
        }

        Err(())
    }
}

impl Display for ResolvedScalarValue<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolvedScalarValue::Table(t) => t.fmt(f),
            ResolvedScalarValue::Single(s) => {
                f.write_char('[')?;
                fmt_value(s.to_value(), f)?;
                f.write_char(']')
            }
            ResolvedScalarValue::Dictionary(d) => d.fmt(f),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ResolvedLogicalValue {
    Single(bool),
    Array {
        data_type: DataType,
        values: Arc<dyn Array>,
    },
}

impl ResolvedLogicalValue {
    pub fn map_into<FSingle, FArray, FRet>(self, when_single: FSingle, when_array: FArray) -> FRet
    where
        FSingle: FnOnce(bool) -> FRet,
        FArray: FnOnce(DataType, Arc<dyn Array>) -> FRet,
    {
        match self {
            ResolvedLogicalValue::Single(single) => when_single(single),
            ResolvedLogicalValue::Array { data_type, values } => when_array(data_type, values),
        }
    }

    pub fn map_into_with_state<TState, FSingle, FArray, FRet>(
        self,
        state: TState,
        when_single: FSingle,
        when_array: FArray,
    ) -> FRet
    where
        FSingle: FnOnce(TState, bool) -> FRet,
        FArray: FnOnce(TState, DataType, Arc<dyn Array>) -> FRet,
    {
        match self {
            ResolvedLogicalValue::Single(single) => when_single(state, single),
            ResolvedLogicalValue::Array { data_type, values } => {
                when_array(state, data_type, values)
            }
        }
    }
}

impl Display for ResolvedLogicalValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolvedLogicalValue::Single(s) => write!(f, "[Boolean({s})]"),
            ResolvedLogicalValue::Array {
                data_type: _,
                values,
            } => {
                let a = values.as_boolean();

                f.write_char('{')?;
                for key in 0..a.len() {
                    if key > 0 {
                        f.write_char(',')?;
                    }
                    if a.is_null(key) {
                        write!(f, "{key}:Null")?;
                    } else {
                        let value = unsafe { a.value_unchecked(key) };
                        write!(f, "{key}:Boolean({value})")?;
                    }
                }
                f.write_char('}')
            }
        }
    }
}

impl From<ResolvedLogicalValue> for ResolvedScalarValue<'_, '_> {
    fn from(value: ResolvedLogicalValue) -> Self {
        match value {
            ResolvedLogicalValue::Single(s) => ResolvedScalarValue::Single(ValueOrRef::Boolean(s)),
            ResolvedLogicalValue::Array { data_type, values } => {
                ResolvedScalarValue::Dictionary(Dictionary::new(
                    DictionaryKeyArray::BooleanArray { data_type, values },
                    DictionaryValueArray::Boolean,
                ))
            }
        }
    }
}

pub enum ResolvedSingleOrDictionaryValue<'a> {
    Single(ValueOrRef<'a>),
    Dictionary(Dictionary<'a>),
}

impl<'a> ResolvedSingleOrDictionaryValue<'a> {
    pub fn into_dictionary(self, key_data_type: DataType, key_count: usize) -> Dictionary<'a> {
        match self {
            ResolvedSingleOrDictionaryValue::Single(v) => match v {
                ValueOrRef::Null => Dictionary::new_null_with_data_type(key_count, key_data_type),
                v => Dictionary::new_scalar_with_data_type(key_data_type, key_count, v),
            },
            ResolvedSingleOrDictionaryValue::Dictionary(d) => d,
        }
    }
}
