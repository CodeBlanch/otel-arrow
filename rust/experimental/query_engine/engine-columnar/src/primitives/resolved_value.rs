// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Write};

use arrow::{array::*, datatypes::*};
use data_engine_expressions::*;

use crate::*;

#[derive(Debug)]
pub(crate) enum ResolvedScalarValue<'a> {
    Single(ValueOrRef<'a>),
    Dictionary(Dictionary<'a>),
    Table(&'a dyn RecordTable),
}

impl<'a> ResolvedScalarValue<'a> {
    pub fn new_null() -> ResolvedScalarValue<'a> {
        ResolvedScalarValue::Single(ValueOrRef::Null)
    }

    pub fn new_int(value: i64) -> ResolvedScalarValue<'a> {
        ResolvedScalarValue::Single(ValueOrRef::Integer(value))
    }

    pub fn new_from_value(value: Value<'a>) -> ResolvedScalarValue<'a> {
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

    pub fn new_from_value_or_ref(value: ValueOrRef<'a>) -> ResolvedScalarValue<'a> {
        ResolvedScalarValue::Single(value)
    }

    pub fn map_into<FSingle, FDictionary, FTable, FRet>(
        self,
        when_single: FSingle,
        when_dictionary: FDictionary,
        when_table: FTable,
    ) -> Result<FRet, ExpressionError>
    where
        FSingle: FnOnce(ValueOrRef<'a>) -> Result<FRet, ExpressionError>,
        FDictionary: FnOnce(Dictionary<'a>) -> Result<FRet, ExpressionError>,
        FTable: FnOnce(&'a dyn RecordTable) -> Result<FRet, ExpressionError>,
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
    ) -> Result<FRet, ExpressionError>
    where
        FSingle: FnOnce(TState, ValueOrRef<'a>) -> Result<FRet, ExpressionError>,
        FDictionary: FnOnce(TState, Dictionary<'a>) -> Result<FRet, ExpressionError>,
        FTable: FnOnce(TState, &'a dyn RecordTable) -> Result<FRet, ExpressionError>,
    {
        match self {
            ResolvedScalarValue::Single(single) => when_single(state, single),
            ResolvedScalarValue::Dictionary(dictionary) => when_dictionary(state, dictionary),
            ResolvedScalarValue::Table(table) => when_table(state, table),
        }
    }

    pub fn try_into_dictionary(
        self,
        key_count: usize,
        key_type: DataType,
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
                    _ => todo!(),
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
                    _ => todo!(),
                },
            }),
            ResolvedScalarValue::Dictionary(d) => Ok(d),
            ResolvedScalarValue::Table(_) => Err(()),
        }
    }
}

impl ResolvedScalarValue<'_> {
    pub fn try_get_key_info(values: &[&ResolvedScalarValue<'_>]) -> Result<(usize, DataType), ()> {
        for value in values {
            if let ResolvedScalarValue::Dictionary(d) = value {
                let keys = d.keys().as_array();
                return Ok((keys.len(), keys.data_type().clone()));
            }
        }

        Err(())
    }
}

impl Display for ResolvedScalarValue<'_> {
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
pub(crate) enum ResolvedLogicalValue<'a> {
    Single(bool),
    DictionaryRef(&'a BooleanArray),
    DictionaryOwned(BooleanArray),
}

impl<'a> ResolvedLogicalValue<'a> {
    pub fn as_single(&self) -> Option<bool> {
        match self {
            ResolvedLogicalValue::Single(s) => Some(*s),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&BooleanArray> {
        match self {
            ResolvedLogicalValue::DictionaryRef(a) => Some(a),
            ResolvedLogicalValue::DictionaryOwned(a) => Some(a),
            _ => None,
        }
    }
}

impl Display for ResolvedLogicalValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(s) = self.as_single() {
            write!(f, "[Boolean({s})]")
        } else if let Some(a) = self.as_array() {
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
        } else {
            unreachable!()
        }
    }
}

impl<'a> From<ResolvedLogicalValue<'a>> for ResolvedScalarValue<'a> {
    fn from(value: ResolvedLogicalValue<'a>) -> Self {
        match value {
            ResolvedLogicalValue::Single(s) => ResolvedScalarValue::Single(ValueOrRef::Boolean(s)),
            ResolvedLogicalValue::DictionaryRef(a) => ResolvedScalarValue::Dictionary(a.into()),
            ResolvedLogicalValue::DictionaryOwned(a) => ResolvedScalarValue::Dictionary(a.into()),
        }
    }
}
