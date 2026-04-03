// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Write};

use arrow::array::{Array, BooleanArray};
use data_engine_expressions::*;

use crate::{slice::Slice, *};

#[derive(Debug)]
pub(crate) enum ResolvedValue<'a> {
    Table(&'a dyn RecordTable),
    Single(ResolvedSingleValue<'a>),
    Dictionary(Dictionary<'a>),
}

impl<'a> ResolvedValue<'a> {
    pub fn map_into<FSingle, FDictionary, FTable, FRet>(
        self,
        mut when_single: FSingle,
        mut when_dictionary: FDictionary,
        mut when_table: FTable,
    ) -> Result<FRet, ExpressionError>
    where
        FSingle: FnMut(ResolvedSingleValue<'a>) -> Result<FRet, ExpressionError>,
        FDictionary: FnMut(Dictionary<'a>) -> Result<FRet, ExpressionError>,
        FTable: FnMut(&'a dyn RecordTable) -> Result<FRet, ExpressionError>,
    {
        match self {
            ResolvedValue::Single(single) => when_single(single),
            ResolvedValue::Dictionary(dictionary) => when_dictionary(dictionary),
            ResolvedValue::Table(table) => when_table(table),
        }
    }
}

impl Display for ResolvedValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolvedValue::Table(t) => t.fmt(f),
            ResolvedValue::Single(s) => s.fmt(f),
            ResolvedValue::Dictionary(d) => d.fmt(f),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ResolvedSingleValue<'a> {
    Ref(Value<'a>),
    Owned(OwnedValue),
    Slice(Slice<'a>),
}

impl AsValue for ResolvedSingleValue<'_> {
    fn get_value_type(&self) -> ValueType {
        match self {
            ResolvedSingleValue::Ref(v) => v.get_value_type(),
            ResolvedSingleValue::Owned(o) => o.get_value_type(),
            ResolvedSingleValue::Slice(s) => s.get_value_type(),
        }
    }

    fn to_value(&self) -> Value<'_> {
        match self {
            ResolvedSingleValue::Ref(v) => v.clone(),
            ResolvedSingleValue::Owned(o) => o.to_value(),
            ResolvedSingleValue::Slice(s) => s.to_value(),
        }
    }
}

impl<'a> TryFrom<ResolvedSingleValue<'a>> for StringValueOrRef<'a> {
    type Error = ResolvedSingleValue<'a>;

    fn try_from(value: ResolvedSingleValue<'a>) -> Result<Self, Self::Error> {
        match value {
            ResolvedSingleValue::Ref(Value::String(s)) => Ok(StringValueOrRef::Ref(s)),
            ResolvedSingleValue::Owned(OwnedValue::String(s)) => Ok(StringValueOrRef::Owned(s)),
            ResolvedSingleValue::Slice(Slice::String(s)) => Ok(StringValueOrRef::Slice(s.into())),
            _ => Err(value),
        }
    }
}

impl Display for ResolvedSingleValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[")?;
        fmt_value(self.to_value(), f)?;
        f.write_str("]")
    }
}

#[derive(Debug)]
pub(crate) enum ResolvedBooleanValue<'a> {
    Single(bool),
    ArrayRef(&'a BooleanArray),
    ArrayOwned(BooleanArray),
}

impl<'a> ResolvedBooleanValue<'a> {
    pub fn as_single(&self) -> Option<bool> {
        match self {
            ResolvedBooleanValue::Single(s) => Some(*s),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&BooleanArray> {
        match self {
            ResolvedBooleanValue::ArrayRef(a) => Some(a),
            ResolvedBooleanValue::ArrayOwned(a) => Some(a),
            _ => None,
        }
    }
}

impl Display for ResolvedBooleanValue<'_> {
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

impl<'a> From<ResolvedBooleanValue<'a>> for ResolvedValue<'a> {
    fn from(value: ResolvedBooleanValue<'a>) -> Self {
        match value {
            ResolvedBooleanValue::Single(s) => {
                ResolvedValue::Single(ResolvedSingleValue::Owned(OwnedValue::Boolean(s)))
            }
            ResolvedBooleanValue::ArrayRef(a) => ResolvedValue::Dictionary(a.into()),
            ResolvedBooleanValue::ArrayOwned(a) => ResolvedValue::Dictionary(a.into()),
        }
    }
}
