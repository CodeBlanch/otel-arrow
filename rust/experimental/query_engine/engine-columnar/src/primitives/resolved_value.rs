// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Display, Write};

use arrow::array::{Array, BooleanArray};
use data_engine_expressions::*;

use crate::*;

#[derive(Debug)]
pub(crate) enum ResolvedValue<'a> {
    Table(&'a dyn RecordTable),
    Single(ResolvedSingleValue<'a>),
    Dictionary(Dictionary<'a>),
}

impl<'a> ResolvedValue<'a> {
    pub fn as_single(&self) -> Option<&ResolvedSingleValue<'a>> {
        match self {
            ResolvedValue::Single(s) => Some(s),
            _ => None,
        }
    }

    pub fn into_single(self) -> Result<ResolvedSingleValue<'a>, ResolvedValue<'a>> {
        match self {
            ResolvedValue::Single(s) => Ok(s),
            _ => Err(self),
        }
    }

    pub fn as_dictionary(&self) -> Option<&Dictionary<'a>> {
        match self {
            ResolvedValue::Dictionary(t) => Some(t),
            _ => None,
        }
    }

    pub fn into_dictionary(self) -> Result<Dictionary<'a>, ResolvedValue<'a>> {
        match self {
            ResolvedValue::Dictionary(t) => Ok(t),
            _ => Err(self),
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
}

impl AsValue for ResolvedSingleValue<'_> {
    fn get_value_type(&self) -> ValueType {
        match self {
            ResolvedSingleValue::Ref(v) => v.get_value_type(),
            ResolvedSingleValue::Owned(o) => o.get_value_type(),
        }
    }

    fn to_value(&self) -> Value<'_> {
        match self {
            ResolvedSingleValue::Ref(v) => v.clone(),
            ResolvedSingleValue::Owned(o) => o.to_value(),
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

pub(crate) fn fmt_value(value: Value<'_>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match value {
        Value::Null => f.write_str("Null"),
        Value::Array(a) => {
            write!(f, "Array(Count={})", a.len())
        }
        Value::Map(m) => {
            write!(f, "Map(Count={})", m.len())
        }
        Value::String(s) => {
            f.write_str("String(")?;
            let v = s.get_value();
            if v.len() <= 32 {
                f.write_str(serde_json::to_string(&v).unwrap().as_str())?;
            } else {
                write!(
                    f,
                    "{}",
                    serde_json::to_string(&format!("{}...", &v[..32]))
                        .unwrap()
                        .as_str()
                )?;
            }
            f.write_str(")")
        }
        v => {
            write!(f, "{}(", v.get_value_type())?;
            v.fmt(f)?;
            f.write_str(")")
        }
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
                    let value = unsafe{ a.value_unchecked(key) };
                    write!(f, "{key}:Boolean({value})")?;
                }
            }
            f.write_char('}')
        }
        else {
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
