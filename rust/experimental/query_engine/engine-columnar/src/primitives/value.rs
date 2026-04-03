// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;
use std::hash::{Hash, Hasher};

use chrono::{DateTime, FixedOffset, TimeDelta};
use data_engine_expressions::*;
use regex::Regex;

use crate::slice::StringSlice;

// todo: Make Display impl on Value do this work
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

#[derive(Debug, Clone)]
pub enum ValueOrRef<'a> {
    StringRef(&'a str),
    StringOwned(String),
    IntegerRef(IntegerRef<'a>),
    IntegerOwned(i64),
    DoubleRef(DoubleRef<'a>),
    DoubleOwned(f64),
    BooleanOwned(bool),
    DateTimeOwned(DateTime<FixedOffset>),
    TimeSpanOwned(TimeDelta),
    RegexRef(&'a Regex),
    RegexOwned(Regex),
}

#[derive(Debug)]
pub(crate) enum StringValueOrRef<'a> {
    Ref(&'a dyn StringValue),
    Owned(String),
    Slice(Box<StringSlice<'a>>),
}

impl StringValue for StringValueOrRef<'_> {
    fn get_value(&self) -> &str {
        match self {
            StringValueOrRef::Ref(r) => r.get_value(),
            StringValueOrRef::Owned(o) => o,
            StringValueOrRef::Slice(s) => s.get_value(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum IntegerRef<'a> {
    Int8(&'a i8),
    Int16(&'a i16),
    Int32(&'a i32),
    Int64(&'a i64),

    UInt8(&'a u8),
    UInt16(&'a u16),
    UInt32(&'a u32),
    UInt64(&'a u64),
}

impl IntegerValue for IntegerRef<'_> {
    fn get_value(&self) -> i64 {
        match self {
            IntegerRef::Int8(i) => (**i).into(),
            IntegerRef::Int16(i) => (**i).into(),
            IntegerRef::Int32(i) => (**i).into(),
            IntegerRef::Int64(i) => **i,
            IntegerRef::UInt8(u) => (**u).into(),
            IntegerRef::UInt16(u) => (**u).into(),
            IntegerRef::UInt32(u) => (**u).into(),
            IntegerRef::UInt64(u) => **u as i64,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DoubleRef<'a> {
    Float16(&'a half::f16),
    Float32(&'a f32),
    Float64(&'a f64),
}

impl DoubleValue for DoubleRef<'_> {
    fn get_value(&self) -> f64 {
        match self {
            DoubleRef::Float16(f) => (**f).into(),
            DoubleRef::Float32(f) => (**f).into(),
            DoubleRef::Float64(f) => **f,
        }
    }
}

impl<'a, 'b> From<&'b ValueOrRef<'a>> for ValueOrRef<'b> {
    fn from(value: &'b ValueOrRef<'a>) -> Self {
        match value {
            ValueOrRef::StringRef(s) => ValueOrRef::StringRef(s),
            ValueOrRef::StringOwned(s) => ValueOrRef::StringRef(s.as_ref()),
            ValueOrRef::IntegerRef(i) => ValueOrRef::IntegerRef(i.clone()),
            ValueOrRef::IntegerOwned(i) => ValueOrRef::IntegerOwned(*i),
            ValueOrRef::DoubleRef(i) => ValueOrRef::DoubleRef(i.clone()),
            ValueOrRef::DoubleOwned(d) => ValueOrRef::DoubleOwned(*d),
            ValueOrRef::BooleanOwned(b) => ValueOrRef::BooleanOwned(*b),
            ValueOrRef::DateTimeOwned(d) => ValueOrRef::DateTimeOwned(*d),
            ValueOrRef::TimeSpanOwned(t) => ValueOrRef::TimeSpanOwned(*t),
            ValueOrRef::RegexRef(r) => ValueOrRef::RegexRef(r),
            ValueOrRef::RegexOwned(r) => ValueOrRef::RegexRef(r),
        }
    }
}

impl Hash for ValueOrRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        //core::mem::discriminant(self).hash(state);
        match self {
            ValueOrRef::StringRef(s) => {
                [0].hash(state);
                s.hash(state);
            }
            ValueOrRef::StringOwned(s) => {
                [0].hash(state);
                s.hash(state);
            }
            ValueOrRef::IntegerRef(i) => {
                [1].hash(state);
                i.get_value().hash(state);
            }
            ValueOrRef::IntegerOwned(i) => {
                [1].hash(state);
                i.get_value().hash(state);
            }
            ValueOrRef::DoubleRef(d) => {
                [2].hash(state);
                state.write_u64(d.get_value().to_bits());
            }
            ValueOrRef::DoubleOwned(d) => {
                [2].hash(state);
                state.write_u64(d.get_value().to_bits());
            }
            ValueOrRef::BooleanOwned(b) => {
                [3].hash(state);
                b.hash(state);
            }
            ValueOrRef::DateTimeOwned(d) => {
                [4].hash(state);
                d.hash(state);
            }
            ValueOrRef::TimeSpanOwned(t) => {
                [5].hash(state);
                t.hash(state);
            }
            ValueOrRef::RegexRef(r) => {
                [6].hash(state);
                r.as_str().hash(state);
            }
            ValueOrRef::RegexOwned(r) => {
                [6].hash(state);
                r.as_str().hash(state);
            }
        }
    }
}

impl PartialEq for ValueOrRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            ValueOrRef::StringRef(s) => eq_str(s, other),
            ValueOrRef::StringOwned(s) => eq_str(s, other),
            ValueOrRef::IntegerRef(i) => eq_int(i.get_value(), other),
            ValueOrRef::IntegerOwned(i) => eq_int(*i, other),
            ValueOrRef::DoubleRef(d) => eq_double(d.get_value(), other),
            ValueOrRef::DoubleOwned(d) => eq_double(*d, other),
            ValueOrRef::BooleanOwned(b) => match other {
                ValueOrRef::BooleanOwned(r) => *b == *r,
                _ => false,
            },
            ValueOrRef::DateTimeOwned(d) => match other {
                ValueOrRef::DateTimeOwned(o) => *d == *o,
                _ => false,
            },
            ValueOrRef::TimeSpanOwned(t) => match other {
                ValueOrRef::TimeSpanOwned(o) => *t == *o,
                _ => false,
            },
            ValueOrRef::RegexRef(r) => eq_regex(r, other),
            ValueOrRef::RegexOwned(r) => eq_regex(r, other),
        }
    }
}

fn eq_str(left: &str, right: &ValueOrRef) -> bool {
    match right {
        ValueOrRef::StringRef(s) => left == *s,
        ValueOrRef::StringOwned(s) => left == s,
        _ => false,
    }
}

fn eq_int(left: i64, right: &ValueOrRef) -> bool {
    match right {
        ValueOrRef::IntegerRef(i) => left == i.get_value(),
        ValueOrRef::IntegerOwned(i) => left == *i,
        _ => false,
    }
}

fn eq_double(left: f64, right: &ValueOrRef) -> bool {
    match right {
        ValueOrRef::DoubleRef(i) => left == i.get_value(),
        ValueOrRef::DoubleOwned(i) => left == *i,
        _ => false,
    }
}

fn eq_regex(left: &Regex, right: &ValueOrRef) -> bool {
    match right {
        ValueOrRef::RegexRef(i) => left.as_str() == i.as_str(),
        ValueOrRef::RegexOwned(i) => left.as_str() == i.as_str(),
        _ => false,
    }
}

impl Eq for ValueOrRef<'_> {}

impl AsValue for ValueOrRef<'_> {
    fn get_value_type(&self) -> ValueType {
        match self {
            ValueOrRef::StringRef(_) => ValueType::String,
            ValueOrRef::StringOwned(_) => ValueType::String,
            ValueOrRef::IntegerRef(_) => ValueType::Integer,
            ValueOrRef::IntegerOwned(_) => ValueType::Integer,
            ValueOrRef::DoubleRef(_) => ValueType::Double,
            ValueOrRef::DoubleOwned(_) => ValueType::Double,
            ValueOrRef::BooleanOwned(_) => ValueType::Boolean,
            ValueOrRef::DateTimeOwned(_) => ValueType::DateTime,
            ValueOrRef::TimeSpanOwned(_) => ValueType::TimeSpan,
            ValueOrRef::RegexRef(_) => ValueType::Regex,
            ValueOrRef::RegexOwned(_) => ValueType::Regex,
        }
    }

    fn to_value(&self) -> Value<'_> {
        match self {
            ValueOrRef::StringRef(s) => Value::String(s),
            ValueOrRef::StringOwned(s) => Value::String(s),
            ValueOrRef::IntegerRef(i) => Value::Integer(i),
            ValueOrRef::IntegerOwned(i) => Value::Integer(i),
            ValueOrRef::DoubleRef(d) => Value::Double(d),
            ValueOrRef::DoubleOwned(d) => Value::Double(d),
            ValueOrRef::BooleanOwned(b) => Value::Boolean(b),
            ValueOrRef::DateTimeOwned(d) => Value::DateTime(d),
            ValueOrRef::TimeSpanOwned(t) => Value::TimeSpan(t),
            ValueOrRef::RegexRef(r) => Value::Regex(r),
            ValueOrRef::RegexOwned(r) => Value::Regex(r),
        }
    }
}
