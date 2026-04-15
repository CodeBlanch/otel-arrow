// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use chrono::{DateTime, FixedOffset, TimeDelta};
use data_engine_expressions::*;
use regex::Regex;

use crate::resolved_value::*;

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
    Array(ArrayValueOrRef<'a>),
    Boolean(bool),
    DateTime(DateTime<FixedOffset>),
    Double(f64),
    Integer(i64),
    Map(MapValueOrRef<'a>),
    Regex(RegexValueOrRef<'a>),
    String(StringValueOrRef<'a>),
    TimeSpan(TimeDelta),
}

#[derive(Debug, Clone)]
pub enum ArrayValueOrRef<'a> {
    Ref(&'a dyn ArrayValue),
    //Owned(Vec<ValueOrRef<'a>>)
}

#[derive(Debug, Clone)]
pub enum MapValueOrRef<'a> {
    Ref(&'a dyn MapValue),
    //Owned(AHashMap<Box<str>, ValueOrRef<'a>>)
}

#[derive(Debug, Clone)]
pub enum RegexValueOrRef<'a> {
    Ref(&'a Regex),
    Owned(Rc<Regex>),
}

impl RegexValueOrRef<'_> {
    pub fn new_owned(value: Regex) -> RegexValueOrRef<'static> {
        RegexValueOrRef::Owned(value.into())
    }
}

impl<'a> RegexValueOrRef<'a> {
    pub fn new_ref(value: &'a Regex) -> RegexValueOrRef<'a> {
        RegexValueOrRef::Ref(value)
    }
}

impl RegexValue for RegexValueOrRef<'_> {
    fn get_value(&self) -> &Regex {
        match self {
            RegexValueOrRef::Ref(r) => r,
            RegexValueOrRef::Owned(r) => r,
        }
    }
}

#[derive(Debug, Clone)]
pub enum StringValueOrRef<'a> {
    Ref(&'a str),
    Owned(Rc<String>),
    OwnedSlice {
        value: Rc<String>,
        start: usize,
        end: usize,
    },
}

impl StringValueOrRef<'_> {
    pub fn new_owned(value: String) -> StringValueOrRef<'static> {
        StringValueOrRef::Owned(value.into())
    }
}

impl<'a> StringValueOrRef<'a> {
    pub fn new_ref(value: &'a str) -> StringValueOrRef<'a> {
        StringValueOrRef::Ref(value)
    }

    pub(crate) fn new_slice(
        inner_value: StringValueOrRef<'a>,
        range_start_inclusive: usize,
        range_end_exclusive: usize,
    ) -> StringValueOrRef<'a> {
        let value = inner_value.get_value();

        // Note: Slice of a str returns raw utf8 bytes. Chars can take 1 to 4
        // bytes. In order to correctly slice the str as chars we have to find
        // the correct byte indices to do the slicing
        let count = range_end_exclusive - range_start_inclusive;
        if count == 0 {
            return StringValueOrRef::Ref("");
        }

        let mut chars = value.char_indices().skip(range_start_inclusive).take(count);

        if let Some(first) = chars.next() {
            let mut buf = [0; 4];
            let (start, end) = if let Some(last) = chars.last() {
                let encoded = last.1.encode_utf8(&mut buf);

                (first.0, last.0 + encoded.len())
            } else {
                let encoded = first.1.encode_utf8(&mut buf);

                (first.0, first.0 + encoded.len())
            };

            if end - start == value.len() {
                inner_value
            } else {
                match inner_value {
                    StringValueOrRef::Ref(r) => StringValueOrRef::Ref(&r[start..end]),
                    StringValueOrRef::Owned(o) => StringValueOrRef::OwnedSlice {
                        value: o,
                        start,
                        end,
                    },
                    StringValueOrRef::OwnedSlice {
                        value,
                        start: s,
                        end: _,
                    } => {
                        let start = start + s;
                        let end = end + s;

                        StringValueOrRef::OwnedSlice { value, start, end }
                    }
                }
            }
        } else {
            StringValueOrRef::Ref("")
        }
    }
}

impl StringValue for StringValueOrRef<'_> {
    fn get_value(&self) -> &str {
        match self {
            StringValueOrRef::Ref(s) => s,
            StringValueOrRef::Owned(o) => o,
            StringValueOrRef::OwnedSlice { value, start, end } => &value[*start..*end],
        }
    }
}

impl<'a> TryFrom<ResolvedSingleValue<'a>> for StringValueOrRef<'a> {
    type Error = ResolvedSingleValue<'a>;

    fn try_from(value: ResolvedSingleValue<'a>) -> Result<Self, Self::Error> {
        match value {
            ResolvedSingleValue::Value(ValueOrRef::String(s)) => Ok(s),
            _ => Err(value),
        }
    }
}

impl<'a> From<StringValueOrRef<'a>> for ResolvedScalarValue<'a> {
    fn from(value: StringValueOrRef<'a>) -> Self {
        ResolvedScalarValue::Single(ResolvedSingleValue::Value(ValueOrRef::String(value)))
    }
}

impl Hash for ValueOrRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ValueOrRef::String(s) => {
                [0].hash(state);
                s.get_value().hash(state);
            }
            ValueOrRef::Integer(i) => {
                [1].hash(state);
                i.get_value().hash(state);
            }
            ValueOrRef::Double(d) => {
                [2].hash(state);
                state.write_u64(d.get_value().to_bits());
            }
            ValueOrRef::Boolean(b) => {
                [3].hash(state);
                b.hash(state);
            }
            ValueOrRef::DateTime(d) => {
                [4].hash(state);
                d.hash(state);
            }
            ValueOrRef::TimeSpan(t) => {
                [5].hash(state);
                t.hash(state);
            }
            ValueOrRef::Regex(r) => {
                [6].hash(state);
                r.get_value().as_str().hash(state);
            }
            ValueOrRef::Array(ArrayValueOrRef::Ref(a)) => {
                [7].hash(state);
                a.len().hash(state);
                a.get_items(&mut IndexValueClosureCallback::new(|_, v| {
                    match TryInto::<ValueOrRef>::try_into(v) {
                        Ok(v) => {
                            [1].hash(state);
                            v.hash(state)
                        }
                        Err(()) => {
                            [0].hash(state);
                        }
                    }
                    true
                }));
            }
            ValueOrRef::Map(MapValueOrRef::Ref(m)) => {
                [8].hash(state);
                m.len().hash(state);
                m.get_items(&mut KeyValueClosureCallback::new(|k, v| {
                    k.hash(state);
                    match TryInto::<ValueOrRef>::try_into(v) {
                        Ok(v) => {
                            [1].hash(state);
                            v.hash(state)
                        }
                        Err(()) => {
                            [0].hash(state);
                        }
                    }
                    true
                }));
            }
        }
    }
}

impl PartialEq for ValueOrRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            ValueOrRef::String(s) => {
                if let ValueOrRef::String(other) = other {
                    s.get_value() == other.get_value()
                } else {
                    false
                }
            }
            ValueOrRef::Integer(i) => match other {
                ValueOrRef::Integer(r) => *i == *r,
                _ => false,
            },
            ValueOrRef::Double(d) => match other {
                ValueOrRef::Double(r) => *d == *r,
                _ => false,
            },
            ValueOrRef::Boolean(b) => match other {
                ValueOrRef::Boolean(r) => *b == *r,
                _ => false,
            },
            ValueOrRef::DateTime(d) => match other {
                ValueOrRef::DateTime(o) => *d == *o,
                _ => false,
            },
            ValueOrRef::TimeSpan(t) => match other {
                ValueOrRef::TimeSpan(o) => *t == *o,
                _ => false,
            },
            ValueOrRef::Regex(r) => {
                if let ValueOrRef::Regex(other) = other {
                    r.get_value().as_str() == other.get_value().as_str()
                } else {
                    false
                }
            }
            ValueOrRef::Array(ArrayValueOrRef::Ref(a)) => {
                if let ValueOrRef::Array(ArrayValueOrRef::Ref(other)) = other
                    && a.len() == other.len()
                {
                    for index in 0..a.len() {
                        match (a.get(index), other.get(index)) {
                            (None, None) => {}
                            (Some(l), Some(r)) => {
                                match (
                                    TryInto::<ValueOrRef>::try_into(l.to_value()),
                                    TryInto::<ValueOrRef>::try_into(r.to_value()),
                                ) {
                                    (Ok(l), Ok(r)) => {
                                        if l != r {
                                            return false;
                                        }
                                    }
                                    (Err(()), Err(())) => {}
                                    _ => return false,
                                }
                            }
                            _ => return false,
                        }
                    }
                    return true;
                }

                false
            }
            ValueOrRef::Map(MapValueOrRef::Ref(m)) => {
                if let ValueOrRef::Map(MapValueOrRef::Ref(other)) = other
                    && m.len() == other.len()
                {
                    return m.get_items(&mut KeyValueClosureCallback::new(|k, l| {
                        match other.get(k) {
                            None => false,
                            Some(r) => match (
                                TryInto::<ValueOrRef>::try_into(l),
                                TryInto::<ValueOrRef>::try_into(r.to_value()),
                            ) {
                                (Ok(l), Ok(r)) => l == r,
                                (Err(()), Err(())) => true,
                                _ => false,
                            },
                        }
                    }));
                }

                false
            }
        }
    }
}

impl Eq for ValueOrRef<'_> {}

impl AsValue for ValueOrRef<'_> {
    fn get_value_type(&self) -> ValueType {
        match self {
            ValueOrRef::Array(_) => ValueType::Array,
            ValueOrRef::String(_) => ValueType::String,
            ValueOrRef::Integer(_) => ValueType::Integer,
            ValueOrRef::Double(_) => ValueType::Double,
            ValueOrRef::Boolean(_) => ValueType::Boolean,
            ValueOrRef::DateTime(_) => ValueType::DateTime,
            ValueOrRef::TimeSpan(_) => ValueType::TimeSpan,
            ValueOrRef::Regex(_) => ValueType::Regex,
            ValueOrRef::Map(_) => ValueType::Map,
        }
    }

    fn to_value(&self) -> Value<'_> {
        match self {
            ValueOrRef::String(s) => Value::String(s),
            ValueOrRef::Integer(i) => Value::Integer(i),
            ValueOrRef::Double(d) => Value::Double(d),
            ValueOrRef::Boolean(b) => Value::Boolean(b),
            ValueOrRef::DateTime(d) => Value::DateTime(d),
            ValueOrRef::TimeSpan(t) => Value::TimeSpan(t),
            ValueOrRef::Regex(r) => Value::Regex(r),
            ValueOrRef::Array(ArrayValueOrRef::Ref(a)) => Value::Array(*a),
            ValueOrRef::Map(MapValueOrRef::Ref(m)) => Value::Map(*m),
        }
    }
}

impl<'a> TryInto<ValueOrRef<'a>> for Value<'a> {
    type Error = ();

    fn try_into(self) -> Result<ValueOrRef<'a>, Self::Error> {
        match self {
            Value::Array(a) => Ok(ValueOrRef::Array(ArrayValueOrRef::Ref(a))),
            Value::Boolean(b) => Ok(ValueOrRef::Boolean(b.get_value())),
            Value::DateTime(d) => Ok(ValueOrRef::DateTime(d.get_value())),
            Value::Double(d) => Ok(ValueOrRef::Double(d.get_value())),
            Value::Integer(i) => Ok(ValueOrRef::Integer(i.get_value())),
            Value::Map(m) => Ok(ValueOrRef::Map(MapValueOrRef::Ref(m))),
            Value::Null => Err(()),
            Value::Regex(r) => Ok(ValueOrRef::Regex(RegexValueOrRef::Ref(r.get_value()))),
            Value::String(s) => Ok(ValueOrRef::String(StringValueOrRef::Ref(s.get_value()))),
            Value::TimeSpan(t) => Ok(ValueOrRef::TimeSpan(t.get_value())),
        }
    }
}
