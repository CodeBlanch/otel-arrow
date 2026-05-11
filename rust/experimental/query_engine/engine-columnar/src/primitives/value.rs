// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

use ahash::AHashMap;
use chrono::{DateTime, FixedOffset, TimeDelta};
use data_engine_expressions::*;
use regex::Regex;

use crate::*;

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
    Null,
    Regex(RegexValueOrRef<'a>),
    String(StringValueOrRef<'a>),
    TimeSpan(TimeDelta),
}

#[derive(Debug, Clone)]
pub enum MapValueOrRef<'a> {
    Ref(&'a (dyn MapValue + 'a)),
    Owned(Rc<OwnedMapValue<'a>>),
}

impl<'a> MapValueOrRef<'a> {
    pub fn as_map_value(&self) -> &'_ (dyn MapValue + 'a) {
        match self {
            MapValueOrRef::Ref(m) => *m,
            MapValueOrRef::Owned(m) => m.as_ref(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OwnedMapValue<'a> {
    values: AHashMap<Box<str>, ValueOrRef<'a>>,
}

impl<'a> OwnedMapValue<'a> {
    pub fn new() -> OwnedMapValue<'a> {
        Self {
            values: AHashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> OwnedMapValue<'a> {
        Self {
            values: AHashMap::with_capacity(capacity),
        }
    }

    pub fn get_values(&self) -> &AHashMap<Box<str>, ValueOrRef<'a>> {
        &self.values
    }

    pub fn get_values_mut(&mut self) -> &mut AHashMap<Box<str>, ValueOrRef<'a>> {
        &mut self.values
    }
}

impl<'a, const N: usize> From<[(Box<str>, ValueOrRef<'a>); N]> for MapValueOrRef<'a> {
    fn from(arr: [(Box<str>, ValueOrRef<'a>); N]) -> Self {
        MapValueOrRef::Owned(
            OwnedMapValue {
                values: AHashMap::<Box<str>, ValueOrRef<'a>>::from_iter(arr),
            }
            .into(),
        )
    }
}

impl<'a> MapValue for OwnedMapValue<'a> {
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn contains_key(&self, key: &str) -> bool {
        self.values.contains_key(key)
    }

    fn get(&self, key: &str) -> Option<&(dyn AsValue + 'a)> {
        self.values.get(key).map(|v| v as &dyn AsValue)
    }

    fn get_static(&self, _key: &str) -> Result<Option<&(dyn AsStaticValue + 'static)>, String> {
        unreachable!("should never be called by columnar engine")
    }

    fn get_items(&self, item_callback: &mut dyn KeyValueCallback) -> bool {
        for (key, value) in self.values.iter() {
            if !item_callback.next(key, value.to_value()) {
                return false;
            }
        }

        true
    }
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

impl Hash for ValueOrRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ValueOrRef::String(s) => {
                [0].hash(state);
                s.hash(state);
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
            ValueOrRef::Array(a) => {
                a.hash(state);
            }
            ValueOrRef::Map(MapValueOrRef::Ref(m)) => {
                [8].hash(state);
                m.len().hash(state);
                m.get_items(&mut KeyValueClosureCallback::new(|k, v| {
                    k.hash(state);
                    Into::<ValueOrRef>::into(v).hash(state);
                    true
                }));
            }
            ValueOrRef::Map(MapValueOrRef::Owned(m)) => {
                [8].hash(state);
                m.len().hash(state);
                for (k, v) in &m.values {
                    k.hash(state);
                    [1].hash(state);
                    v.hash(state);
                }
            }
            ValueOrRef::Null => [9].hash(state),
        }
    }
}

impl PartialEq for ValueOrRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        match self {
            ValueOrRef::String(s) => {
                if let ValueOrRef::String(other) = other {
                    s == other
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
            ValueOrRef::Array(a) => {
                let a = a.as_array_value();
                if let ValueOrRef::Array(other) = other {
                    let other = other.as_array_value();
                    if a.len() == other.len() {
                        for index in 0..a.len() {
                            match (a.get(index), other.get(index)) {
                                (None, None) => {}
                                (Some(l), Some(r)) => {
                                    if Into::<ValueOrRef>::into(l.to_value())
                                        != Into::<ValueOrRef>::into(r.to_value())
                                    {
                                        return false;
                                    }
                                }
                                _ => return false,
                            }
                        }
                        return true;
                    }
                }

                false
            }
            ValueOrRef::Map(m) => {
                let m = m.as_map_value();

                if let ValueOrRef::Map(other) = other {
                    let other = other.as_map_value();
                    if m.len() == other.len() {
                        return m.get_items(&mut KeyValueClosureCallback::new(|k, l| match other
                            .get(k)
                        {
                            None => false,
                            Some(r) => {
                                Into::<ValueOrRef>::into(l)
                                    == Into::<ValueOrRef>::into(r.to_value())
                            }
                        }));
                    }
                }

                false
            }
            ValueOrRef::Null => matches!(other, ValueOrRef::Null),
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
            ValueOrRef::Null => ValueType::Null,
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
            ValueOrRef::Map(m) => Value::Map(m.as_map_value()),
            ValueOrRef::Array(a) => Value::Array(a.as_array_value()),
            ValueOrRef::Null => Value::Null,
        }
    }
}

impl<'a> From<Value<'a>> for ValueOrRef<'a> {
    fn from(val: Value<'a>) -> Self {
        match val {
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
        }
    }
}
