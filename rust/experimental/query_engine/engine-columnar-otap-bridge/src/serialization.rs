// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_columnar::*;
use data_engine_expressions::{
    IndexValueClosureCallback, KeyValueClosureCallback, RegexValue, StringValue, Value,
};
use serde::{
    Deserializer,
    de::{Error, MapAccess, SeqAccess, Visitor},
    ser::{SerializeMap, SerializeSeq},
};

pub(crate) fn from_slice(value: &[u8]) -> Result<ValueOrRef<'static>, serde_cbor::Error> {
    serde_cbor::from_slice(value).map(|v: ValueOrRefSerializationWrapper| v.0)
}

pub(crate) fn to_slice(value: ValueOrRef) -> Result<Vec<u8>, serde_cbor::Error> {
    serde_cbor::to_vec(&ValueOrRefSerializationWrapper(value))
}

struct ValueOrRefSerializationWrapper<'a>(pub ValueOrRef<'a>);

impl<'a> serde::Deserialize<'a> for ValueOrRefSerializationWrapper<'_> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        Ok(ValueOrRefSerializationWrapper(
            deserializer.deserialize_any(ValueOrRefVisitor)?,
        ))
    }
}

impl serde::Serialize for ValueOrRefSerializationWrapper<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.0 {
            ValueOrRef::Array(a) => match a {
                ArrayValueOrRef::Buffer(BufferArray::U8(v)) => {
                    serializer.serialize_bytes(v.get_buffer().as_slice())
                }
                a => {
                    let a = a.as_array_value();
                    let mut s = serializer.serialize_seq(Some(a.len()))?;
                    let mut e = None;
                    if !a.get_items(&mut IndexValueClosureCallback::new(|_, value| {
                        match s.serialize_element(&ValueOrRefSerializationWrapper(value.into())) {
                            Ok(_) => {}
                            Err(err) => {
                                e = Some(err);
                                return false;
                            }
                        }
                        true
                    })) {
                        return Err(e.expect("has error"));
                    }
                    s.end()
                }
            },
            ValueOrRef::Boolean(b) => serializer.serialize_bool(*b),
            ValueOrRef::DateTime(d) => match Value::DateTime(d).convert_to_integer() {
                None => serializer.serialize_none(),
                Some(v) => serializer.serialize_i64(v),
            },
            ValueOrRef::Double(d) => serializer.serialize_f64(*d),
            ValueOrRef::Integer(i) => serializer.serialize_i64(*i),
            ValueOrRef::Map(m) => {
                let m = m.as_map_value();
                let mut s = serializer.serialize_map(Some(m.len()))?;
                let mut e = None;
                if !m.get_items(&mut KeyValueClosureCallback::new(|key, value| {
                    match s.serialize_key(key) {
                        Ok(_) => {}
                        Err(err) => {
                            e = Some(err);
                            return false;
                        }
                    }
                    match s.serialize_value(&ValueOrRefSerializationWrapper(value.into())) {
                        Ok(_) => {}
                        Err(err) => {
                            e = Some(err);
                            return false;
                        }
                    }
                    true
                })) {
                    return Err(e.expect("has error"));
                }
                s.end()
            }
            ValueOrRef::Null => serializer.serialize_none(),
            ValueOrRef::Regex(r) => serializer.serialize_str(r.get_value().as_str()),
            ValueOrRef::String(s) => serializer.serialize_str(s.get_value()),
            ValueOrRef::TimeSpan(t) => match Value::TimeSpan(t).convert_to_integer() {
                None => serializer.serialize_none(),
                Some(v) => serializer.serialize_i64(v),
            },
        }
    }
}

struct ValueOrRefVisitor;

impl<'a> Visitor<'a> for ValueOrRefVisitor {
    type Value = ValueOrRef<'static>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a string, boolean, number, map, or array")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(ValueOrRef::Boolean(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(ValueOrRef::Integer(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        if value > i64::MAX as u64 {
            let message = format!("value {} out of range (max {})", value, i64::MAX);
            return Err(Error::custom(message));
        }
        Ok(ValueOrRef::Integer(value as i64))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(ValueOrRef::Double(value))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(ValueOrRef::String(StringValueOrRef::new_owned(
            value.into(),
        )))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'a>,
    {
        let mut value = match seq.size_hint() {
            Some(size) => OwnedArrayValue::with_capacity(size),
            None => OwnedArrayValue::new(),
        };

        let values = value.get_values_mut();

        while let Some::<ValueOrRefSerializationWrapper>(elem) = seq.next_element()? {
            values.push(elem.0);
        }

        Ok(ValueOrRef::Array(ArrayValueOrRef::Owned(value.into())))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'a>,
    {
        let mut value = match map.size_hint() {
            Some(size) => OwnedMapValue::with_capacity(size),
            None => OwnedMapValue::new(),
        };

        let values = value.get_values_mut();

        while let Some((k, v)) = map.next_entry::<&str, ValueOrRefSerializationWrapper>()? {
            values.insert(k.into(), v.0);
        }

        Ok(ValueOrRef::Map(MapValueOrRef::Owned(value.into())))
    }
}
