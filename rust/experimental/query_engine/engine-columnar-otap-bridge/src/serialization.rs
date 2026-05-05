// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_columnar::*;
use serde::{
    Deserializer,
    de::{Error, MapAccess, SeqAccess, Visitor},
};

pub(crate) fn from_slice(value: &[u8]) -> Result<ValueOrRef<'static>, serde_cbor::Error> {
    serde_cbor::from_slice(value).map(|v: ValueOrRefSerializationWrapper| v.0)
}

struct ValueOrRefSerializationWrapper(pub ValueOrRef<'static>);

impl<'a> serde::Deserialize<'a> for ValueOrRefSerializationWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        Ok(ValueOrRefSerializationWrapper(
            deserializer.deserialize_any(ValueOrRefVisitor)?,
        ))
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
