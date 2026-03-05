// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use chrono::{DateTime, FixedOffset, TimeDelta};
use data_engine_expressions::*;
use regex::Regex;

#[derive(Debug, Clone)]
pub enum OwnedValue {
    Array(VecArrayValue<OwnedValue>),
    Boolean(bool),
    DateTime(DateTime<FixedOffset>),
    Double(f64),
    Integer(i64),
    Map(HashMapValue<Box<str>, OwnedValue>),
    Null,
    Regex(Regex),
    String(String),
    TimeSpan(TimeDelta),
}

impl OwnedValue {
    pub fn from_json(
        query_location: &QueryLocation,
        input: &str,
    ) -> Result<OwnedValue, ExpressionError> {
        return match serde_json::from_str::<serde_json::Value>(input) {
            Ok(v) => from_value(query_location, v),
            Err(e) => Err(ExpressionError::ParseError(
                query_location.clone(),
                format!("Input could not be parsed as JSON: {e}"),
            )),
        };

        fn from_value(
            query_location: &QueryLocation,
            value: serde_json::Value,
        ) -> Result<OwnedValue, ExpressionError> {
            match value {
                serde_json::Value::Null => Ok(OwnedValue::Null),
                serde_json::Value::Bool(b) => Ok(OwnedValue::Boolean(b)),
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Ok(OwnedValue::Integer(i))
                    } else {
                        match n.as_f64().map(|f| OwnedValue::Double(f)) {
                            Some(v) => Ok(v),
                            None => Err(ExpressionError::ParseError(
                                query_location.clone(),
                                format!("Input '{n}' could not be parsed as a number"),
                            )),
                        }
                    }
                }
                serde_json::Value::String(s) => Ok(OwnedValue::String(s)),
                serde_json::Value::Array(v) => {
                    let mut values = Vec::new();
                    for value in v {
                        values.push(from_value(query_location, value)?);
                    }
                    Ok(OwnedValue::Array(VecArrayValue::new(values)))
                }
                serde_json::Value::Object(m) => {
                    let mut values = HashMap::new();
                    for (key, value) in m {
                        values.insert(key.into(), from_value(query_location, value)?);
                    }
                    Ok(OwnedValue::Map(HashMapValue::new(values)))
                }
            }
        }
    }
}

impl AsStaticValue for OwnedValue {
    fn to_static_value(&self) -> StaticValue<'_> {
        match self {
            OwnedValue::Array(a) => StaticValue::Array(a),
            OwnedValue::Boolean(b) => StaticValue::Boolean(b),
            OwnedValue::DateTime(d) => StaticValue::DateTime(d),
            OwnedValue::Double(d) => StaticValue::Double(d),
            OwnedValue::Integer(i) => StaticValue::Integer(i),
            OwnedValue::Map(m) => StaticValue::Map(m),
            OwnedValue::Null => StaticValue::Null,
            OwnedValue::Regex(r) => StaticValue::Regex(r),
            OwnedValue::String(s) => StaticValue::String(s),
            OwnedValue::TimeSpan(t) => StaticValue::TimeSpan(t),
        }
    }
}

impl From<Value<'_>> for OwnedValue {
    fn from(value: Value<'_>) -> Self {
        match value {
            Value::Array(a) => OwnedValue::Array(VecArrayValue::new(a.into())),
            Value::Boolean(b) => OwnedValue::Boolean(b.get_value()),
            Value::DateTime(d) => OwnedValue::DateTime(d.get_value()),
            Value::Double(d) => OwnedValue::Double(d.get_value()),
            Value::Integer(i) => OwnedValue::Integer(i.get_value()),
            Value::Map(m) => OwnedValue::Map(HashMapValue::new(m.into())),
            Value::Null => OwnedValue::Null,
            Value::Regex(r) => OwnedValue::Regex(r.get_value().clone()),
            Value::String(s) => OwnedValue::String(s.get_value().into()),
            Value::TimeSpan(t) => OwnedValue::TimeSpan(t.get_value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_from_json() {
        let run_test = |input: &str| {
            let value = OwnedValue::from_json(&QueryLocation::new_fake(), input).unwrap();

            assert_eq!(input, value.to_value().to_string());
        };

        run_test("true");
        run_test("false");
        run_test("18");
        run_test("18.18");
        run_test("null");
        run_test("[]");
        run_test("[1,\"two\",null]");
        run_test("{}");
        run_test("{\"key1\":1,\"key2\":\"two\",\"key3\":null}");
    }
}
