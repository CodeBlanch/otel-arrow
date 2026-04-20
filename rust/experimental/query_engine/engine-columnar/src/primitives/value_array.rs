// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::hash::{Hash, Hasher};
use std::rc::Rc;

use data_engine_expressions::*;

use crate::resolved_value::*;
use crate::*;

#[derive(Debug, Clone)]
pub enum ArrayValueOrRef<'a> {
    Ref(&'a (dyn ArrayValue + 'a)),
    Owned(Rc<OwnedArrayValue<'a>>),
    Slice(ArrayValueOrRefSlice<'a>),
}

impl<'a> ArrayValueOrRef<'a> {
    pub fn as_array_value(&self) -> &'_ (dyn ArrayValue + 'a) {
        match self {
            ArrayValueOrRef::Ref(a) => *a,
            ArrayValueOrRef::Owned(a) => a.as_ref(),
            ArrayValueOrRef::Slice(a) => a,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            ArrayValueOrRef::Ref(a) => a.len(),
            ArrayValueOrRef::Owned(a) => a.len(),
            ArrayValueOrRef::Slice(a) => a.len(),
        }
    }

    pub fn get(&self, index: usize) -> ValueOrRef<'a> {
        match self {
            ArrayValueOrRef::Ref(a) => a
                .get(index)
                .map(|v| v.to_value().into())
                .unwrap_or(ValueOrRef::Null),
            ArrayValueOrRef::Owned(a) => a
                .get_values()
                .get(index)
                .cloned()
                .unwrap_or(ValueOrRef::Null),
            ArrayValueOrRef::Slice(a) => a.get(index),
        }
    }
}

impl Hash for ArrayValueOrRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            ArrayValueOrRef::Ref(a) => {
                hash_array_value(state, *a);
            }
            ArrayValueOrRef::Owned(a) => {
                [7].hash(state);
                a.len().hash(state);
                for v in &a.values {
                    v.hash(state);
                }
            }
            ArrayValueOrRef::Slice(a) => {
                hash_array_value(state, a);
            }
        }
    }
}

fn hash_array_value<H: Hasher>(state: &mut H, a: &dyn ArrayValue) {
    [7].hash(state);
    a.len().hash(state);
    a.get_items(&mut IndexValueClosureCallback::new(|_, v| {
        Into::<ValueOrRef>::into(v).hash(state);
        true
    }));
}

impl<'a> From<ArrayValueOrRef<'a>> for ResolvedScalarValue<'a> {
    fn from(value: ArrayValueOrRef<'a>) -> Self {
        ResolvedScalarValue::Single(ValueOrRef::Array(value))
    }
}

#[derive(Debug, Clone)]
pub struct OwnedArrayValue<'a> {
    values: Vec<ValueOrRef<'a>>,
}

impl<'a> OwnedArrayValue<'a> {
    pub fn new() -> OwnedArrayValue<'a> {
        Self { values: vec![] }
    }

    pub fn get_values(&self) -> &[ValueOrRef<'a>] {
        &self.values
    }

    pub fn get_values_mut(&mut self) -> &mut Vec<ValueOrRef<'a>> {
        &mut self.values
    }
}

impl<'a, const N: usize> From<[ValueOrRef<'a>; N]> for ArrayValueOrRef<'a> {
    fn from(arr: [ValueOrRef<'a>; N]) -> Self {
        ArrayValueOrRef::Owned(
            OwnedArrayValue {
                values: Vec::from_iter(arr),
            }
            .into(),
        )
    }
}

impl<'a> ArrayValue for OwnedArrayValue<'a> {
    fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn get(&self, index: usize) -> Option<&(dyn AsValue + 'a)> {
        self.values.get(index).map(|v| v as &dyn AsValue)
    }

    fn get_static(&self, _index: usize) -> Result<Option<&(dyn AsStaticValue + 'static)>, String> {
        unreachable!("should never be called by columnar engine")
    }

    fn get_item_range(
        &self,
        range: ArrayRange,
        item_callback: &mut dyn IndexValueCallback,
    ) -> bool {
        for (index, value) in range.get_slice(&self.values).iter().enumerate() {
            if !item_callback.next(index, value.to_value()) {
                return false;
            }
        }

        true
    }
}

#[derive(Debug, Clone)]
pub struct ArrayValueOrRefSlice<'a> {
    value: Box<ArrayValueOrRef<'a>>,
    range_start_inclusive: usize,
    range_end_exclusive: usize,
}

impl<'a> ArrayValueOrRefSlice<'a> {
    pub fn new(
        value: ArrayValueOrRef<'a>,
        range_start_inclusive: usize,
        range_end_exclusive: usize,
    ) -> ArrayValueOrRefSlice<'a> {
        Self {
            value: value.into(),
            range_start_inclusive,
            range_end_exclusive,
        }
    }

    pub fn get(&self, index: usize) -> ValueOrRef<'a> {
        match self.value.as_ref() {
            ArrayValueOrRef::Ref(a) => a
                .get(self.range_start_inclusive + index)
                .map(|v| v.to_value().into())
                .unwrap_or(ValueOrRef::Null),
            ArrayValueOrRef::Owned(a) => a
                .get_values()
                .get(self.range_start_inclusive + index)
                .cloned()
                .unwrap_or(ValueOrRef::Null),
            ArrayValueOrRef::Slice(a) => a.get(self.range_start_inclusive + index),
        }
    }
}

impl ArrayValue for ArrayValueOrRefSlice<'_> {
    fn is_empty(&self) -> bool {
        self.range_end_exclusive - self.range_start_inclusive > 0
    }

    fn len(&self) -> usize {
        self.range_end_exclusive - self.range_start_inclusive
    }

    fn get(&self, index: usize) -> Option<&dyn AsValue> {
        self.value
            .as_array_value()
            .get(self.range_start_inclusive + index)
    }

    fn get_static(&self, index: usize) -> Result<Option<&(dyn AsStaticValue + 'static)>, String> {
        self.value
            .as_array_value()
            .get_static(self.range_start_inclusive + index)
    }

    fn get_item_range(
        &self,
        range: ArrayRange,
        item_callback: &mut dyn IndexValueCallback,
    ) -> bool {
        let start = range
            .get_start_range_inclusize()
            .map(|v| v + self.range_start_inclusive)
            .unwrap_or(self.range_start_inclusive);
        let end = range
            .get_end_range_exclusive()
            .map(|v| v + self.range_start_inclusive)
            .unwrap_or(self.range_end_exclusive);

        if end > self.range_end_exclusive {
            panic!(
                "range end index {} out of range for slice of length {}",
                range.get_end_range_exclusive().unwrap_or(0),
                self.range_end_exclusive - self.range_start_inclusive
            )
        }

        self.value
            .as_array_value()
            .get_item_range((start..end).into(), item_callback)
    }
}
