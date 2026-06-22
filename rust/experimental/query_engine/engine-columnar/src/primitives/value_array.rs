// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::rc::Rc;

use arrow::buffer::Buffer;
use arrow::datatypes::*;
use data_engine_expressions::*;

use crate::resolved_value::*;
use crate::*;

#[derive(Debug, Clone)]
pub enum ArrayValueOrRef<'a> {
    Ref(&'a (dyn ArrayValue + 'a)),
    Buffer(BufferArray),
    Owned(Rc<OwnedArrayValue<'a>>),
    Slice(ArrayValueOrRefSlice<'a>),
}

impl<'a> ArrayValueOrRef<'a> {
    pub fn as_array_value(&self) -> &'_ (dyn ArrayValue + 'a) {
        match self {
            ArrayValueOrRef::Ref(a) => *a,
            ArrayValueOrRef::Buffer(a) => a.as_array_value(),
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
            ArrayValueOrRef::Buffer(a) => a.as_array_value().len(),
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
            ArrayValueOrRef::Buffer(a) => a.get(index),
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
        [7].hash(state);
        match self {
            ArrayValueOrRef::Ref(a) => {
                hash_array_value(state, *a);
            }
            ArrayValueOrRef::Buffer(a) => {
                hash_array_value(state, a.as_array_value());
            }
            ArrayValueOrRef::Owned(a) => {
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

impl PartialEq for ArrayValueOrRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        let left = self.as_array_value();
        let right = other.as_array_value();

        if left.len() == right.len() {
            for index in 0..left.len() {
                match (left.get(index), right.get(index)) {
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

        false
    }
}

impl Eq for ArrayValueOrRef<'_> { }

fn hash_array_value<H: Hasher>(state: &mut H, a: &dyn ArrayValue) {
    a.len().hash(state);
    a.get_items(&mut IndexValueClosureCallback::new(|_, v| {
        Into::<ValueOrRef>::into(v).hash(state);
        true
    }));
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

impl<'a> From<ArrayValueOrRef<'a>> for ResolvedScalarValue<'a, '_> {
    fn from(value: ArrayValueOrRef<'a>) -> Self {
        ResolvedScalarValue::Single(ValueOrRef::Array(value))
    }
}

#[derive(Debug, Clone)]
pub enum BufferArray {
    U8(BufferWrapper<u8>),
}

impl BufferArray {
    pub fn new_u8(value: Buffer) -> BufferArray {
        BufferArray::U8(BufferWrapper::new(value))
    }

    pub fn as_array_value<'a>(&self) -> &(dyn ArrayValue + 'a) {
        match self {
            BufferArray::U8(b) => b,
        }
    }

    pub fn get<'a>(&self, index: usize) -> ValueOrRef<'a> {
        match self {
            BufferArray::U8(a) => a.get(index),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BufferWrapper<T> {
    value: Buffer,
    marker: PhantomData<T>,
}

impl<T: ArrowNativeType + AsStaticValue + Into<i64>> BufferWrapper<T> {
    pub fn new(value: Buffer) -> BufferWrapper<T> {
        Self {
            value,
            marker: Default::default(),
        }
    }

    pub fn get_buffer(&self) -> &Buffer {
        &self.value
    }

    pub fn get<'a>(&self, index: usize) -> ValueOrRef<'a> {
        self.value
            .typed_data::<T>()
            .get(index)
            .map(|v| ValueOrRef::Integer(Into::<i64>::into(*v)))
            .unwrap_or(ValueOrRef::Null)
    }
}

impl<T: ArrowNativeType> AsRef<[T]> for BufferWrapper<T> {
    fn as_ref(&self) -> &[T] {
        self.value.typed_data()
    }
}

impl<T: ArrowNativeType + AsStaticValue + Into<i64>> ArrayValue for BufferWrapper<T> {
    fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    fn len(&self) -> usize {
        self.value.len()
    }

    fn get(&self, index: usize) -> Option<&dyn AsValue> {
        self.value
            .typed_data::<T>()
            .get(index)
            .map(|v| v as &dyn AsValue)
    }

    fn get_static(&self, index: usize) -> Result<Option<&(dyn AsStaticValue + 'static)>, String> {
        Ok(self
            .value
            .typed_data::<T>()
            .get(index)
            .map(|v| v as &dyn AsStaticValue))
    }

    fn get_item_range(
        &self,
        range: ArrayRange,
        item_callback: &mut dyn IndexValueCallback,
    ) -> bool {
        let values = range.get_slice(self.value.typed_data::<T>());
        for (index, value) in values.iter().enumerate() {
            if !item_callback.next(index, Value::Integer(&Into::<i64>::into(*value))) {
                return false;
            }
        }
        true
    }
}

impl<T> Hash for BufferWrapper<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<T> PartialEq for BufferWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T> Eq for BufferWrapper<T> {}

#[derive(Debug, Clone)]
pub struct OwnedArrayValue<'a> {
    values: Vec<ValueOrRef<'a>>,
}

impl<'a> OwnedArrayValue<'a> {
    pub fn new() -> OwnedArrayValue<'a> {
        Self { values: vec![] }
    }

    pub fn with_capacity(capacity: usize) -> OwnedArrayValue<'a> {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn get_values(&self) -> &[ValueOrRef<'a>] {
        &self.values
    }

    pub fn get_values_mut(&mut self) -> &mut Vec<ValueOrRef<'a>> {
        &mut self.values
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

impl<'a> Default for OwnedArrayValue<'a> {
    fn default() -> Self {
        Self::new()
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
            ArrayValueOrRef::Buffer(a) => a.get(self.range_start_inclusive + index),
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
