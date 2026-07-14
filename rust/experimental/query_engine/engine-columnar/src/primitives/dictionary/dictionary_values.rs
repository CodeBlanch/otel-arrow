// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{cell::OnceCell, hash::Hash, marker::PhantomData, rc::Rc, sync::Arc};

use ahash::{AHashMap, RandomState};
use arrow::{array::*, buffer::*, datatypes::*, util::bit_util};
use chrono::{TimeZone, Utc};
use data_engine_expressions::*;
use indexmap::IndexSet;

use crate::*;

pub(crate) type ValueOrRefSet<'a> = GenericSet<ValueOrRef<'a>>;
pub type GenericSet<T> = IndexSet<T, RandomState>;
pub type IndexLookup = Option<AHashMap<usize, Option<usize>>>;
pub(crate) type SetWithLookup<T> = (GenericSet<T>, IndexLookup);

#[derive(Debug, Clone)]
pub enum DictionaryValueArray<'a> {
    Array(Arc<dyn Array>),
    Vec(Rc<Vec<ValueOrRef<'a>>>),
    Set(Rc<ValueOrRefSet<'a>>),
    Boolean,
}

impl DictionaryValueArray<'_> {
    pub fn len(&self) -> usize {
        match self {
            DictionaryValueArray::Array(a) => a.len(),
            DictionaryValueArray::Vec(a) => a.len(),
            DictionaryValueArray::Set(a) => a.len(),
            DictionaryValueArray::Boolean => 2,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DictionaryValueArray::Array(a) => a.is_empty(),
            DictionaryValueArray::Vec(a) => a.is_empty(),
            DictionaryValueArray::Set(a) => a.is_empty(),
            DictionaryValueArray::Boolean => false,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            DictionaryValueArray::Array(a) => a.null_count() == a.len(),
            DictionaryValueArray::Vec(a) => a.iter().all(|v| matches!(v, ValueOrRef::Null)),
            DictionaryValueArray::Set(a) => a.iter().all(|v| matches!(v, ValueOrRef::Null)),
            DictionaryValueArray::Boolean => false,
        }
    }

    pub fn nulls(&self) -> Option<NullBuffer> {
        match self {
            DictionaryValueArray::Array(a) => a.nulls().cloned(),
            DictionaryValueArray::Vec(a) => {
                let mut buffer = OnceCell::new();

                for (index, value) in a.iter().enumerate() {
                    if matches!(value, ValueOrRef::Null) {
                        buffer.get_or_init(|| {
                            let l = a.len();
                            BooleanBufferBuilder::new_from_buffer(
                                MutableBuffer::new_null(a.len()),
                                l,
                            )
                        });
                        let buffer = buffer.get_mut().expect("has buffer");
                        buffer.set_bit(index, true);
                    }
                }

                buffer.take().map(|mut b| {
                    for byte in b.as_slice_mut() {
                        *byte = !*byte;
                    }

                    NullBuffer::new(b.build())
                })
            }
            DictionaryValueArray::Set(a) => {
                let mut buffer = OnceCell::new();

                for (index, value) in a.iter().enumerate() {
                    if matches!(value, ValueOrRef::Null) {
                        buffer.get_or_init(|| {
                            let l = a.len();
                            BooleanBufferBuilder::new_from_buffer(
                                MutableBuffer::new_null(a.len()),
                                l,
                            )
                        });
                        let buffer = buffer.get_mut().expect("has buffer");
                        buffer.set_bit(index, true);
                    }
                }

                buffer.take().map(|mut b| {
                    for byte in b.as_slice_mut() {
                        *byte = !*byte;
                    }

                    NullBuffer::new(b.build())
                })
            }
            DictionaryValueArray::Boolean => None,
        }
    }

    pub fn into_string_array(self) -> (StringArray, IndexLookup) {
        self.transform_into_array(
            ArrayRef::as_string_opt,
            |v| Some(v.to_string()),
            StringArray::from_iter_values,
        )
    }

    pub fn into_int_array<T: ArrowPrimitiveType>(self) -> (PrimitiveArray<T>, IndexLookup)
    where
        T::Native: Hash + Eq + TryFrom<i64>,
    {
        self.transform_into_array(
            ArrayRef::as_primitive_opt::<T>,
            |v| v.to_int::<T::Native>(),
            PrimitiveArray::<T>::from_iter_values,
        )
    }

    pub fn into_timestamp_nanoseconds_array(
        self,
    ) -> (PrimitiveArray<TimestampNanosecondType>, IndexLookup) {
        self.transform_into_array(
            ArrayRef::as_primitive_opt::<TimestampNanosecondType>,
            |v| {
                v.to_value()
                    .convert_to_datetime()
                    .and_then(|v| v.timestamp_nanos_opt())
            },
            PrimitiveArray::<TimestampNanosecondType>::from_iter_values,
        )
    }

    pub fn into_fixed_sized_binary_array<const SIZE: usize>(
        self,
    ) -> (FixedSizeBinaryArray, IndexLookup) {
        self.transform_into_array(
            |v| {
                v.as_fixed_size_binary_opt().and_then(|v| {
                    if v.value_length() as usize == SIZE {
                        Some(v)
                    } else {
                        None
                    }
                })
            },
            |v| match v {
                ValueOrRef::Array(ArrayValueOrRef::Buffer(BufferArray::U8(values))) => {
                    if values.len() == SIZE {
                        Some(values.clone())
                    } else {
                        None
                    }
                }
                ValueOrRef::Array(array) => {
                    if array.len() == SIZE {
                        let mut buffer = MutableBuffer::from_len_zeroed(SIZE);
                        let builder = buffer.as_mut_ptr();
                        if array
                            .as_array_value()
                            .get_items(&mut IndexValueClosureCallback::new(|index, value| {
                                if let Some(v) = value.convert_to_integer()
                                    && let Ok(v) = TryInto::<u8>::try_into(v)
                                {
                                    unsafe { *builder.add(index) = v };
                                    return true;
                                }
                                false
                            }))
                        {
                            Some(BufferWrapper::new(buffer.into()))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            },
            |v| {
                if v.len() == 0 {
                    FixedSizeBinaryArray::new_null(SIZE as i32, 0)
                } else {
                    FixedSizeBinaryArray::try_from_iter(v.into_iter()).expect("valid array")
                }
            },
        )
    }
}

impl<'a> DictionaryValueArray<'a> {
    pub fn get_value_at(&self, index: usize) -> ValueOrRef<'a> {
        match self {
            DictionaryValueArray::Array(a) => get_value_from_array(a, index),
            DictionaryValueArray::Vec(a) => a.get(index).cloned().unwrap_or(ValueOrRef::Null),
            DictionaryValueArray::Set(a) => a.get_index(index).cloned().unwrap_or(ValueOrRef::Null),
            DictionaryValueArray::Boolean => ValueOrRef::Boolean(index != 0),
        }
    }

    pub fn into_set(self) -> SetWithLookup<ValueOrRef<'a>> {
        let transform = |v| {
            if matches!(v, ValueOrRef::Null) {
                None
            } else {
                Some(v)
            }
        };

        let (set, lookup) = match self {
            DictionaryValueArray::Array(a) => transform_array_into_set(transform, a),
            DictionaryValueArray::Vec(a) => match Rc::try_unwrap(a) {
                Ok(v) => transform_iter_into_set(transform, v.into_iter()),
                Err(v) => transform_iter_into_set(transform, v.iter().cloned()),
            },
            DictionaryValueArray::Set(a) => (Rc::unwrap_or_clone(a), None),
            DictionaryValueArray::Boolean => {
                let mut set = ValueOrRefSet::with_capacity_and_hasher(2, RandomState::new());
                set.insert(ValueOrRef::Boolean(false));
                set.insert(ValueOrRef::Boolean(true));
                (set, None)
            }
        };

        (set, lookup)
    }

    pub fn transform_into_set<T: Hash + Eq, FTransform>(
        self,
        transform: FTransform,
    ) -> SetWithLookup<T>
    where
        FTransform: FnMut(ValueOrRef<'a>) -> Option<T>,
    {
        match self {
            DictionaryValueArray::Array(a) => transform_array_into_set(transform, a),
            DictionaryValueArray::Vec(a) => match Rc::try_unwrap(a) {
                Ok(v) => transform_iter_into_set(transform, v.into_iter()),
                Err(v) => transform_iter_into_set(transform, v.iter().cloned()),
            },
            DictionaryValueArray::Set(a) => match Rc::try_unwrap(a) {
                Ok(v) => transform_iter_into_set(transform, v.into_iter()),
                Err(v) => transform_iter_into_set(transform, v.iter().cloned()),
            },
            DictionaryValueArray::Boolean => transform_iter_into_set(
                transform,
                [ValueOrRef::Boolean(false), ValueOrRef::Boolean(true)].into_iter(),
            ),
        }
    }

    pub fn transform_into_boolean<FTransform>(self, transform: FTransform) -> BooleanArray
    where
        FTransform: FnMut(&ValueOrRef<'a>) -> Option<bool>,
    {
        match self {
            DictionaryValueArray::Array(a) => transform_array_into_boolean(transform, a),
            DictionaryValueArray::Vec(a) => iter_into_boolean_array(a.iter().map(transform)),
            DictionaryValueArray::Set(a) => iter_into_boolean_array(a.iter().map(transform)),
            DictionaryValueArray::Boolean => {
                let mut buffer = MutableBuffer::from_len_zeroed(1);
                unsafe { *buffer.as_mut_ptr() = 0b10 };
                BooleanArray::new(BooleanBuffer::new(buffer.into(), 0, 2), None)
            }
        }
    }

    pub fn transform_into_array<TArray: Array + Clone, T: Hash + Eq, FAsArray, FTransform, FBuild>(
        self,
        as_array: FAsArray,
        transform: FTransform,
        build: FBuild,
    ) -> (TArray, IndexLookup)
    where
        FAsArray: Fn(&Arc<dyn Array>) -> Option<&TArray>,
        FTransform: Fn(&ValueOrRef<'a>) -> Option<T>,
        FBuild: Fn(indexmap::set::IntoIter<T>) -> TArray,
    {
        match self {
            DictionaryValueArray::Array(a) => {
                if let Some(s) = as_array(&a) {
                    (s.clone(), None)
                } else {
                    let (values, lookup) = transform_array_into_set(|v| transform(&v), a);

                    (build(values.into_iter()), lookup)
                }
            }
            DictionaryValueArray::Vec(a) => {
                let null_value = transform(&ValueOrRef::Null);

                iter_into_array(a.iter().map(transform), null_value, build)
            }
            DictionaryValueArray::Set(a) => {
                let null_value = transform(&ValueOrRef::Null);

                iter_into_array(a.iter().map(transform), null_value, build)
            }
            DictionaryValueArray::Boolean => {
                let values = [ValueOrRef::Boolean(false), ValueOrRef::Boolean(true)];
                iter_into_array(values.iter().map(transform), None, build)
            }
        }
    }
}

impl PartialEq for DictionaryValueArray<'_> {
    fn eq(&self, other: &Self) -> bool {
        let length = self.len();

        if length != other.len() {
            return false;
        }

        for index in 0..length {
            if self.get_value_at(index) != other.get_value_at(index) {
                return false;
            }
        }

        true
    }
}

impl<'a, T: Array + 'a> From<&T> for DictionaryValueArray<'a> {
    fn from(value: &T) -> DictionaryValueArray<'a> {
        DictionaryValueArray::Array((value as &dyn Array).slice(0, value.len()))
    }
}

impl<'a> From<&dyn Array> for DictionaryValueArray<'a> {
    fn from(value: &dyn Array) -> DictionaryValueArray<'a> {
        DictionaryValueArray::Array(value.slice(0, value.len()))
    }
}

impl<'a> From<ValueOrRefSet<'a>> for DictionaryValueArray<'a> {
    fn from(value: ValueOrRefSet<'a>) -> DictionaryValueArray<'a> {
        DictionaryValueArray::Set(value.into())
    }
}

impl<'a> From<Vec<ValueOrRef<'a>>> for DictionaryValueArray<'a> {
    fn from(value: Vec<ValueOrRef<'a>>) -> DictionaryValueArray<'a> {
        DictionaryValueArray::Vec(value.into())
    }
}

fn transform_iter_into_set<'a, T: Hash + Eq, FTransform, I>(
    mut transform: FTransform,
    values: I,
) -> SetWithLookup<T>
where
    FTransform: FnMut(ValueOrRef<'a>) -> Option<T>,
    I: Iterator<Item = ValueOrRef<'a>> + ExactSizeIterator,
{
    let null_value = transform(ValueOrRef::Null);

    iter_into_set(values.map(transform), null_value)
}

fn transform_array_into_set<'a, T: Hash + Eq, FTransform>(
    transform: FTransform,
    value: Arc<dyn Array>,
) -> SetWithLookup<T>
where
    FTransform: FnMut(ValueOrRef<'a>) -> Option<T>,
{
    match value.data_type() {
        DataType::Int8 => {
            let a = value.as_primitive::<Int8Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))),
            )
        }
        DataType::Int16 => {
            let a = value.as_primitive::<Int16Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))),
            )
        }
        DataType::Int32 => {
            let a = value.as_primitive::<Int32Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))),
            )
        }
        DataType::Int64 => {
            let a = value.as_primitive::<Int64Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, ValueOrRef::Integer)),
            )
        }

        DataType::UInt8 => {
            let a = value.as_primitive::<UInt8Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))),
            )
        }
        DataType::UInt16 => {
            let a = value.as_primitive::<UInt16Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))),
            )
        }
        DataType::UInt32 => {
            let a = value.as_primitive::<UInt32Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))),
            )
        }
        DataType::UInt64 => {
            let a = value.as_primitive::<UInt64Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))),
            )
        }

        DataType::Float16 => {
            let a = value.as_primitive::<Float16Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, |v| ValueOrRef::Double(f64::from(v)))),
            )
        }
        DataType::Float32 => {
            let a = value.as_primitive::<Float32Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, |v| ValueOrRef::Double(v as f64))),
            )
        }
        DataType::Float64 => {
            let a = value.as_primitive::<Float64Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.map(|v| v.map_or(ValueOrRef::Null, ValueOrRef::Double)),
            )
        }

        DataType::Utf8 => {
            transform_iter_into_set(transform, StringArrayIter::new(value.as_string::<i32>()))
        }
        DataType::LargeUtf8 => {
            transform_iter_into_set(transform, StringArrayIter::new(value.as_string::<i64>()))
        }

        DataType::Timestamp(time_unit, _) => match time_unit {
            TimeUnit::Second => {
                let a = value.as_primitive::<TimestampSecondType>().into_iter();

                transform_iter_into_set(
                    transform,
                    a.map(|v| {
                        v.map_or(ValueOrRef::Null, |secs| {
                            ValueOrRef::DateTime(Utc.timestamp_opt(secs, 0).unwrap().into())
                        })
                    }),
                )
            }
            TimeUnit::Millisecond => {
                let a = value.as_primitive::<TimestampMillisecondType>().into_iter();

                transform_iter_into_set(
                    transform,
                    a.map(|v| {
                        v.map_or(ValueOrRef::Null, |millis| {
                            ValueOrRef::DateTime(Utc.timestamp_millis_opt(millis).unwrap().into())
                        })
                    }),
                )
            }
            TimeUnit::Microsecond => {
                let a = value.as_primitive::<TimestampMicrosecondType>().into_iter();

                transform_iter_into_set(
                    transform,
                    a.map(|v| {
                        v.map_or(ValueOrRef::Null, |micros| {
                            ValueOrRef::DateTime(Utc.timestamp_micros(micros).unwrap().into())
                        })
                    }),
                )
            }
            TimeUnit::Nanosecond => {
                let a = value.as_primitive::<TimestampNanosecondType>().into_iter();

                transform_iter_into_set(
                    transform,
                    a.map(|v| {
                        v.map_or(ValueOrRef::Null, |nanos| {
                            ValueOrRef::DateTime(Utc.timestamp_nanos(nanos).into())
                        })
                    }),
                )
            }
        },

        DataType::FixedSizeBinary(_) => transform_iter_into_set(
            transform,
            FixedSizeBinaryArrayIter::new(value.as_fixed_size_binary()),
        ),

        d => todo!("{d} is not implemented"),
    }
}

pub fn transform_array_into_boolean<'a, FTransform>(
    mut transform: FTransform,
    value: Arc<dyn Array>,
) -> BooleanArray
where
    FTransform: FnMut(&ValueOrRef<'a>) -> Option<bool>,
{
    match value.data_type() {
        DataType::Int8 => iter_into_boolean_array(
            value
                .as_primitive::<Int8Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64)))),
        ),
        DataType::Int16 => iter_into_boolean_array(
            value
                .as_primitive::<Int16Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64)))),
        ),
        DataType::Int32 => iter_into_boolean_array(
            value
                .as_primitive::<Int32Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64)))),
        ),
        DataType::Int64 => iter_into_boolean_array(
            value
                .as_primitive::<Int64Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, ValueOrRef::Integer))),
        ),

        DataType::UInt8 => iter_into_boolean_array(
            value
                .as_primitive::<UInt8Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64)))),
        ),
        DataType::UInt16 => iter_into_boolean_array(
            value
                .as_primitive::<UInt16Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64)))),
        ),
        DataType::UInt32 => iter_into_boolean_array(
            value
                .as_primitive::<UInt32Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64)))),
        ),
        DataType::UInt64 => iter_into_boolean_array(
            value
                .as_primitive::<UInt64Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64)))),
        ),

        DataType::Float16 => {
            iter_into_boolean_array(value.as_primitive::<Float16Type>().into_iter().map(|v| {
                transform(&v.map_or(ValueOrRef::Null, |v| ValueOrRef::Double(f64::from(v))))
            }))
        }
        DataType::Float32 => iter_into_boolean_array(
            value
                .as_primitive::<Float32Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, |v| ValueOrRef::Double(v as f64)))),
        ),
        DataType::Float64 => iter_into_boolean_array(
            value
                .as_primitive::<Float64Type>()
                .into_iter()
                .map(|v| transform(&v.map_or(ValueOrRef::Null, ValueOrRef::Double))),
        ),

        DataType::Utf8 => iter_into_boolean_array(
            StringArrayIter::new(value.as_string::<i32>()).map(|v| transform(&v)),
        ),
        DataType::LargeUtf8 => iter_into_boolean_array(
            StringArrayIter::new(value.as_string::<i64>()).map(|v| transform(&v)),
        ),

        DataType::Timestamp(time_unit, _) => match time_unit {
            TimeUnit::Second => iter_into_boolean_array(
                value
                    .as_primitive::<TimestampSecondType>()
                    .into_iter()
                    .map(|v| {
                        transform(&v.map_or(ValueOrRef::Null, |secs| {
                            ValueOrRef::DateTime(Utc.timestamp_opt(secs, 0).unwrap().into())
                        }))
                    }),
            ),
            TimeUnit::Millisecond => iter_into_boolean_array(
                value
                    .as_primitive::<TimestampMillisecondType>()
                    .into_iter()
                    .map(|v| {
                        transform(&v.map_or(ValueOrRef::Null, |millis| {
                            ValueOrRef::DateTime(Utc.timestamp_millis_opt(millis).unwrap().into())
                        }))
                    }),
            ),
            TimeUnit::Microsecond => iter_into_boolean_array(
                value
                    .as_primitive::<TimestampMicrosecondType>()
                    .into_iter()
                    .map(|v| {
                        transform(&v.map_or(ValueOrRef::Null, |micros| {
                            ValueOrRef::DateTime(Utc.timestamp_micros(micros).unwrap().into())
                        }))
                    }),
            ),
            TimeUnit::Nanosecond => iter_into_boolean_array(
                value
                    .as_primitive::<TimestampNanosecondType>()
                    .into_iter()
                    .map(|v| {
                        transform(&v.map_or(ValueOrRef::Null, |nanos| {
                            ValueOrRef::DateTime(Utc.timestamp_nanos(nanos).into())
                        }))
                    }),
            ),
        },

        DataType::FixedSizeBinary(_) => iter_into_boolean_array(
            FixedSizeBinaryArrayIter::new(value.as_fixed_size_binary()).map(|v| transform(&v)),
        ),

        d => todo!("{d} is not implemented"),
    }
}

fn init_lookup(capacity: usize, fill_count: usize) -> AHashMap<usize, Option<usize>> {
    let mut lookup = AHashMap::with_capacity(capacity);
    for value_index in 0..fill_count {
        lookup.insert(value_index, Some(value_index));
    }
    lookup
}

fn iter_into_set<T: Hash + Eq, I>(values: I, mut null_value: Option<T>) -> SetWithLookup<T>
where
    I: Iterator<Item = Option<T>> + ExactSizeIterator,
{
    let mut set = IndexSet::with_capacity_and_hasher(values.len(), RandomState::new());
    let mut lookup = None;
    let mut null_value_index = None;

    for (value_index, value) in values.enumerate() {
        if let Some(value) = value {
            let (index, inserted) = set.insert_full(value);

            if !inserted {
                lookup
                    .get_or_insert_with(|| init_lookup(set.capacity(), value_index))
                    .insert(value_index, Some(index));
            } else if let Some(lookup) = lookup.as_mut() {
                lookup.insert(value_index, Some(index));
            }
        } else {
            match null_value_index {
                None => {
                    if let Some(null_value) = null_value.take() {
                        let (index, inserted) = set.insert_full(null_value);

                        if !inserted {
                            lookup
                                .get_or_insert_with(|| init_lookup(set.capacity(), value_index))
                                .insert(value_index, Some(index));
                        } else if let Some(lookup) = lookup.as_mut() {
                            lookup.insert(value_index, Some(index));
                        }

                        null_value_index = Some(Some(index));
                    } else {
                        lookup
                            .get_or_insert_with(|| init_lookup(set.capacity(), value_index))
                            .insert(value_index, None);

                        null_value_index = Some(None)
                    }
                }
                Some(Some(null_value_index)) => {
                    lookup
                        .get_or_insert_with(|| init_lookup(set.capacity(), value_index))
                        .insert(value_index, Some(null_value_index));
                }
                Some(None) => {
                    lookup
                        .get_or_insert_with(|| init_lookup(set.capacity(), value_index))
                        .insert(value_index, None);
                }
            }
        }
    }

    (set, lookup)
}

fn iter_into_array<
    TItems: Iterator<Item = Option<TInput>> + ExactSizeIterator,
    TInput: Hash + PartialEq + Eq,
    TOutput: Array,
    FBuild,
>(
    values: TItems,
    null_value: Option<TInput>,
    build: FBuild,
) -> (TOutput, IndexLookup)
where
    FBuild: Fn(indexmap::set::IntoIter<TInput>) -> TOutput,
{
    let (set, lookup) = iter_into_set(values, null_value);

    (build(set.into_iter()), lookup)
}

fn iter_into_boolean_array<TIterator>(values: TIterator) -> BooleanArray
where
    TIterator: Iterator<Item = Option<bool>> + ExactSizeIterator,
{
    let length = values.len();
    let mut buffer = MutableBuffer::new_null(length);
    let mut nulls = None;

    for (value_index, value) in values.enumerate() {
        match value {
            None => {
                unsafe { arrow_utils::ensure_nulls_unchecked(&mut nulls, length, value_index) };
            }
            Some(v) => {
                if v {
                    bit_util::set_bit(&mut buffer, value_index);
                }
                if let Some(nulls) = &mut nulls {
                    bit_util::set_bit(nulls, value_index);
                }
            }
        }
    }
    BooleanArray::new(
        BooleanBuffer::new(buffer.into(), 0, length),
        nulls.and_then(|v| NullBufferBuilder::new_from_buffer(v, length).build()),
    )
}

pub(crate) fn get_value_from_array(value: &Arc<dyn Array>, index: usize) -> ValueOrRef<'static> {
    if index > value.len() || value.nulls().map(|n| n.is_null(index)).unwrap_or(false) {
        return ValueOrRef::Null;
    }

    unsafe {
        match value.data_type() {
            DataType::Int8 => ValueOrRef::Integer(
                *value
                    .as_primitive::<Int8Type>()
                    .values()
                    .get_unchecked(index) as i64,
            ),
            DataType::Int16 => ValueOrRef::Integer(
                *value
                    .as_primitive::<Int16Type>()
                    .values()
                    .get_unchecked(index) as i64,
            ),
            DataType::Int32 => ValueOrRef::Integer(
                *value
                    .as_primitive::<Int32Type>()
                    .values()
                    .get_unchecked(index) as i64,
            ),
            DataType::Int64 => ValueOrRef::Integer(
                *value
                    .as_primitive::<Int64Type>()
                    .values()
                    .get_unchecked(index),
            ),

            DataType::UInt8 => ValueOrRef::Integer(
                *value
                    .as_primitive::<UInt8Type>()
                    .values()
                    .get_unchecked(index) as i64,
            ),
            DataType::UInt16 => ValueOrRef::Integer(
                *value
                    .as_primitive::<UInt16Type>()
                    .values()
                    .get_unchecked(index) as i64,
            ),
            DataType::UInt32 => ValueOrRef::Integer(
                *value
                    .as_primitive::<UInt32Type>()
                    .values()
                    .get_unchecked(index) as i64,
            ),
            DataType::UInt64 => ValueOrRef::Integer(
                *value
                    .as_primitive::<UInt64Type>()
                    .values()
                    .get_unchecked(index) as i64,
            ),

            DataType::Float16 => ValueOrRef::Double(
                (*value
                    .as_primitive::<Float16Type>()
                    .values()
                    .get_unchecked(index))
                .into(),
            ),
            DataType::Float32 => ValueOrRef::Double(
                *value
                    .as_primitive::<Float32Type>()
                    .values()
                    .get_unchecked(index) as f64,
            ),
            DataType::Float64 => ValueOrRef::Double(
                *value
                    .as_primitive::<Float64Type>()
                    .values()
                    .get_unchecked(index),
            ),

            DataType::Utf8 => ValueOrRef::String(StringValueOrRef::Buffer({
                let strings = value.as_string::<i32>();
                let offsets = strings.value_offsets();
                let end = *offsets.get_unchecked(index + 1) as usize;
                let start = *offsets.get_unchecked(index) as usize;
                strings.values().slice_with_length(start, end - start)
            })),
            DataType::LargeUtf8 => ValueOrRef::String(StringValueOrRef::Buffer({
                let strings = value.as_string::<i64>();
                let offsets = strings.value_offsets();
                let end = *offsets.get_unchecked(index + 1) as usize;
                let start = *offsets.get_unchecked(index) as usize;
                strings.values().slice_with_length(start, end - start)
            })),

            DataType::Timestamp(time_unit, _) => ValueOrRef::DateTime(match time_unit {
                TimeUnit::Second => {
                    let secs = *value
                        .as_primitive::<TimestampSecondType>()
                        .values()
                        .get_unchecked(index);
                    Utc.timestamp_opt(secs, 0).unwrap().into()
                }
                TimeUnit::Millisecond => {
                    let millis = *value
                        .as_primitive::<TimestampMillisecondType>()
                        .values()
                        .get_unchecked(index);
                    Utc.timestamp_millis_opt(millis).unwrap().into()
                }
                TimeUnit::Microsecond => {
                    let micros = *value
                        .as_primitive::<TimestampMicrosecondType>()
                        .values()
                        .get_unchecked(index);
                    Utc.timestamp_micros(micros).unwrap().into()
                }
                TimeUnit::Nanosecond => {
                    let nanos = *value
                        .as_primitive::<TimestampNanosecondType>()
                        .values()
                        .get_unchecked(index);
                    Utc.timestamp_nanos(nanos).into()
                }
            }),

            DataType::FixedSizeBinary(_) => ValueOrRef::Array(ArrayValueOrRef::Buffer({
                let bytes = value.as_fixed_size_binary();
                let start = bytes.value_offset(index) as usize;
                let buffer = bytes
                    .values()
                    .slice_with_length(start, bytes.value_length() as usize)
                    .clone();
                BufferArray::new_u8(buffer)
            })),

            d => todo!("{d} is not implemented"),
        }
    }
}

struct StringArrayIter<'a, 'b, T: OffsetSizeTrait> {
    length: usize,
    nulls: Option<&'b NullBuffer>,
    offsets: &'b [T],
    values: &'b Buffer,
    marker: PhantomData<&'a T>,
    current: usize,
}

impl<'a, 'b, T: OffsetSizeTrait> StringArrayIter<'a, 'b, T> {
    pub fn new(values: &'b GenericByteArray<GenericStringType<T>>) -> StringArrayIter<'a, 'b, T> {
        Self {
            length: values.len(),
            nulls: values.nulls(),
            offsets: values.offsets(),
            values: values.values(),
            marker: Default::default(),
            current: 0,
        }
    }
}

impl<'a, T: OffsetSizeTrait> Iterator for StringArrayIter<'a, '_, T> {
    type Item = ValueOrRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.current;

        if current >= self.length {
            return None;
        }

        let ret = if let Some(nulls) = self.nulls
            && nulls.is_null(current)
        {
            ValueOrRef::Null
        } else {
            let offsets = self.offsets;
            let end = T::as_usize(unsafe { *offsets.get_unchecked(current + 1) });
            let start = T::as_usize(unsafe { *offsets.get_unchecked(current) });
            ValueOrRef::String(StringValueOrRef::Buffer(
                self.values.slice_with_length(start, end - start),
            ))
        };

        self.current = current + 1;

        Some(ret)
    }
}

impl<T: OffsetSizeTrait> ExactSizeIterator for StringArrayIter<'_, '_, T> {
    fn len(&self) -> usize {
        self.length
    }
}

struct FixedSizeBinaryArrayIter<'a, 'b> {
    values: &'b FixedSizeBinaryArray,
    marker: PhantomData<&'a FixedSizeBinaryArray>,
    current: usize,
}

impl<'a, 'b> FixedSizeBinaryArrayIter<'a, 'b> {
    pub fn new(values: &'b FixedSizeBinaryArray) -> FixedSizeBinaryArrayIter<'a, 'b> {
        Self {
            values,
            marker: Default::default(),
            current: 0,
        }
    }
}

impl<'a> Iterator for FixedSizeBinaryArrayIter<'a, '_> {
    type Item = ValueOrRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let values = self.values;

        let current = self.current;

        if current >= values.len() {
            return None;
        }

        let ret = if let Some(nulls) = values.nulls()
            && nulls.is_null(current)
        {
            ValueOrRef::Null
        } else {
            let start = values.value_offset(current) as usize;
            let buffer = values
                .values()
                .slice_with_length(start, values.value_length() as usize)
                .clone();
            ValueOrRef::Array(ArrayValueOrRef::Buffer(BufferArray::new_u8(buffer)))
        };

        self.current = current + 1;

        Some(ret)
    }
}

impl ExactSizeIterator for FixedSizeBinaryArrayIter<'_, '_> {
    fn len(&self) -> usize {
        self.values.len()
    }
}

pub trait ArrowArraySetTransformer {
    fn to_set<V: Hash + Eq, FTransform>(&self, transform: FTransform) -> SetWithLookup<V>
    where
        FTransform: FnMut(Option<Buffer>) -> Option<V>;
}

impl<T: OffsetSizeTrait> ArrowArraySetTransformer for GenericBinaryArray<T> {
    fn to_set<V: Hash + Eq, FTransform>(&self, mut transform: FTransform) -> SetWithLookup<V>
    where
        FTransform: FnMut(Option<Buffer>) -> Option<V>,
    {
        let offsets = self.value_offsets();
        let values = self.values();
        let null_value = transform(None);

        if let Some(nulls) = self.nulls() {
            let mut previous_offset = 0;
            let i = offsets.iter().enumerate().map(|(index, end)| {
                let end = T::as_usize(*end);
                let r = if nulls.is_null(index) {
                    None
                } else {
                    let buffer = values.slice_with_length(previous_offset, end);
                    transform(Some(buffer))
                };
                previous_offset = end;
                r
            });
            iter_into_set(i, null_value)
        } else {
            let mut previous_offset = 0;
            let i = offsets.iter().map(|end| {
                let end = T::as_usize(*end);
                let buffer = values.slice_with_length(previous_offset, end);
                previous_offset = end;
                transform(Some(buffer))
            });
            iter_into_set(i, null_value)
        }
    }
}
