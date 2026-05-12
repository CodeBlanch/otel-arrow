// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{hash::Hash, marker::PhantomData, rc::Rc, sync::Arc};

use ahash::{AHashMap, RandomState};
use arrow::{
    array::*,
    buffer::{Buffer, NullBuffer},
    datatypes::*,
};
use chrono::{TimeZone, Utc};
use data_engine_expressions::StringValue;
use indexmap::IndexSet;

use crate::*;

pub type ValueOrRefSet<'a> = IndexSet<ValueOrRef<'a>, RandomState>;

#[derive(Debug, Clone)]
pub enum DictionaryValueArray<'a> {
    Array(Arc<dyn Array>),
    Vec(Rc<Vec<ValueOrRef<'a>>>),
    Set(Rc<ValueOrRefSet<'a>>),
    Boolean,
}

impl<'a> DictionaryValueArray<'a> {
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

    pub fn get_value_at(&self, index: usize) -> ValueOrRef<'a> {
        match self {
            DictionaryValueArray::Array(a) => get_value_from_array(a, index),
            DictionaryValueArray::Vec(a) => a.get(index).cloned().unwrap_or(ValueOrRef::Null),
            DictionaryValueArray::Set(a) => a.get_index(index).cloned().unwrap_or(ValueOrRef::Null),
            DictionaryValueArray::Boolean => ValueOrRef::Boolean(index != 0),
        }
    }

    pub(crate) fn transform_into_set<T: Hash + Eq, FTransform>(
        self,
        transform: &mut FTransform,
    ) -> (IndexSet<T, RandomState>, AHashMap<usize, Option<usize>>)
    where
        FTransform: FnMut(ValueOrRef<'a>) -> Option<T>,
    {
        match self {
            DictionaryValueArray::Array(a) => transform_array_into_set(transform, a),
            DictionaryValueArray::Vec(a) => transform_iter_into_set(
                transform,
                a.len(),
                Rc::unwrap_or_clone(a).into_iter().enumerate(),
            ),
            DictionaryValueArray::Set(a) => transform_iter_into_set(
                transform,
                a.len(),
                Rc::unwrap_or_clone(a).into_iter().enumerate(),
            ),
            DictionaryValueArray::Boolean => todo!(),
        }
    }

    pub(crate) fn transform_into_vec<T, FTransform>(
        self,
        mut transform: &mut FTransform,
    ) -> Vec<Option<T>>
    where
        FTransform: FnMut(ValueOrRef<'a>) -> Option<T>,
    {
        match self {
            DictionaryValueArray::Array(a) => transform_array_into_vec(transform, a),
            DictionaryValueArray::Vec(a) => Rc::unwrap_or_clone(a)
                .into_iter()
                .map(&mut transform)
                .collect(),
            DictionaryValueArray::Set(a) => Rc::unwrap_or_clone(a)
                .into_iter()
                .map(&mut transform)
                .collect(),
            DictionaryValueArray::Boolean => {
                vec![
                    transform(ValueOrRef::Boolean(false)),
                    transform(ValueOrRef::Boolean(true)),
                ]
            }
        }
    }

    pub fn transform_into_string_array(
        self,
    ) -> (StringArray, Option<AHashMap<usize, Option<usize>>>) {
        match self {
            DictionaryValueArray::Array(a) => {
                if let Some(s) = a.as_string_opt() {
                    (s.clone(), None)
                } else {
                    let (values, lookup) = transform_array_into_set(
                        &mut |v| Some(Into::<StringValueOrRef>::into(v)),
                        a,
                    );

                    (
                        StringArray::from(
                            values
                                .iter()
                                .map(|v: &StringValueOrRef<'_>| v.get_value())
                                .collect::<Vec<_>>(),
                        ),
                        Some(lookup),
                    )
                }
            }
            DictionaryValueArray::Vec(a) => {
                let length = a.len();
                let values = Rc::unwrap_or_clone(a).into_iter();

                transform_iter_into_string_array(length, values)
            }
            DictionaryValueArray::Set(a) => {
                let length = a.len();
                let values = Rc::unwrap_or_clone(a).into_iter();

                transform_iter_into_string_array(length, values)
            }
            DictionaryValueArray::Boolean => (StringArray::from(vec!["false", "true"]), None),
        }
    }

    pub fn transform_into_int_32_array(
        self,
    ) -> (
        PrimitiveArray<Int32Type>,
        Option<AHashMap<usize, Option<usize>>>,
    ) {
        match self {
            DictionaryValueArray::Array(a) => {
                if let Some(s) = a.as_primitive_opt::<Int32Type>() {
                    (s.clone(), None)
                } else {
                    let (values, lookup) = transform_array_into_set(&mut |v| v.to_int_32(), a);

                    (
                        PrimitiveArray::<Int32Type>::from(values.into_iter().collect::<Vec<_>>()),
                        Some(lookup),
                    )
                }
            }
            DictionaryValueArray::Vec(a) => {
                let length = a.len();
                let values = Rc::unwrap_or_clone(a).into_iter();

                transform_iter_into_int_32_array(length, values)
            }
            DictionaryValueArray::Set(a) => {
                let length = a.len();
                let values = Rc::unwrap_or_clone(a).into_iter();

                transform_iter_into_int_32_array(length, values)
            }
            DictionaryValueArray::Boolean => (PrimitiveArray::<Int32Type>::from(vec![0, 1]), None),
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

fn transform_array_into_vec<'a, T, FTransform>(
    mut transform: FTransform,
    value: Arc<dyn Array>,
) -> Vec<Option<T>>
where
    FTransform: FnMut(ValueOrRef<'a>) -> Option<T>,
{
    match value.data_type() {
        DataType::Int8 => value
            .as_primitive::<Int8Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))))
            .collect(),
        DataType::Int16 => value
            .as_primitive::<Int16Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))))
            .collect(),
        DataType::Int32 => value
            .as_primitive::<Int32Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))))
            .collect(),
        DataType::Int64 => value
            .as_primitive::<Int64Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, ValueOrRef::Integer)))
            .collect(),

        DataType::UInt8 => value
            .as_primitive::<UInt8Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))))
            .collect(),
        DataType::UInt16 => value
            .as_primitive::<UInt16Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))))
            .collect(),
        DataType::UInt32 => value
            .as_primitive::<UInt32Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))))
            .collect(),
        DataType::UInt64 => value
            .as_primitive::<UInt64Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, |v| ValueOrRef::Integer(v as i64))))
            .collect(),

        DataType::Float16 => value
            .as_primitive::<Float16Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, |v| ValueOrRef::Double(f64::from(v)))))
            .collect(),
        DataType::Float32 => value
            .as_primitive::<Float32Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, |v| ValueOrRef::Double(v as f64))))
            .collect(),
        DataType::Float64 => value
            .as_primitive::<Float64Type>()
            .into_iter()
            .map(|v| transform(v.map_or(ValueOrRef::Null, ValueOrRef::Double)))
            .collect(),

        DataType::Utf8 => StringArrayIter::new(value.as_string::<i32>())
            .map(transform)
            .collect(),
        DataType::LargeUtf8 => StringArrayIter::new(value.as_string::<i64>())
            .map(transform)
            .collect(),

        DataType::Timestamp(time_unit, _) => match time_unit {
            TimeUnit::Second => value
                .as_primitive::<TimestampSecondType>()
                .into_iter()
                .map(|v| {
                    transform(v.map_or(ValueOrRef::Null, |secs| {
                        ValueOrRef::DateTime(Utc.timestamp_opt(secs, 0).unwrap().into())
                    }))
                })
                .collect(),
            TimeUnit::Millisecond => value
                .as_primitive::<TimestampMillisecondType>()
                .into_iter()
                .map(|v| {
                    transform(v.map_or(ValueOrRef::Null, |millis| {
                        ValueOrRef::DateTime(Utc.timestamp_millis_opt(millis).unwrap().into())
                    }))
                })
                .collect(),
            TimeUnit::Microsecond => value
                .as_primitive::<TimestampMicrosecondType>()
                .into_iter()
                .map(|v| {
                    transform(v.map_or(ValueOrRef::Null, |micros| {
                        ValueOrRef::DateTime(Utc.timestamp_micros(micros).unwrap().into())
                    }))
                })
                .collect(),
            TimeUnit::Nanosecond => value
                .as_primitive::<TimestampNanosecondType>()
                .into_iter()
                .map(|v| {
                    transform(v.map_or(ValueOrRef::Null, |nanos| {
                        ValueOrRef::DateTime(Utc.timestamp_nanos(nanos).into())
                    }))
                })
                .collect(),
        },

        DataType::FixedSizeBinary(_) => FixedSizeBinaryArrayIter::new(value.as_fixed_size_binary())
            .map(transform)
            .collect(),

        d => todo!("{d} is not implemented"),
    }
}

fn transform_array_into_set<'a, T: Hash + Eq, FTransform>(
    transform: &mut FTransform,
    value: Arc<dyn Array>,
) -> (IndexSet<T, RandomState>, AHashMap<usize, Option<usize>>)
where
    FTransform: FnMut(ValueOrRef<'a>) -> Option<T>,
{
    match value.data_type() {
        DataType::Int8 => {
            let a = value.as_primitive::<Int8Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Integer(v as i64)))),
            )
        }
        DataType::Int16 => {
            let a = value.as_primitive::<Int16Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Integer(v as i64)))),
            )
        }
        DataType::Int32 => {
            let a = value.as_primitive::<Int32Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Integer(v as i64)))),
            )
        }
        DataType::Int64 => {
            let a = value.as_primitive::<Int64Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Integer(v)))),
            )
        }

        DataType::UInt8 => {
            let a = value.as_primitive::<UInt8Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Integer(v as i64)))),
            )
        }
        DataType::UInt16 => {
            let a = value.as_primitive::<UInt16Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Integer(v as i64)))),
            )
        }
        DataType::UInt32 => {
            let a = value.as_primitive::<UInt32Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Integer(v as i64)))),
            )
        }
        DataType::UInt64 => {
            let a = value.as_primitive::<UInt64Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Integer(v as i64)))),
            )
        }

        DataType::Float16 => {
            let a = value.as_primitive::<Float16Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Double(f64::from(v))))),
            )
        }
        DataType::Float32 => {
            let a = value.as_primitive::<Float32Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Double(v as f64)))),
            )
        }
        DataType::Float64 => {
            let a = value.as_primitive::<Float64Type>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate()
                    .filter_map(|(i, v)| v.map(|v| (i, ValueOrRef::Double(v)))),
            )
        }

        DataType::Utf8 => transform_iter_into_set(
            transform,
            value.len(),
            StringArrayIter::new(value.as_string::<i32>()).enumerate(),
        ),
        DataType::LargeUtf8 => transform_iter_into_set(
            transform,
            value.len(),
            StringArrayIter::new(value.as_string::<i64>()).enumerate(),
        ),

        DataType::Timestamp(time_unit, _) => match time_unit {
            TimeUnit::Second => {
                let a = value.as_primitive::<TimestampSecondType>().into_iter();

                transform_iter_into_set(
                    transform,
                    a.len(),
                    a.enumerate().filter_map(|(i, v)| {
                        v.map(|secs| {
                            (
                                i,
                                ValueOrRef::DateTime(Utc.timestamp_opt(secs, 0).unwrap().into()),
                            )
                        })
                    }),
                )
            }
            TimeUnit::Millisecond => {
                let a = value.as_primitive::<TimestampMillisecondType>().into_iter();

                transform_iter_into_set(
                    transform,
                    a.len(),
                    a.enumerate().filter_map(|(i, v)| {
                        v.map(|millis| {
                            (
                                i,
                                ValueOrRef::DateTime(
                                    Utc.timestamp_millis_opt(millis).unwrap().into(),
                                ),
                            )
                        })
                    }),
                )
            }
            TimeUnit::Microsecond => {
                let a = value.as_primitive::<TimestampMicrosecondType>().into_iter();

                transform_iter_into_set(
                    transform,
                    a.len(),
                    a.enumerate().filter_map(|(i, v)| {
                        v.map(|micros| {
                            (
                                i,
                                ValueOrRef::DateTime(Utc.timestamp_micros(micros).unwrap().into()),
                            )
                        })
                    }),
                )
            }
            TimeUnit::Nanosecond => {
                let a = value.as_primitive::<TimestampNanosecondType>().into_iter();

                transform_iter_into_set(
                    transform,
                    a.len(),
                    a.enumerate().filter_map(|(i, v)| {
                        v.map(|nanos| (i, ValueOrRef::DateTime(Utc.timestamp_nanos(nanos).into())))
                    }),
                )
            }
        },

        DataType::FixedSizeBinary(_) => transform_iter_into_set(
            transform,
            value.len(),
            FixedSizeBinaryArrayIter::new(value.as_fixed_size_binary()).enumerate(),
        ),

        d => todo!("{d} is not implemented"),
    }
}

fn transform_iter_into_set<'a, T: Hash + Eq, FTransform, I>(
    transform: &mut FTransform,
    max_length: usize,
    iter: I,
) -> (IndexSet<T, RandomState>, AHashMap<usize, Option<usize>>)
where
    FTransform: FnMut(ValueOrRef<'a>) -> Option<T>,
    I: Iterator<Item = (usize, ValueOrRef<'a>)>,
{
    let mut value_index_lookup = AHashMap::with_capacity(max_length);
    let mut transformed_values = IndexSet::with_capacity_and_hasher(max_length, RandomState::new());

    for (index, value) in iter {
        if let Some(transformed_value) = transform(value) {
            let (transformed_index, _) = transformed_values.insert_full(transformed_value);
            value_index_lookup.insert(index, Some(transformed_index));
        } else {
            value_index_lookup.insert(index, None);
        }
    }

    (transformed_values, value_index_lookup)
}

fn transform_iter_into_string_array<'a, T: Iterator<Item = ValueOrRef<'a>>>(
    length: usize,
    values: T,
) -> (StringArray, Option<AHashMap<usize, Option<usize>>>) {
    transform_iter_into_array(
        length,
        values,
        |set| {
            StringArray::from(
                set.iter()
                    .map(|v: &StringValueOrRef<'_>| v.get_value())
                    .collect::<Vec<_>>(),
            )
        },
        |value| Some(value.into()),
    )
}

fn transform_iter_into_int_32_array<'a, T: Iterator<Item = ValueOrRef<'a>>>(
    length: usize,
    values: T,
) -> (
    PrimitiveArray<Int32Type>,
    Option<AHashMap<usize, Option<usize>>>,
) {
    transform_iter_into_array(
        length,
        values,
        |set| PrimitiveArray::<Int32Type>::from(set.into_iter().collect::<Vec<_>>()),
        |value| value.to_int_32(),
    )
}

fn transform_iter_into_array<
    'a,
    TItems: Iterator<Item = ValueOrRef<'a>>,
    TInput: Hash + PartialEq + Eq,
    TOutput: Array,
    FBuild,
    FTransform,
>(
    length: usize,
    values: TItems,
    build: FBuild,
    mut transform: FTransform,
) -> (TOutput, Option<AHashMap<usize, Option<usize>>>)
where
    FBuild: FnOnce(IndexSet<TInput, RandomState>) -> TOutput,
    FTransform: FnMut(ValueOrRef<'a>) -> Option<TInput>,
{
    let mut lookup = AHashMap::with_capacity(length);
    let mut set = IndexSet::with_capacity_and_hasher(length, RandomState::new());

    for (value_index, v) in values.enumerate() {
        if let Some(v) = transform(v) {
            let (index, _) = set.insert_full(v);

            lookup.insert(value_index, Some(index));
        } else {
            lookup.insert(value_index, None);
        }
    }

    (build(set), Some(lookup))
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
