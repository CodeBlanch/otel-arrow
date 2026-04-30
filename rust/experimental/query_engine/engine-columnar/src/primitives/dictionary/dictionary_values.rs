// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{hash::Hash, sync::Arc};

use ahash::{AHashMap, RandomState};
use arrow::{array::*, datatypes::*};
use chrono::{TimeZone, Utc};
use indexmap::IndexSet;

use crate::*;

pub type ValueOrRefSet<'a> = IndexSet<ValueOrRef<'a>, RandomState>;

#[derive(Debug, Clone, PartialEq)]
pub enum DictionaryValueArray<'a> {
    ArrayRef(&'a dyn Array),
    VecAnyOwned(Arc<Vec<ValueOrRef<'a>>>),
    IndexAnyOwned(Arc<ValueOrRefSet<'a>>),
    Boolean,
}

impl<'a> DictionaryValueArray<'a> {
    pub fn len(&self) -> usize {
        match self {
            DictionaryValueArray::ArrayRef(a) => a.len(),
            DictionaryValueArray::Boolean => 2,
            DictionaryValueArray::VecAnyOwned(a) => a.len(),
            DictionaryValueArray::IndexAnyOwned(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DictionaryValueArray::ArrayRef(a) => a.is_empty(),
            DictionaryValueArray::Boolean => false,
            DictionaryValueArray::VecAnyOwned(a) => a.is_empty(),
            DictionaryValueArray::IndexAnyOwned(a) => a.is_empty(),
        }
    }

    pub fn get_value_at(&self, index: usize) -> ValueOrRef<'a> {
        match self {
            DictionaryValueArray::ArrayRef(a) => get_value_from_array(*a, index),
            DictionaryValueArray::VecAnyOwned(a) => {
                a.get(index).unwrap_or(&ValueOrRef::Null).clone()
            }
            DictionaryValueArray::IndexAnyOwned(a) => {
                a.get_index(index).cloned().unwrap_or(ValueOrRef::Null)
            }
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
            DictionaryValueArray::ArrayRef(a) => transform_array_into_set(transform, a),
            DictionaryValueArray::VecAnyOwned(a) => transform_iter_into_set(
                transform,
                a.len(),
                Arc::unwrap_or_clone(a).into_iter().enumerate(),
            ),
            DictionaryValueArray::IndexAnyOwned(a) => transform_iter_into_set(
                transform,
                a.len(),
                Arc::unwrap_or_clone(a).into_iter().enumerate(),
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
            DictionaryValueArray::ArrayRef(a) => transform_array_into_vec(transform, a),
            DictionaryValueArray::VecAnyOwned(a) => Arc::unwrap_or_clone(a)
                .into_iter()
                .map(&mut transform)
                .collect(),
            DictionaryValueArray::IndexAnyOwned(a) => Arc::unwrap_or_clone(a)
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
}

impl<'a, T: Array + 'a> From<&'a T> for DictionaryValueArray<'a> {
    fn from(value: &'a T) -> DictionaryValueArray<'a> {
        DictionaryValueArray::ArrayRef(value)
    }
}

impl<'a> From<ValueOrRefSet<'a>> for DictionaryValueArray<'a> {
    fn from(value: ValueOrRefSet<'a>) -> DictionaryValueArray<'a> {
        DictionaryValueArray::IndexAnyOwned(value.into())
    }
}

impl<'a> From<Vec<ValueOrRef<'a>>> for DictionaryValueArray<'a> {
    fn from(value: Vec<ValueOrRef<'a>>) -> DictionaryValueArray<'a> {
        DictionaryValueArray::VecAnyOwned(value.into())
    }
}

fn transform_array_into_vec<'a, T, FTransform>(
    mut transform: FTransform,
    value: &'a dyn Array,
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

        DataType::Utf8 => value
            .as_string::<i32>()
            .into_iter()
            .map(|v| {
                transform(v.map_or(ValueOrRef::Null, |v| {
                    ValueOrRef::String(StringValueOrRef::Ref(v))
                }))
            })
            .collect(),
        DataType::LargeUtf8 => value
            .as_string::<i64>()
            .into_iter()
            .map(|v| {
                transform(v.map_or(ValueOrRef::Null, |v| {
                    ValueOrRef::String(StringValueOrRef::Ref(v))
                }))
            })
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

        DataType::FixedSizeBinary(_) => value
            .as_fixed_size_binary()
            .into_iter()
            .map(|v| {
                transform(v.map_or(ValueOrRef::Null, |v| {
                    ValueOrRef::Array(ArrayValueOrRef::WrappedRef(ArrayValueWrappedRef::new_u8(v)))
                }))
            })
            .collect(),

        d => todo!("{d} is not implemented"),
    }
}

fn transform_array_into_set<'a, T: Hash + Eq, FTransform>(
    transform: &mut FTransform,
    value: &'a dyn Array,
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

        DataType::Utf8 => {
            let a = value.as_string::<i32>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate().filter_map(|(i, v)| {
                    v.map(|v| (i, ValueOrRef::String(StringValueOrRef::Ref(v))))
                }),
            )
        }
        DataType::LargeUtf8 => {
            let a = value.as_string::<i64>().into_iter();

            transform_iter_into_set(
                transform,
                a.len(),
                a.enumerate().filter_map(|(i, v)| {
                    v.map(|v| (i, ValueOrRef::String(StringValueOrRef::Ref(v))))
                }),
            )
        }

        _ => todo!(),
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

pub(crate) fn get_value_from_array(value: &dyn Array, index: usize) -> ValueOrRef<'_> {
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

            DataType::Utf8 => ValueOrRef::String(StringValueOrRef::Ref(
                value.as_string::<i32>().value_unchecked(index),
            )),
            DataType::LargeUtf8 => ValueOrRef::String(StringValueOrRef::Ref(
                value.as_string::<i64>().value_unchecked(index),
            )),

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

            DataType::FixedSizeBinary(_) => {
                let v: &[u8] = value.as_fixed_size_binary().value_unchecked(index);

                ValueOrRef::Array(ArrayValueOrRef::WrappedRef(ArrayValueWrappedRef::new_u8(v)))
            }

            d => todo!("{d} is not implemented"),
        }
    }
}
