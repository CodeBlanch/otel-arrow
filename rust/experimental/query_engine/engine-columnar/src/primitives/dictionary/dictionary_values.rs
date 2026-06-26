// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{hash::Hash, marker::PhantomData, rc::Rc, sync::Arc};

use ahash::{AHashMap, RandomState};
use arrow::{
    array::*,
    buffer::{Buffer, MutableBuffer, NullBuffer},
    datatypes::*,
};
use chrono::{TimeZone, Utc};
use data_engine_expressions::{ArrayValue, AsValue, IndexValueClosureCallback};
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

    pub fn transform_into_string_array(
        self,
    ) -> (StringArray, Option<AHashMap<usize, Option<usize>>>) {
        self.transform_into_array(
            ArrayRef::as_string_opt,
            |v| Some(v.to_string()),
            StringArray::from_iter_values,
        )
    }

    pub fn transform_into_int_array<T: ArrowPrimitiveType>(
        self,
    ) -> (PrimitiveArray<T>, Option<AHashMap<usize, Option<usize>>>)
    where
        T::Native: Hash + Eq + TryFrom<i64>,
        PrimitiveArray<T>: From<Vec<<T as ArrowPrimitiveType>::Native>>,
    {
        self.transform_into_array(
            ArrayRef::as_primitive_opt::<T>,
            |v| v.to_int::<T::Native>(),
            PrimitiveArray::<T>::from,
        )
    }

    pub fn transform_into_timestamp_nanoseconds_array(
        self,
    ) -> (
        PrimitiveArray<TimestampNanosecondType>,
        Option<AHashMap<usize, Option<usize>>>,
    ) {
        self.transform_into_array(
            ArrayRef::as_primitive_opt::<TimestampNanosecondType>,
            |v| {
                v.to_value()
                    .convert_to_datetime()
                    .and_then(|v| v.timestamp_nanos_opt())
            },
            PrimitiveArray::<TimestampNanosecondType>::from,
        )
    }

    pub fn transform_into_fixed_sized_binary_array<const SIZE: usize>(
        self,
    ) -> (FixedSizeBinaryArray, Option<AHashMap<usize, Option<usize>>>) {
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
                        Some(values)
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
                if v.is_empty() {
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

    pub fn into_set(self) -> (ValueOrRefSet<'a>, Option<AHashMap<usize, Option<usize>>>) {
        let t = &mut |v| {
            if matches!(v, ValueOrRef::Null) {
                None
            } else {
                Some(v)
            }
        };

        let (set, lookup) = match self {
            DictionaryValueArray::Array(a) => transform_array_into_set(t, a),
            DictionaryValueArray::Vec(a) => {
                transform_iter_into_set(t, a.len(), Rc::unwrap_or_clone(a).into_iter().enumerate())
            }
            DictionaryValueArray::Set(a) => {
                return (Rc::unwrap_or_clone(a), None);
            }
            DictionaryValueArray::Boolean => {
                let mut set = ValueOrRefSet::with_capacity_and_hasher(2, RandomState::new());
                set.insert(ValueOrRef::Boolean(false));
                set.insert(ValueOrRef::Boolean(true));
                return (set, None);
            }
        };

        (set, Some(lookup))
    }

    pub fn transform_into_set<T: Hash + Eq, FTransform>(
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
            DictionaryValueArray::Boolean => transform_iter_into_set(
                transform,
                2,
                [ValueOrRef::Boolean(false), ValueOrRef::Boolean(true)]
                    .into_iter()
                    .enumerate(),
            ),
        }
    }

    pub fn transform_into_vec<T, FTransform>(self, mut transform: &mut FTransform) -> Vec<Option<T>>
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

    pub fn transform_into_array<TArray: Array + Clone, T: Hash + Eq, FAsArray, FConvert, FBuild>(
        self,
        as_array: FAsArray,
        convert: FConvert,
        build: FBuild,
    ) -> (TArray, Option<AHashMap<usize, Option<usize>>>)
    where
        FAsArray: Fn(&Arc<dyn Array>) -> Option<&TArray>,
        FConvert: Fn(ValueOrRef<'a>) -> Option<T>,
        FBuild: Fn(Vec<T>) -> TArray,
    {
        match self {
            DictionaryValueArray::Array(a) => {
                if let Some(s) = as_array(&a) {
                    (s.clone(), None)
                } else {
                    let (values, lookup) = transform_array_into_set(&mut |v| convert(v), a);

                    (build(values.into_iter().collect::<Vec<_>>()), Some(lookup))
                }
            }
            DictionaryValueArray::Vec(a) => {
                let length = a.len();
                let values = Rc::unwrap_or_clone(a).into_iter();

                transform_iter_into_array(length, values, build, convert)
            }
            DictionaryValueArray::Set(a) => {
                let length = a.len();
                let values = Rc::unwrap_or_clone(a).into_iter();

                transform_iter_into_array(length, values, build, convert)
            }
            DictionaryValueArray::Boolean => (
                build(vec![
                    convert(ValueOrRef::Boolean(false)).expect("false value"),
                    convert(ValueOrRef::Boolean(true)).expect("true value"),
                ]),
                None,
            ),
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
    transform: FTransform,
) -> (TOutput, Option<AHashMap<usize, Option<usize>>>)
where
    FBuild: Fn(Vec<TInput>) -> TOutput,
    FTransform: Fn(ValueOrRef<'a>) -> Option<TInput>,
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

    (build(set.into_iter().collect::<Vec<_>>()), Some(lookup))
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
