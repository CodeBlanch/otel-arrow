// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::hash::Hash;

use ahash::{AHashMap, RandomState};
use arrow::{array::*, datatypes::*};
use data_engine_expressions::*;
use indexmap::IndexSet;

use crate::*;

pub type ValueOrRefSet<'a> = IndexSet<ValueOrRef<'a>, RandomState>;

#[derive(Debug, Clone, PartialEq)]
pub enum DictionaryValueArray<'a> {
    ArrayRef(&'a dyn Array),
    VecAnyOwned(Vec<ValueOrRef<'a>>),
    IndexAnyOwned(ValueOrRefSet<'a>),
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

    pub fn get_value_at(&self, index: usize) -> Option<ValueOrRef<'a>> {
        match self {
            DictionaryValueArray::ArrayRef(a) => get_value_from_array(*a, index),
            DictionaryValueArray::VecAnyOwned(a) => Some(a[index].clone()),
            DictionaryValueArray::IndexAnyOwned(a) => a.get_index(index).cloned(),
            DictionaryValueArray::Boolean => Some(ValueOrRef::Boolean(index != 0)),
        }
    }

    pub(crate) fn validate<FValidate>(&self, mut validate: FValidate) -> Result<(), ExpressionError>
    where
        FValidate: FnMut(Option<&ValueOrRef<'a>>) -> Result<(), ExpressionError>,
    {
        match self {
            DictionaryValueArray::ArrayRef(a) => validate_array(*a, validate),
            DictionaryValueArray::VecAnyOwned(a) => {
                for i in a {
                    validate(Some(i))?;
                }

                Ok(())
            }
            DictionaryValueArray::IndexAnyOwned(a) => {
                for i in a {
                    validate(Some(i))?;
                }

                Ok(())
            }
            DictionaryValueArray::Boolean => {
                validate(Some(&ValueOrRef::Boolean(false)))?;
                validate(Some(&ValueOrRef::Boolean(true)))
            }
        }
    }

    pub(crate) fn transform_into_set<T: Hash + Eq, FTransform>(
        self,
        transform: &mut FTransform,
    ) -> Result<(IndexSet<T, RandomState>, AHashMap<usize, Option<usize>>), ExpressionError>
    where
        FTransform: FnMut(Option<ValueOrRef<'a>>) -> Result<Option<T>, ExpressionError>,
    {
        match self {
            DictionaryValueArray::ArrayRef(a) => transform_array_into_set(transform, a),
            DictionaryValueArray::VecAnyOwned(a) => {
                transform_iter_into_set(transform, a.len(), a.into_iter().enumerate())
            }
            DictionaryValueArray::IndexAnyOwned(a) => {
                transform_iter_into_set(transform, a.len(), a.into_iter().enumerate())
            }
            DictionaryValueArray::Boolean => todo!(),
        }
    }

    pub(crate) fn transform_into_vec<T, FTransform>(
        self,
        transform: &mut FTransform,
    ) -> Result<Vec<Option<T>>, ExpressionError>
    where
        FTransform: FnMut(Option<ValueOrRef<'a>>) -> Result<Option<T>, ExpressionError>,
    {
        Ok(match self {
            DictionaryValueArray::ArrayRef(a) => transform_array_into_vec(transform, a)?,
            DictionaryValueArray::VecAnyOwned(a) => {
                a.into_iter()
                    .map(|v| transform(Some(v)))
                    .collect::<Result<Vec<Option<T>>, ExpressionError>>()?
            }
            DictionaryValueArray::IndexAnyOwned(a) => {
                a.into_iter()
                    .map(|v| transform(Some(v)))
                    .collect::<Result<Vec<Option<T>>, ExpressionError>>()?
            }
            DictionaryValueArray::Boolean => {
                vec![
                    transform(Some(ValueOrRef::Boolean(false)))?,
                    transform(Some(ValueOrRef::Boolean(true)))?,
                ]
            }
        })
    }
}

impl<'a, T: Array + 'a> From<&'a T> for DictionaryValueArray<'a> {
    fn from(value: &'a T) -> DictionaryValueArray<'a> {
        DictionaryValueArray::ArrayRef(value)
    }
}

impl<'a> From<ValueOrRefSet<'a>> for DictionaryValueArray<'a> {
    fn from(value: ValueOrRefSet<'a>) -> DictionaryValueArray<'a> {
        DictionaryValueArray::IndexAnyOwned(value)
    }
}

impl<'a> From<Vec<ValueOrRef<'a>>> for DictionaryValueArray<'a> {
    fn from(value: Vec<ValueOrRef<'a>>) -> DictionaryValueArray<'a> {
        DictionaryValueArray::VecAnyOwned(value)
    }
}

fn transform_array_into_vec<'a, T, FTransform>(
    mut transform: FTransform,
    value: &'a dyn Array,
) -> Result<Vec<Option<T>>, ExpressionError>
where
    FTransform: FnMut(Option<ValueOrRef<'a>>) -> Result<Option<T>, ExpressionError>,
{
    Ok(match value.data_type() {
        DataType::Int8 => value
            .as_primitive::<Int8Type>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::Integer(v as i64))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,
        DataType::Int16 => value
            .as_primitive::<Int16Type>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::Integer(v as i64))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,
        DataType::Int32 => value
            .as_primitive::<Int32Type>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::Integer(v as i64))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,
        DataType::Int64 => value
            .as_primitive::<Int64Type>()
            .into_iter()
            .map(|v| transform(v.map(ValueOrRef::Integer)))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,

        DataType::UInt8 => value
            .as_primitive::<UInt8Type>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::Integer(v as i64))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,
        DataType::UInt16 => value
            .as_primitive::<UInt16Type>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::Integer(v as i64))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,
        DataType::UInt32 => value
            .as_primitive::<UInt32Type>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::Integer(v as i64))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,
        DataType::UInt64 => value
            .as_primitive::<UInt64Type>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::Integer(v as i64))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,

        DataType::Float16 => value
            .as_primitive::<Float16Type>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::Double(f64::from(v)))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,
        DataType::Float32 => value
            .as_primitive::<Float32Type>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::Double(v as f64))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,
        DataType::Float64 => value
            .as_primitive::<Float64Type>()
            .into_iter()
            .map(|v| transform(v.map(ValueOrRef::Double)))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,

        DataType::Utf8 => value
            .as_string::<i32>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::String(StringValueOrRef::Ref(v)))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,
        DataType::LargeUtf8 => value
            .as_string::<i64>()
            .into_iter()
            .map(|v| transform(v.map(|v| ValueOrRef::String(StringValueOrRef::Ref(v)))))
            .collect::<Result<Vec<Option<T>>, ExpressionError>>()?,

        _ => todo!(),
    })
}

fn transform_array_into_set<'a, T: Hash + Eq, FTransform>(
    transform: &mut FTransform,
    value: &'a dyn Array,
) -> Result<(IndexSet<T, RandomState>, AHashMap<usize, Option<usize>>), ExpressionError>
where
    FTransform: FnMut(Option<ValueOrRef<'a>>) -> Result<Option<T>, ExpressionError>,
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
) -> Result<(IndexSet<T, RandomState>, AHashMap<usize, Option<usize>>), ExpressionError>
where
    FTransform: FnMut(Option<ValueOrRef<'a>>) -> Result<Option<T>, ExpressionError>,
    I: Iterator<Item = (usize, ValueOrRef<'a>)>,
{
    let mut value_index_lookup = AHashMap::with_capacity(max_length);
    let mut transformed_values = IndexSet::with_capacity_and_hasher(max_length, RandomState::new());

    for (index, value) in iter {
        if let Some(transformed_value) = transform(Some(value))? {
            let (transformed_index, _) = transformed_values.insert_full(transformed_value);
            value_index_lookup.insert(index, Some(transformed_index));
        } else {
            value_index_lookup.insert(index, None);
        }
    }

    Ok((transformed_values, value_index_lookup))
}

pub(crate) fn get_value_from_array(value: &dyn Array, index: usize) -> Option<ValueOrRef<'_>> {
    if let Some(nulls) = value.nulls()
        && nulls.is_null(index)
    {
        return None;
    }

    unsafe {
        match value.data_type() {
            DataType::Int8 => Some(ValueOrRef::Integer(
                *value
                    .as_primitive::<Int8Type>()
                    .values()
                    .get_unchecked(index) as i64,
            )),
            DataType::Int16 => Some(ValueOrRef::Integer(
                *value
                    .as_primitive::<Int16Type>()
                    .values()
                    .get_unchecked(index) as i64,
            )),
            DataType::Int32 => Some(ValueOrRef::Integer(
                *value
                    .as_primitive::<Int32Type>()
                    .values()
                    .get_unchecked(index) as i64,
            )),
            DataType::Int64 => Some(ValueOrRef::Integer(
                *value
                    .as_primitive::<Int64Type>()
                    .values()
                    .get_unchecked(index),
            )),

            DataType::UInt8 => Some(ValueOrRef::Integer(
                *value
                    .as_primitive::<UInt8Type>()
                    .values()
                    .get_unchecked(index) as i64,
            )),
            DataType::UInt16 => Some(ValueOrRef::Integer(
                *value
                    .as_primitive::<UInt16Type>()
                    .values()
                    .get_unchecked(index) as i64,
            )),
            DataType::UInt32 => Some(ValueOrRef::Integer(
                *value
                    .as_primitive::<UInt32Type>()
                    .values()
                    .get_unchecked(index) as i64,
            )),
            DataType::UInt64 => Some(ValueOrRef::Integer(
                *value
                    .as_primitive::<UInt64Type>()
                    .values()
                    .get_unchecked(index) as i64,
            )),

            DataType::Float16 => Some(ValueOrRef::Double(
                (*value
                    .as_primitive::<Float16Type>()
                    .values()
                    .get_unchecked(index))
                .into(),
            )),
            DataType::Float32 => Some(ValueOrRef::Double(
                *value
                    .as_primitive::<Float32Type>()
                    .values()
                    .get_unchecked(index) as f64,
            )),
            DataType::Float64 => Some(ValueOrRef::Double(
                *value
                    .as_primitive::<Float64Type>()
                    .values()
                    .get_unchecked(index),
            )),

            DataType::Utf8 => Some(ValueOrRef::String(StringValueOrRef::Ref(
                value.as_string::<i32>().value_unchecked(index),
            ))),
            DataType::LargeUtf8 => Some(ValueOrRef::String(StringValueOrRef::Ref(
                value.as_string::<i64>().value_unchecked(index),
            ))),

            _ => todo!(),
        }
    }
}

fn validate_array<'a, FValidate>(
    value: &'a dyn Array,
    mut validate: FValidate,
) -> Result<(), ExpressionError>
where
    FValidate: FnMut(Option<&ValueOrRef<'a>>) -> Result<(), ExpressionError>,
{
    match value.data_type() {
        DataType::Int8 => {
            for v in value.as_primitive::<Int8Type>().into_iter() {
                validate(v.map(|v| ValueOrRef::Integer(v as i64)).as_ref())?;
            }

            Ok(())
        }
        DataType::Int16 => {
            for v in value.as_primitive::<Int16Type>().into_iter() {
                validate(v.map(|v| ValueOrRef::Integer(v as i64)).as_ref())?;
            }

            Ok(())
        }
        DataType::Int32 => {
            for v in value.as_primitive::<Int32Type>().into_iter() {
                validate(v.map(|v| ValueOrRef::Integer(v as i64)).as_ref())?;
            }

            Ok(())
        }
        DataType::Int64 => {
            for v in value.as_primitive::<Int64Type>().into_iter() {
                validate(v.map(ValueOrRef::Integer).as_ref())?;
            }

            Ok(())
        }

        DataType::UInt8 => {
            for v in value.as_primitive::<UInt8Type>().into_iter() {
                validate(v.map(|v| ValueOrRef::Integer(v as i64)).as_ref())?;
            }

            Ok(())
        }
        DataType::UInt16 => {
            for v in value.as_primitive::<UInt16Type>().into_iter() {
                validate(v.map(|v| ValueOrRef::Integer(v as i64)).as_ref())?;
            }

            Ok(())
        }
        DataType::UInt32 => {
            for v in value.as_primitive::<UInt32Type>().into_iter() {
                validate(v.map(|v| ValueOrRef::Integer(v as i64)).as_ref())?;
            }

            Ok(())
        }
        DataType::UInt64 => {
            for v in value.as_primitive::<UInt64Type>().into_iter() {
                validate(v.map(|v| ValueOrRef::Integer(v as i64)).as_ref())?;
            }

            Ok(())
        }

        DataType::Float16 => {
            for v in value.as_primitive::<Float16Type>().into_iter() {
                validate(v.map(|v| ValueOrRef::Double(v.into())).as_ref())?;
            }

            Ok(())
        }
        DataType::Float32 => {
            for v in value.as_primitive::<Float32Type>().into_iter() {
                validate(v.map(|v| ValueOrRef::Double(v as f64)).as_ref())?;
            }

            Ok(())
        }
        DataType::Float64 => {
            for v in value.as_primitive::<Float64Type>().into_iter() {
                validate(v.map(ValueOrRef::Double).as_ref())?;
            }

            Ok(())
        }

        DataType::Utf8 => {
            for v in value.as_string::<i32>().into_iter() {
                validate(
                    v.map(|v| ValueOrRef::String(StringValueOrRef::Ref(v)))
                        .as_ref(),
                )?;
            }

            Ok(())
        }
        DataType::LargeUtf8 => {
            for v in value.as_string::<i64>().into_iter() {
                validate(
                    v.map(|v| ValueOrRef::String(StringValueOrRef::Ref(v)))
                        .as_ref(),
                )?;
            }

            Ok(())
        }

        _ => todo!(),
    }
}
