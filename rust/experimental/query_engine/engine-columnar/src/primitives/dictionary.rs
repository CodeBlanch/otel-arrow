// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::{Display, Write},
    hash::Hash,
    sync::Arc,
};

use arrow::{array::*, datatypes::*};
use data_engine_expressions::*;
use indexmap::IndexSet;

use crate::*;

#[derive(Debug, Clone)]
pub struct Dictionary<'a> {
    keys: DictionaryKeyArray<'a>,
    values: DictionaryValueArray<'a>,
}

impl<'a> Dictionary<'a> {
    pub fn new(keys: DictionaryKeyArray<'a>, values: DictionaryValueArray<'a>) -> Dictionary<'a> {
        Self { keys, values }
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn keys(&self) -> &DictionaryKeyArray<'a> {
        &self.keys
    }

    pub fn values(&self) -> &DictionaryValueArray<'a> {
        &self.values
    }

    pub fn into_parts(self) -> (DictionaryKeyArray<'a>, DictionaryValueArray<'a>) {
        (self.keys, self.values)
    }

    pub fn get_value_index(&self, key_index: usize) -> Option<usize> {
        self.keys.get_value_index_for_key_index(key_index)
    }

    pub fn get_value(&self, key_index: usize) -> Option<ValueOrRef<'_>> {
        if let Some(value_index) = self.get_value_index(key_index) {
            return self.values.get_value_at(value_index);
        }

        None
    }

    pub(crate) fn get_len_dictionary<D: DiagnosticReceiver>(
        &self,
        diagnostic_receiver: &D,
    ) -> Result<Dictionary<'a>, ExpressionError> {
        self.transform_into_primitive::<Int64Type, _, _>(diagnostic_receiver, |d, v| {
            match v {
                Some(ValueOrRef::StringOwned(s)) => Some(s.chars().count() as i64),
                Some(ValueOrRef::StringRef(s)) => Some(s.chars().count() as i64),
                // todo: Map
                // todo: Array
                Some(v) => {
                    d.add_diagnostic_if_enabled(ColumnarEngineDiagnosticLevel::Warn, || {
                        format!(
                            "Cannot calculate the length of '{}' input",
                            v.to_value().get_value_type()
                        )
                    });
                    None
                }
                _ => None,
            }
        })
    }

    pub(crate) fn transform_into_boolean<D: DiagnosticReceiver, FTransform>(
        &self,
        diagnostic_receiver: &D,
        transform: FTransform,
    ) -> Result<BooleanArray, ExpressionError>
    where
        FTransform: Fn(&D, Option<ValueOrRef<'_>>) -> Option<bool>,
    {
        let array = self.keys.as_array();

        match array.data_type() {
            DataType::Int8 => transform_boolean_typed(
                diagnostic_receiver,
                array.as_primitive::<Int8Type>(),
                &self.values,
                transform,
            ),
            DataType::Int16 => transform_boolean_typed(
                diagnostic_receiver,
                array.as_primitive::<Int16Type>(),
                &self.values,
                transform,
            ),
            DataType::Int32 => transform_boolean_typed(
                diagnostic_receiver,
                array.as_primitive::<Int32Type>(),
                &self.values,
                transform,
            ),
            DataType::Int64 => transform_boolean_typed(
                diagnostic_receiver,
                array.as_primitive::<Int64Type>(),
                &self.values,
                transform,
            ),

            DataType::UInt8 => transform_boolean_typed(
                diagnostic_receiver,
                array.as_primitive::<UInt8Type>(),
                &self.values,
                transform,
            ),
            DataType::UInt16 => transform_boolean_typed(
                diagnostic_receiver,
                array.as_primitive::<UInt16Type>(),
                &self.values,
                transform,
            ),
            DataType::UInt32 => transform_boolean_typed(
                diagnostic_receiver,
                array.as_primitive::<UInt32Type>(),
                &self.values,
                transform,
            ),
            DataType::UInt64 => transform_boolean_typed(
                diagnostic_receiver,
                array.as_primitive::<UInt64Type>(),
                &self.values,
                transform,
            ),

            _ => panic!("Unexpected dictionary key type"),
        }
    }

    pub(crate) fn transform_into_primitive<
        V: ArrowPrimitiveType,
        D: DiagnosticReceiver,
        FTransform,
    >(
        &self,
        diagnostic_receiver: &D,
        transform: FTransform,
    ) -> Result<Dictionary<'a>, ExpressionError>
    where
        <V as ArrowPrimitiveType>::Native: Hash,
        <V as ArrowPrimitiveType>::Native: Eq,
        FTransform: Fn(&D, Option<ValueOrRef<'_>>) -> Option<V::Native>,
    {
        let array = self.keys.as_array();

        match array.data_type() {
            DataType::Int8 => transform_primitive_typed::<_, V, _, _>(
                diagnostic_receiver,
                array.as_primitive::<Int8Type>(),
                &self.values,
                transform,
            ),
            DataType::Int16 => transform_primitive_typed::<_, V, _, _>(
                diagnostic_receiver,
                array.as_primitive::<Int16Type>(),
                &self.values,
                transform,
            ),
            DataType::Int32 => transform_primitive_typed::<_, V, _, _>(
                diagnostic_receiver,
                array.as_primitive::<Int32Type>(),
                &self.values,
                transform,
            ),
            DataType::Int64 => transform_primitive_typed::<_, V, _, _>(
                diagnostic_receiver,
                array.as_primitive::<Int64Type>(),
                &self.values,
                transform,
            ),

            DataType::UInt8 => transform_primitive_typed::<_, V, _, _>(
                diagnostic_receiver,
                array.as_primitive::<UInt8Type>(),
                &self.values,
                transform,
            ),
            DataType::UInt16 => transform_primitive_typed::<_, V, _, _>(
                diagnostic_receiver,
                array.as_primitive::<UInt16Type>(),
                &self.values,
                transform,
            ),
            DataType::UInt32 => transform_primitive_typed::<_, V, _, _>(
                diagnostic_receiver,
                array.as_primitive::<UInt32Type>(),
                &self.values,
                transform,
            ),
            DataType::UInt64 => transform_primitive_typed::<_, V, _, _>(
                diagnostic_receiver,
                array.as_primitive::<UInt64Type>(),
                &self.values,
                transform,
            ),

            _ => panic!("Unexpected dictionary key type"),
        }
    }

    pub(crate) fn transform_into_any<'b, V: ArrowPrimitiveType, D: DiagnosticReceiver, FTransform>(
        &'b self,
        diagnostic_receiver: &D,
        transform: FTransform,
    ) -> Result<Dictionary<'b>, ExpressionError>
    where
        <V as ArrowPrimitiveType>::Native: Hash,
        <V as ArrowPrimitiveType>::Native: Eq,
        FTransform: Fn(&D, Option<ValueOrRef<'_>>) -> Option<ValueOrRef<'b>>,
    {
        let array = self.keys.as_array();

        match array.data_type() {
            DataType::Int8 => transform_any_typed(
                diagnostic_receiver,
                array.as_primitive::<Int8Type>(),
                &self.values,
                transform,
            ),
            DataType::Int16 => transform_any_typed(
                diagnostic_receiver,
                array.as_primitive::<Int16Type>(),
                &self.values,
                transform,
            ),
            DataType::Int32 => transform_any_typed(
                diagnostic_receiver,
                array.as_primitive::<Int32Type>(),
                &self.values,
                transform,
            ),
            DataType::Int64 => transform_any_typed(
                diagnostic_receiver,
                array.as_primitive::<Int64Type>(),
                &self.values,
                transform,
            ),

            DataType::UInt8 => transform_any_typed(
                diagnostic_receiver,
                array.as_primitive::<UInt8Type>(),
                &self.values,
                transform,
            ),
            DataType::UInt16 => transform_any_typed(
                diagnostic_receiver,
                array.as_primitive::<UInt16Type>(),
                &self.values,
                transform,
            ),
            DataType::UInt32 => transform_any_typed(
                diagnostic_receiver,
                array.as_primitive::<UInt32Type>(),
                &self.values,
                transform,
            ),
            DataType::UInt64 => transform_any_typed(
                diagnostic_receiver,
                array.as_primitive::<UInt64Type>(),
                &self.values,
                transform,
            ),

            _ => panic!("Unexpected dictionary key type"),
        }
    }
}

impl Display for Dictionary<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('{')?;
        for key in 0..self.keys.len() {
            if key > 0 {
                f.write_char(',')?;
            }
            match self.get_value(key) {
                None => {write!(f, "{key}:Null")?;}
                Some(v) => {
                    write!(f, "{key}:")?;
                    fmt_value(v.to_value(), f)?;
                },
            }
        }
        f.write_char('}')
    }
}

impl From<BooleanArray> for Dictionary<'_> {
    fn from(value: BooleanArray) -> Self {
        Dictionary {
            keys: DictionaryKeyArray::BooleanOwned(value),
            values: DictionaryValueArray::Boolean,
        }
    }
}

impl<'a> From<&'a BooleanArray> for Dictionary<'a> {
    fn from(value: &'a BooleanArray) -> Self {
        Dictionary {
            keys: DictionaryKeyArray::BooleanRef(value),
            values: DictionaryValueArray::Boolean,
        }
    }
}

impl<T: ArrowDictionaryKeyType> From<DictionaryArray<T>> for Dictionary<'_> {
    fn from(value: DictionaryArray<T>) -> Self {
        let (keys, values) = value.into_parts();
        Dictionary {
            keys: keys.into(),
            values: values.into(),
        }
    }
}

impl<'a, K: ArrowDictionaryKeyType, V> From<TypedDictionaryArray<'a, K, V>> for Dictionary<'a>
where
    DictionaryValueArray<'a>: From<&'a V>,
{
    fn from(value: TypedDictionaryArray<'a, K, V>) -> Self {
        Dictionary {
            keys: value.keys().into(),
            values: value.values().into(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DictionaryKeyArray<'a> {
    ArrayRef(&'a dyn Array),
    ArrayOwned(Arc<dyn Array>),
    BooleanRef(&'a BooleanArray),
    BooleanOwned(BooleanArray),
}

impl DictionaryKeyArray<'_> {
    pub fn len(&self) -> usize {
        match self {
            DictionaryKeyArray::ArrayRef(a) => a.len(),
            DictionaryKeyArray::ArrayOwned(a) => a.len(),
            DictionaryKeyArray::BooleanRef(a) => a.len(),
            DictionaryKeyArray::BooleanOwned(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DictionaryKeyArray::ArrayRef(a) => a.is_empty(),
            DictionaryKeyArray::ArrayOwned(a) => a.is_empty(),
            DictionaryKeyArray::BooleanRef(a) => a.is_empty(),
            DictionaryKeyArray::BooleanOwned(a) => a.is_empty(),
        }
    }

    pub fn as_array(&self) -> &dyn Array {
        match self {
            DictionaryKeyArray::ArrayRef(a) => *a,
            DictionaryKeyArray::ArrayOwned(a) => a,
            DictionaryKeyArray::BooleanRef(a) => *a,
            DictionaryKeyArray::BooleanOwned(a) => a,
        }
    }

    pub fn get_value_index_for_key_index(&self, index: usize) -> Option<usize> {
        match self {
            DictionaryKeyArray::ArrayRef(a) => get_key_array_value_index_for_key_index(*a, index),
            DictionaryKeyArray::ArrayOwned(a) => get_key_array_value_index_for_key_index(a, index),
            DictionaryKeyArray::BooleanRef(a) => {
                get_bool_array_value_index_for_key_index(*a, index)
            }
            DictionaryKeyArray::BooleanOwned(a) => {
                get_bool_array_value_index_for_key_index(a, index)
            }
        }
    }

    pub fn create_builder(&self) -> Box<dyn DictionaryKeyArrayBuilder> {
        let key_count = self.len();

        let array = self.as_array();

        match array.data_type() {
            DataType::Int8 => Box::new(TypeDictionaryKeyArrayBuilder::<Int8Type>::new(key_count)),
            DataType::Int16 => Box::new(TypeDictionaryKeyArrayBuilder::<Int16Type>::new(key_count)),
            DataType::Int32 => Box::new(TypeDictionaryKeyArrayBuilder::<Int32Type>::new(key_count)),
            DataType::Int64 => Box::new(TypeDictionaryKeyArrayBuilder::<Int64Type>::new(key_count)),

            DataType::UInt8 => Box::new(TypeDictionaryKeyArrayBuilder::<UInt8Type>::new(key_count)),
            DataType::UInt16 => {
                Box::new(TypeDictionaryKeyArrayBuilder::<UInt16Type>::new(key_count))
            }
            DataType::UInt32 => {
                Box::new(TypeDictionaryKeyArrayBuilder::<UInt32Type>::new(key_count))
            }
            DataType::UInt64 => {
                Box::new(TypeDictionaryKeyArrayBuilder::<UInt64Type>::new(key_count))
            }

            _ => panic!("Unexpected dictionary key type"),
        }
    }
}

impl<T: ArrowDictionaryKeyType> From<PrimitiveArray<T>> for DictionaryKeyArray<'_> {
    fn from(value: PrimitiveArray<T>) -> DictionaryKeyArray<'static> {
        DictionaryKeyArray::ArrayOwned(Arc::new(value))
    }
}

impl<'a, T: ArrowDictionaryKeyType> From<&'a PrimitiveArray<T>> for DictionaryKeyArray<'a> {
    fn from(value: &'a PrimitiveArray<T>) -> DictionaryKeyArray<'a> {
        DictionaryKeyArray::ArrayRef(value)
    }
}

pub trait DictionaryKeyArrayBuilder {
    fn push_value_index(&mut self, value_index: usize);

    fn push_null(&mut self);

    fn finish(&mut self) -> DictionaryKeyArray<'static>;
}

struct TypeDictionaryKeyArrayBuilder<K: ArrowDictionaryKeyType> {
    builder: PrimitiveBuilder<K>,
}

impl<K: ArrowDictionaryKeyType> TypeDictionaryKeyArrayBuilder<K> {
    pub fn new(capacity: usize) -> TypeDictionaryKeyArrayBuilder<K> {
        Self {
            builder: PrimitiveBuilder::with_capacity(capacity),
        }
    }
}

impl<K: ArrowDictionaryKeyType> DictionaryKeyArrayBuilder for TypeDictionaryKeyArrayBuilder<K> {
    fn push_value_index(&mut self, value_index: usize) {
        self.builder
            .append_value(K::Native::from_usize(value_index).unwrap());
    }

    fn push_null(&mut self) {
        self.builder.append_null();
    }

    fn finish(&mut self) -> DictionaryKeyArray<'static> {
        PrimitiveBuilder::<K>::finish(&mut self.builder).into()
    }
}

fn get_key_array_value_index_for_key_index(array: &dyn Array, key_index: usize) -> Option<usize> {
    if array.is_null(key_index) {
        return None;
    }

    unsafe {
        Some(match array.data_type() {
            DataType::Int8 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int8Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::Int16 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int16Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::Int32 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int32Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<Int64Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,

            DataType::UInt8 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt8Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::UInt16 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt16Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::UInt32 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt32Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,
            DataType::UInt64 => array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()
                .unwrap()
                .value_unchecked(key_index) as usize,

            _ => panic!(),
        })
    }
}

fn get_bool_array_value_index_for_key_index(
    array: &BooleanArray,
    key_index: usize,
) -> Option<usize> {
    if array.is_null(key_index) {
        return None;
    }
    Some(match array.value(key_index) {
        true => 1,
        false => 0,
    })
}

#[derive(Debug, Clone)]
pub enum DictionaryValueArray<'a> {
    ArrayRef(&'a dyn Array),
    ArrayOwned(Arc<dyn Array>),
    AnyOwned(Vec<ValueOrRef<'a>>),
    Boolean,
}

impl<'a> DictionaryValueArray<'a> {
    pub fn len(&self) -> usize {
        match self {
            DictionaryValueArray::ArrayRef(a) => a.len(),
            DictionaryValueArray::ArrayOwned(a) => a.len(),
            DictionaryValueArray::Boolean => 2,
            DictionaryValueArray::AnyOwned(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DictionaryValueArray::ArrayRef(a) => a.is_empty(),
            DictionaryValueArray::ArrayOwned(a) => a.is_empty(),
            DictionaryValueArray::Boolean => false,
            DictionaryValueArray::AnyOwned(a) => a.is_empty(),
        }
    }

    pub fn get_value_at(&self, index: usize) -> Option<ValueOrRef<'_>> {
        match self {
            DictionaryValueArray::ArrayRef(a) => get_value_from_array(*a, index),
            DictionaryValueArray::ArrayOwned(a) => get_value_from_array(a, index),
            DictionaryValueArray::AnyOwned(a) => Some((&a[index]).into()),
            DictionaryValueArray::Boolean => Some(ValueOrRef::BooleanOwned(if index == 0 {
                false
            } else {
                true
            })),
        }
    }
}

impl<T: ArrowPrimitiveType> From<PrimitiveArray<T>> for DictionaryValueArray<'_> {
    fn from(value: PrimitiveArray<T>) -> DictionaryValueArray<'static> {
        DictionaryValueArray::ArrayOwned(Arc::new(value))
    }
}

impl From<Arc<dyn Array>> for DictionaryValueArray<'_> {
    fn from(value: Arc<dyn Array>) -> DictionaryValueArray<'static> {
        DictionaryValueArray::ArrayOwned(value)
    }
}

impl<'a, T: Array + 'a> From<&'a T> for DictionaryValueArray<'a> {
    fn from(value: &'a T) -> DictionaryValueArray<'a> {
        DictionaryValueArray::ArrayRef(value)
    }
}

pub(crate) fn get_value_from_array(value: &dyn Array, index: usize) -> Option<ValueOrRef<'_>> {
    if !value.is_valid(index) {
        return None;
    }

    unsafe {
        match value.data_type() {
            DataType::Int8 => Some(ValueOrRef::IntegerRef(IntegerRef::Int8(
                value
                    .as_primitive::<Int8Type>()
                    .values()
                    .get_unchecked(index),
            ))),
            DataType::Int16 => Some(ValueOrRef::IntegerRef(IntegerRef::Int16(
                value
                    .as_primitive::<Int16Type>()
                    .values()
                    .get_unchecked(index),
            ))),
            DataType::Int32 => Some(ValueOrRef::IntegerRef(IntegerRef::Int32(
                value
                    .as_primitive::<Int32Type>()
                    .values()
                    .get_unchecked(index),
            ))),
            DataType::Int64 => Some(ValueOrRef::IntegerRef(IntegerRef::Int64(
                value
                    .as_primitive::<Int64Type>()
                    .values()
                    .get_unchecked(index),
            ))),

            DataType::UInt8 => Some(ValueOrRef::IntegerRef(IntegerRef::UInt8(
                value
                    .as_primitive::<UInt8Type>()
                    .values()
                    .get_unchecked(index),
            ))),
            DataType::UInt16 => Some(ValueOrRef::IntegerRef(IntegerRef::UInt16(
                value
                    .as_primitive::<UInt16Type>()
                    .values()
                    .get_unchecked(index),
            ))),
            DataType::UInt32 => Some(ValueOrRef::IntegerRef(IntegerRef::UInt32(
                value
                    .as_primitive::<UInt32Type>()
                    .values()
                    .get_unchecked(index),
            ))),
            DataType::UInt64 => Some(ValueOrRef::IntegerRef(IntegerRef::UInt64(
                value
                    .as_primitive::<UInt64Type>()
                    .values()
                    .get_unchecked(index),
            ))),

            DataType::Float16 => Some(ValueOrRef::DoubleRef(DoubleRef::Float16(
                value
                    .as_primitive::<Float16Type>()
                    .values()
                    .get_unchecked(index),
            ))),
            DataType::Float32 => Some(ValueOrRef::DoubleRef(DoubleRef::Float32(
                value
                    .as_primitive::<Float32Type>()
                    .values()
                    .get_unchecked(index),
            ))),
            DataType::Float64 => Some(ValueOrRef::DoubleRef(DoubleRef::Float64(
                value
                    .as_primitive::<Float64Type>()
                    .values()
                    .get_unchecked(index),
            ))),

            DataType::Utf8 => Some(ValueOrRef::StringRef(
                value.as_string::<i32>().value_unchecked(index),
            )),
            DataType::LargeUtf8 => Some(ValueOrRef::StringRef(
                value.as_string::<i64>().value_unchecked(index),
            )),

            _ => todo!(),
        }
    }
}

fn transform_boolean_typed<K: ArrowDictionaryKeyType, D: DiagnosticReceiver, FTransform>(
    diagnostic_receiver: &D,
    keys: &PrimitiveArray<K>,
    values: &DictionaryValueArray<'_>,
    transform: FTransform,
) -> Result<BooleanArray, ExpressionError>
where
    FTransform: Fn(&D, Option<ValueOrRef<'_>>) -> Option<bool>,
{
    let key_length = keys.len();

    let mut builder = BooleanBuilder::with_capacity(key_length);
    let mut value_index_lookup = HashMap::new();

    for index in 0..key_length {
        if keys.is_valid(index) {
            let value_index =
                <K as ArrowPrimitiveType>::Native::as_usize(unsafe { keys.value_unchecked(index) });
            match value_index_lookup.entry(value_index) {
                Entry::Occupied(o) => {
                    if let Some(transformed_value) = o.get() {
                        builder.append_value(*transformed_value);
                        continue;
                    }
                }
                Entry::Vacant(v) => {
                    if let Some(transformed_value) =
                        transform(diagnostic_receiver, values.get_value_at(value_index))
                    {
                        builder.append_value(transformed_value);
                        v.insert(Some(transformed_value));
                        continue;
                    } else {
                        v.insert(None);
                    }
                }
            }
        }

        builder.append_null();
    }

    Ok(builder.finish())
}

fn transform_primitive_typed<
    'a,
    K: ArrowDictionaryKeyType,
    V: ArrowPrimitiveType,
    D: DiagnosticReceiver,
    FTransform,
>(
    diagnostic_receiver: &D,
    keys: &PrimitiveArray<K>,
    values: &DictionaryValueArray<'a>,
    transform: FTransform,
) -> Result<Dictionary<'a>, ExpressionError>
where
    <V as ArrowPrimitiveType>::Native: Hash,
    <V as ArrowPrimitiveType>::Native: Eq,
    FTransform: Fn(&D, Option<ValueOrRef<'_>>) -> Option<V::Native>,
{
    let key_length = keys.len();

    let mut builder = PrimitiveBuilder::<K>::with_capacity(key_length);
    let mut value_index_lookup = HashMap::new();
    let mut transformed_values: IndexSet<<V as ArrowPrimitiveType>::Native> = IndexSet::new();
    let mut null_index = None;

    for index in 0..key_length {
        if keys.is_valid(index) {
            let value_index =
                <K as ArrowPrimitiveType>::Native::as_usize(unsafe { keys.value_unchecked(index) });
            match value_index_lookup.entry(value_index) {
                Entry::Occupied(o) => {
                    if let Some(index) = o.get() {
                        builder.append_value(*index);
                        continue;
                    }
                }
                Entry::Vacant(v) => {
                    if let Some(transformed_value) =
                        transform(diagnostic_receiver, values.get_value_at(value_index))
                    {
                        let (index, _) = transformed_values.insert_full(transformed_value);
                        let value_index =
                            <K as ArrowPrimitiveType>::Native::from_usize(index).unwrap();
                        builder.append_value(value_index);
                        v.insert(Some(value_index));
                        continue;
                    } else {
                        v.insert(None);
                    }
                }
            }
        } else {
            let (has_value_index, value_index) = null_index.get_or_insert_with(|| {
                if let Some(null_value) = transform(diagnostic_receiver, None) {
                    let (index, _) = transformed_values.insert_full(null_value);
                    (
                        true,
                        <K as ArrowPrimitiveType>::Native::from_usize(index).unwrap(),
                    )
                } else {
                    (
                        false,
                        <K as ArrowPrimitiveType>::Native::from_usize(0).unwrap(),
                    )
                }
            });

            if *has_value_index {
                builder.append_value(*value_index);
                continue;
            }
        }

        builder.append_null();
    }

    let values: Vec<<V as ArrowPrimitiveType>::Native> = transformed_values.drain(..).collect();

    Ok(Dictionary {
        keys: builder.finish().into(),
        values: PrimitiveArray::<V>::new(values.into(), None).into(),
    })
}

fn transform_any_typed<'a, K: ArrowDictionaryKeyType, D: DiagnosticReceiver, FTransform>(
    diagnostic_receiver: &D,
    keys: &PrimitiveArray<K>,
    values: &'a DictionaryValueArray<'_>,
    transform: FTransform,
) -> Result<Dictionary<'a>, ExpressionError>
where
    FTransform: Fn(&D, Option<ValueOrRef<'_>>) -> Option<ValueOrRef<'a>>,
{
    let key_count = keys.len();

    let mut key_builder = PrimitiveBuilder::<K>::with_capacity(key_count);
    let mut value_index_lookup = HashMap::new();
    let mut transformed_values = IndexSet::new();
    let mut null_index = None;

    for index in 0..key_count {
        if keys.is_valid(index) {
            let value_index =
                <K as ArrowPrimitiveType>::Native::as_usize(unsafe { keys.value_unchecked(index) });

            match value_index_lookup.entry(value_index) {
                Entry::Occupied(o) => {
                    if let Some(value_index) = o.get() {
                        key_builder.append_value(*value_index);
                        continue;
                    }
                }
                Entry::Vacant(v) => {
                    if let Some(transformed_value) =
                        transform(diagnostic_receiver, values.get_value_at(value_index))
                    {
                        let (index, _) = transformed_values.insert_full(transformed_value);
                        let value_index =
                            <K as ArrowPrimitiveType>::Native::from_usize(index).unwrap();
                        key_builder.append_value(value_index);
                        v.insert(Some(value_index));
                        continue;
                    } else {
                        v.insert(None);
                    }
                }
            }
        } else {
            let (has_value_index, value_index) = null_index.get_or_insert_with(|| {
                if let Some(null_value) = transform(diagnostic_receiver, None) {
                    let (index, _) = transformed_values.insert_full(null_value);
                    (
                        true,
                        <K as ArrowPrimitiveType>::Native::from_usize(index).unwrap(),
                    )
                } else {
                    (
                        false,
                        <K as ArrowPrimitiveType>::Native::from_usize(0).unwrap(),
                    )
                }
            });

            if *has_value_index {
                key_builder.append_value(*value_index);
                continue;
            }
        }

        key_builder.append_null();
    }

    Ok(Dictionary {
        keys: key_builder.finish().into(),
        values: DictionaryValueArray::AnyOwned(transformed_values.drain(..).collect()),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::UInt8Type;

    use super::*;

    #[test]
    fn string_dictionary_get_len_dictionary() {
        let mut strings_builder = StringDictionaryBuilder::<UInt8Type>::new();
        strings_builder.append("value1").unwrap();
        strings_builder.append("value1").unwrap();
        strings_builder.append_null();
        strings_builder.append_option(Some("other_value"));
        strings_builder.append_option(None::<&str>);

        let strings: Dictionary = strings_builder.finish().into();

        let len_view = strings
            .get_len_dictionary(&NoopDiagnosticReceiver {})
            .unwrap();

        assert_eq!(5, len_view.keys().len());
        assert_eq!(2, len_view.values().len());
        assert_eq!(Some(ValueOrRef::IntegerOwned(6)), len_view.get_value(0));
        assert_eq!(Some(ValueOrRef::IntegerOwned(6)), len_view.get_value(1));
        assert_eq!(None, len_view.get_value(2));
        assert_eq!(Some(ValueOrRef::IntegerOwned(11)), len_view.get_value(3));
        assert_eq!(None, len_view.get_value(4));

        // Second phase tests a StringArray which contains a null value

        let mut keys = PrimitiveBuilder::<UInt8Type>::new();
        keys.append_value(0);
        keys.append_value(1);
        keys.append_value(2);

        let keys = keys.finish();

        let mut values = StringBuilder::new();
        values.append_value("value1");
        values.append_value("other_value");
        values.append_null();

        let strings: Dictionary = DictionaryArray::new(keys, Arc::new(values.finish())).into();

        let len_view = strings
            .get_len_dictionary(&NoopDiagnosticReceiver {})
            .unwrap();

        assert_eq!(3, len_view.keys().len());
        assert_eq!(2, len_view.values().len());
        assert_eq!(Some(ValueOrRef::IntegerOwned(6)), len_view.get_value(0));
        assert_eq!(Some(ValueOrRef::IntegerOwned(11)), len_view.get_value(1));
        assert_eq!(None, len_view.get_value(2));
    }
}
