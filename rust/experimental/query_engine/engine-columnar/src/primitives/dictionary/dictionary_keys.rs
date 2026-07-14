// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{marker::PhantomData, sync::Arc};

use arrow::{
    array::*,
    buffer::{MutableBuffer, NullBuffer},
    datatypes::*,
};

use crate::{dictionary_transform::push_null, *};

#[derive(Debug, Clone, PartialEq)]
pub enum DictionaryKeyArray {
    KeyArray(Arc<dyn Array>),
    BooleanArray {
        data_type: DataType,
        values: Arc<dyn Array>,
    },
    UniqueValues {
        data_type: DataType,
        length: usize,
    },
    SingleValue {
        data_type: DataType,
        length: usize,
        value_index: Option<usize>,
    },
}

impl DictionaryKeyArray {
    pub fn len(&self) -> usize {
        match self {
            DictionaryKeyArray::KeyArray(a) => a.len(),
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => values.len(),
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length,
            } => *length,
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length,
                value_index: _,
            } => *length,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            DictionaryKeyArray::KeyArray(a) => a.is_empty(),
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => values.is_empty(),
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length,
            } => *length == 0,
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length,
                value_index: _,
            } => *length == 0,
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            DictionaryKeyArray::KeyArray(array) => array.null_count() == array.len(),
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => values.null_count() == values.len(),
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length: _,
            } => false,
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length: _,
                value_index,
            } => value_index.is_none(),
        }
    }

    pub fn nulls(&self) -> Option<NullBuffer> {
        match self {
            DictionaryKeyArray::KeyArray(array) => array.nulls().cloned(),
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => values.nulls().cloned(),
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length: _,
            } => None,
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length,
                value_index,
            } => {
                if value_index.is_none() {
                    Some(NullBuffer::new_null(*length))
                } else {
                    None
                }
            }
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            DictionaryKeyArray::KeyArray(a) => a.data_type().clone(),
            DictionaryKeyArray::BooleanArray {
                data_type,
                values: _,
            } => data_type.clone(),
            DictionaryKeyArray::UniqueValues {
                data_type,
                length: _,
            } => data_type.clone(),
            DictionaryKeyArray::SingleValue {
                data_type,
                length: _,
                value_index: _,
            } => data_type.clone(),
        }
    }

    pub fn get_value_index_for_key_index(&self, index: usize) -> Option<usize> {
        match self {
            DictionaryKeyArray::KeyArray(a) => get_key_array_value_index_for_key_index(a, index),
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => get_bool_array_value_index_for_key_index(values.as_boolean(), index),
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length,
            } => {
                if index > *length {
                    None
                } else {
                    Some(index)
                }
            }
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length,
                value_index,
            } => {
                if index > *length {
                    None
                } else {
                    *value_index
                }
            }
        }
    }

    pub fn transform_into_key_array<KOutput: ArrowDictionaryKeyType>(
        self,
        value_index_lookup: IndexLookup,
    ) -> PrimitiveArray<KOutput> {
        self.transform_into_key_builder(value_index_lookup).finish()
    }

    pub(crate) fn transform_into_key_builder<KOutput: ArrowDictionaryKeyType>(
        self,
        value_index_lookup: IndexLookup,
    ) -> DictionaryKeyArrayBuilder<KOutput> {
        match self {
            DictionaryKeyArray::KeyArray(array) => match array.data_type() {
                DataType::UInt8 => transform_key_array_into_key_builder(
                    array.as_primitive::<UInt8Type>(),
                    value_index_lookup,
                ),
                DataType::UInt16 => transform_key_array_into_key_builder(
                    array.as_primitive::<UInt16Type>(),
                    value_index_lookup,
                ),
                DataType::UInt32 => transform_key_array_into_key_builder(
                    array.as_primitive::<UInt32Type>(),
                    value_index_lookup,
                ),
                DataType::UInt64 => transform_key_array_into_key_builder(
                    array.as_primitive::<UInt64Type>(),
                    value_index_lookup,
                ),

                DataType::Int8 => transform_key_array_into_key_builder(
                    array.as_primitive::<Int8Type>(),
                    value_index_lookup,
                ),
                DataType::Int16 => transform_key_array_into_key_builder(
                    array.as_primitive::<Int16Type>(),
                    value_index_lookup,
                ),
                DataType::Int32 => transform_key_array_into_key_builder(
                    array.as_primitive::<Int32Type>(),
                    value_index_lookup,
                ),
                DataType::Int64 => transform_key_array_into_key_builder(
                    array.as_primitive::<Int64Type>(),
                    value_index_lookup,
                ),

                d => panic!("Key type '{d}' is not supported"),
            },
            DictionaryKeyArray::BooleanArray {
                data_type: _,
                values,
            } => {
                let mut builder = DictionaryKeyArrayBuilder::<KOutput>::new(values.len());
                let mut writer = builder.get_writer();

                let (false_value_index, true_value_index) = if let Some(lookup) = value_index_lookup
                {
                    (
                        lookup.get(&0).and_then(|v| {
                            v.map(|v| {
                                <KOutput as ArrowPrimitiveType>::Native::from_usize(v).unwrap()
                            })
                        }),
                        lookup.get(&1).and_then(|v| {
                            v.map(|v| {
                                <KOutput as ArrowPrimitiveType>::Native::from_usize(v).unwrap()
                            })
                        }),
                    )
                } else {
                    (
                        Some(<KOutput as ArrowPrimitiveType>::Native::from_usize(0).unwrap()),
                        Some(<KOutput as ArrowPrimitiveType>::Native::from_usize(1).unwrap()),
                    )
                };

                for (key_index, v) in values.as_boolean().into_iter().enumerate() {
                    if let Some(v) = v {
                        if v {
                            if let Some(true_value_index) = true_value_index {
                                unsafe {
                                    writer.set_value_index_typed_unchecked(
                                        key_index,
                                        true_value_index,
                                    )
                                }
                                continue;
                            }
                        } else if let Some(false_value_index) = false_value_index {
                            unsafe {
                                writer.set_value_index_typed_unchecked(key_index, false_value_index)
                            }
                            continue;
                        }
                    }

                    unsafe { writer.set_null_unchecked(key_index) }
                }

                builder
            }
            DictionaryKeyArray::UniqueValues {
                data_type: _,
                length,
            } => {
                let mut builder = DictionaryKeyArrayBuilder::<KOutput>::new(length);
                let mut writer = builder.get_writer();

                for key_index in 0..length {
                    let transformed_value_index = if let Some(lookup) = value_index_lookup.as_ref()
                    {
                        lookup.get(&key_index).and_then(|v| *v)
                    } else {
                        Some(key_index)
                    };

                    if let Some(value_index) = transformed_value_index {
                        unsafe { writer.set_value_index_unchecked(key_index, value_index) };
                    } else {
                        unsafe { writer.set_null_unchecked(key_index) };
                    }
                }

                builder
            }
            DictionaryKeyArray::SingleValue {
                data_type: _,
                length,
                value_index,
            } => {
                if let Some(value_index) = value_index {
                    let mut builder = DictionaryKeyArrayBuilder::<KOutput>::new(length);
                    let mut writer = builder.get_writer();

                    let transformed_value_index = if let Some(lookup) = value_index_lookup.as_ref()
                    {
                        lookup.get(&value_index).and_then(|v| {
                            v.map(|v| {
                                <KOutput as ArrowPrimitiveType>::Native::from_usize(v).unwrap()
                            })
                        })
                    } else {
                        Some(
                            <KOutput as ArrowPrimitiveType>::Native::from_usize(value_index)
                                .expect("value index converted to output size"),
                        )
                    };

                    if let Some(transformed_value_index) = transformed_value_index {
                        for key_index in 0..length {
                            unsafe {
                                writer.set_value_index_typed_unchecked(
                                    key_index,
                                    transformed_value_index,
                                )
                            };
                        }
                        return builder;
                    }
                }

                DictionaryKeyArrayBuilder::<KOutput>::new_null(length)
            }
        }
    }
}

fn transform_key_array_into_key_builder<
    KInput: ArrowDictionaryKeyType,
    KOutput: ArrowDictionaryKeyType,
>(
    keys: &PrimitiveArray<KInput>,
    value_index_lookup: IndexLookup,
) -> DictionaryKeyArrayBuilder<KOutput> {
    let mut builder = DictionaryKeyArrayBuilder::<KOutput>::new(keys.len());
    let mut writer = builder.get_writer();

    for (key_index, value_index) in keys.into_iter().enumerate() {
        if let Some(value_index) = value_index {
            let transformed_value_index = match value_index_lookup.as_ref() {
                Some(lookup) => lookup
                    .get(&<KInput as ArrowPrimitiveType>::Native::as_usize(
                        value_index,
                    ))
                    .and_then(|v| *v),
                None => Some(<KInput as ArrowPrimitiveType>::Native::as_usize(
                    value_index,
                )),
            };

            if let Some(transformed_value_index) = transformed_value_index {
                unsafe {
                    writer.set_value_index_unchecked(key_index, transformed_value_index);
                };
                continue;
            }
        }

        unsafe { writer.set_null_unchecked(key_index) };
    }

    builder
}

impl<T: ArrowDictionaryKeyType> From<PrimitiveArray<T>> for DictionaryKeyArray {
    fn from(value: PrimitiveArray<T>) -> DictionaryKeyArray {
        DictionaryKeyArray::KeyArray(Arc::new(value))
    }
}

impl From<&dyn Array> for DictionaryKeyArray {
    fn from(value: &dyn Array) -> DictionaryKeyArray {
        DictionaryKeyArray::KeyArray(value.slice(0, value.len()))
    }
}

impl<'a, T: ArrowPrimitiveType> From<&'a PrimitiveArray<T>> for DictionaryKeyArray {
    fn from(value: &'a PrimitiveArray<T>) -> DictionaryKeyArray {
        DictionaryKeyArray::KeyArray((value as &dyn Array).slice(0, value.len()))
    }
}

fn get_key_array_value_index_for_key_index(array: &dyn Array, key_index: usize) -> Option<usize> {
    if key_index > array.len() || array.is_null(key_index) {
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

            d => panic!("Key type '{d}' is not supported"),
        })
    }
}

fn get_bool_array_value_index_for_key_index(
    array: &BooleanArray,
    key_index: usize,
) -> Option<usize> {
    if key_index > array.len() || array.is_null(key_index) {
        return None;
    }
    Some(match unsafe { array.value_unchecked(key_index) } {
        true => 1,
        false => 0,
    })
}

pub struct DictionaryKeyArrayBuilder<K: ArrowDictionaryKeyType> {
    key_length: usize,
    key_bit_length: usize,
    key_buffer: MutableBuffer,
    null_buffer: Option<MutableBuffer>,
    marker: PhantomData<K>,
}

impl<K: ArrowDictionaryKeyType> DictionaryKeyArrayBuilder<K> {
    pub fn new(key_length: usize) -> DictionaryKeyArrayBuilder<K> {
        let key_bit_length = arrow::util::bit_util::ceil(key_length, 8);

        Self {
            key_length,
            key_bit_length,
            key_buffer: MutableBuffer::from_len_zeroed(size_of::<K::Native>() * key_length),
            null_buffer: None,
            marker: Default::default(),
        }
    }

    pub fn new_null(key_length: usize) -> DictionaryKeyArrayBuilder<K> {
        let key_bit_length = arrow::util::bit_util::ceil(key_length, 8);

        Self {
            key_length,
            key_bit_length,
            key_buffer: MutableBuffer::from_len_zeroed(size_of::<K::Native>() * key_length),
            null_buffer: Some(MutableBuffer::from_len_zeroed(key_bit_length)),
            marker: Default::default(),
        }
    }

    pub fn get_key_length(&self) -> usize {
        self.key_length
    }

    pub fn get_writer(&mut self) -> DictionaryKeyArrayWriter<'_, K> {
        DictionaryKeyArrayWriter {
            key_bit_length: self.key_bit_length,
            key_builder: self.key_buffer.typed_data_mut::<K::Native>().as_mut_ptr(),
            null_buffer: &mut self.null_buffer,
        }
    }

    pub fn finish(self) -> PrimitiveArray<K> {
        PrimitiveArray::<K>::new(
            self.key_buffer.into(),
            self.null_buffer
                .and_then(|v| NullBufferBuilder::new_from_buffer(v, self.key_length).build()),
        )
    }
}

pub struct DictionaryKeyArrayWriter<'a, K: ArrowDictionaryKeyType> {
    key_bit_length: usize,
    key_builder: *mut K::Native,
    null_buffer: &'a mut Option<MutableBuffer>,
}

impl<'a, K: ArrowDictionaryKeyType> DictionaryKeyArrayWriter<'a, K> {
    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    pub unsafe fn set_value_index_unchecked(&mut self, key_index: usize, value_index: usize) {
        unsafe {
            *self.key_builder.add(key_index) =
                <K as ArrowPrimitiveType>::Native::from_usize(value_index)
                    .expect("transformed value index converted to output size")
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    pub unsafe fn set_value_index_typed_unchecked(
        &mut self,
        key_index: usize,
        value_index: K::Native,
    ) {
        unsafe { *self.key_builder.add(key_index) = value_index }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    pub unsafe fn set_null_unchecked(&mut self, key_index: usize) {
        unsafe { push_null(self.null_buffer, key_index, self.key_bit_length) }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    pub unsafe fn set_nonnull_unchecked(&mut self, key_index: usize) {
        if let Some(nulls) = self.null_buffer {
            let ptr = nulls.typed_data_mut::<u8>().as_mut_ptr();

            let i = key_index / 8;
            let b = 1 << (key_index % 8);

            unsafe { *ptr.add(i) |= b };
        }
    }
}
