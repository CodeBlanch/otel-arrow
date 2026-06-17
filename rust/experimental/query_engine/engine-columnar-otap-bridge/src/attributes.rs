// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::{OnceCell, RefCell, RefMut},
    collections::hash_map::Entry,
    fmt::Display,
    ops::Deref,
    sync::Arc,
};

use crate::*;
use ahash::AHashMap;
use arrow::{array::*, buffer::MutableBuffer, datatypes::*};
use data_engine_columnar::*;
use otap_df_pdata::{otlp::attributes::*, schema::consts};

pub(crate) const EMPTY_ATTRIBUTE_VALUE_TYPE: u8 = AttributeValueType::Empty as u8;
pub(crate) const STRING_ATTRIBUTE_VALUE_TYPE: u8 = AttributeValueType::Str as u8;
pub(crate) const INT_ATTRIBUTE_VALUE_TYPE: u8 = AttributeValueType::Int as u8;
pub(crate) const DOUBLE_ATTRIBUTE_VALUE_TYPE: u8 = AttributeValueType::Double as u8;
pub(crate) const BOOL_ATTRIBUTE_VALUE_TYPE: u8 = AttributeValueType::Bool as u8;
pub(crate) const MAP_ATTRIBUTE_VALUE_TYPE: u8 = AttributeValueType::Map as u8;
pub(crate) const SLICE_ATTRIBUTE_VALUE_TYPE: u8 = AttributeValueType::Slice as u8;
pub(crate) const BYTES_ATTRIBUTE_VALUE_TYPE: u8 = AttributeValueType::Bytes as u8;

#[derive(Debug)]
pub struct OtapAttributes<'pipeline, 'record> {
    values: RefCell<(bool, AHashMap<Box<str>, OtapValue<'pipeline>>)>,
    batch: Option<OtapAttributesBatch<'record>>,
}

impl<'pipeline, 'record> OtapAttributes<'pipeline, 'record> {
    pub fn new(
        ids: OtapIds<'record>,
        attributes_batch: &'record RecordBatch,
    ) -> OtapAttributes<'pipeline, 'record> {
        Self {
            values: RefCell::new((false, AHashMap::new())),
            batch: Some(OtapAttributesBatch::new(
                ids,
                OtapParentIds::new(attributes_batch),
                None,
                attributes_batch,
            )),
        }
    }

    pub fn new_empty() -> OtapAttributes<'pipeline, 'record> {
        Self {
            values: RefCell::new((false, AHashMap::new())),
            batch: None,
        }
    }

    /*pub fn from_parts(
        ids: OtapIds<'record>,
        decoded_parent_ids: Option<PrimitiveArray<UInt16Type>>,
        id_to_record_index_map: Option<PrimitiveArray<UInt16Type>>,
        values: AHashMap<Box<str>, OtapAttributeValue<'pipeline>>,
        attributes_batch: &'record RecordBatch,
    ) -> OtapAttributes<'pipeline, 'record> {
        Self::new_internal(
            ids,
            decoded_parent_ids
                .map(OtapParentIds::from_decoded)
                .unwrap_or_else(|| OtapParentIds::new(attributes_batch)),
            id_to_record_index_map,
            values,
            attributes_batch,
        )
    }*/

    pub fn into_parts(self) -> OtapAttributesState<'pipeline> {
        let (modified, values) = self.values.take();

        if let Some(mut batch) = self.batch {
            OtapAttributesState {
                decoded_ids: Some(OtapDecodedIds {
                    ids: batch.ids.into_parts(),
                    parent_ids: batch.parent_ids.into_parts(),
                }),
                id_to_record_index_map: batch.id_to_record_index_map.take(),
                modified,
                values,
            }
        } else {
            OtapAttributesState {
                decoded_ids: None,
                id_to_record_index_map: None,
                modified,
                values,
            }
        }
    }

    pub(crate) fn get_values(
        &self,
        key: &str,
    ) -> (RefMut<'_, bool>, RefMut<'_, OtapValue<'pipeline>>) {
        RefMut::map_split(self.values.borrow_mut(), |(modified, values)| {
            let value = match values.entry(key.into()) {
                Entry::Occupied(occupied) => occupied.into_mut(),
                Entry::Vacant(vacant) => {
                    if let Some(batch) = self.batch.as_ref()
                        && let Some(value) = batch.get_values(key)
                    {
                        vacant.insert(OtapValue::Read(value))
                    } else {
                        vacant.insert(OtapValue::NotFound)
                    }
                }
            };

            (modified, value)
        })
    }
}

impl<'pipeline, 'record> RecordTable<'pipeline> for OtapAttributes<'pipeline, 'record> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'pipeline, '_>> {
        match self.get_values(key).1.deref() {
            OtapValue::NotFound | OtapValue::Removed => None,
            OtapValue::Read(v) | OtapValue::Set(v) => Some(RecordTableValue::Dictionary(v.clone())),
        }
    }
}

impl Display for OtapAttributes<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Attributes(RecordCount={})",
            self.batch
                .as_ref()
                .map_or(0, |v| v.attributes_batch.num_rows())
        )
    }
}

#[derive(Debug)]
struct OtapAttributesBatch<'record> {
    ids: OtapIds<'record>,
    parent_ids: OtapParentIds<'record>,
    id_to_record_index_map: OnceCell<PrimitiveArray<UInt16Type>>,
    attributes_batch: &'record RecordBatch,
    attribute_keys: AdaptiveDictionaryReader<'record, GenericByteArray<GenericStringType<i32>>>,
    attribute_types: &'record PrimitiveArray<UInt8Type>,
    attribute_string_keys: &'record PrimitiveArray<UInt16Type>,
    attribute_string_values: &'record GenericByteArray<GenericStringType<i32>>,
    attribute_ints: Option<TypedDictionaryArray<'record, UInt16Type, PrimitiveArray<Int64Type>>>,
    attribute_doubles: Option<&'record PrimitiveArray<Float64Type>>,
    attribute_bools: Option<&'record BooleanArray>,
    attribute_bytes_keys: Option<&'record PrimitiveArray<UInt16Type>>,
    attribute_bytes_values: Option<&'record GenericBinaryArray<i32>>,
    attribute_ser_keys: Option<&'record PrimitiveArray<UInt16Type>>,
    attribute_ser_values: Option<&'record GenericBinaryArray<i32>>,
}

impl<'record> OtapAttributesBatch<'record> {
    pub fn new(
        ids: OtapIds<'record>,
        parent_ids: OtapParentIds<'record>,
        id_to_record_index_map: Option<PrimitiveArray<UInt16Type>>,
        attributes_batch: &'record RecordBatch,
    ) -> OtapAttributesBatch<'record> {
        let strings = attributes_batch
            .column_by_name(consts::ATTRIBUTE_STR)
            .expect("strings")
            .as_dictionary::<UInt16Type>()
            .downcast_dict::<StringArray>()
            .expect("Attribute strings were an unexpected type");

        let bytes = attributes_batch
            .column_by_name(consts::ATTRIBUTE_BYTES)
            .map(|c| {
                c.as_dictionary::<UInt16Type>()
                    .downcast_dict::<BinaryArray>()
                    .expect("Attribute bytes were an unexpected type")
            });

        let ser = attributes_batch
            .column_by_name(consts::ATTRIBUTE_SER)
            .map(|c| {
                c.as_dictionary::<UInt16Type>()
                    .downcast_dict::<BinaryArray>()
                    .expect("Attribute ser was an unexpected type")
            });

        let id_to_record_index_map = if let Some(id_to_record_index_map) = id_to_record_index_map {
            let v = OnceCell::new();
            v.set(id_to_record_index_map).expect("set");
            v
        } else {
            OnceCell::new()
        };

        Self {
            ids,
            parent_ids,
            id_to_record_index_map,
            attributes_batch,
            attribute_keys: AdaptiveDictionaryReader::<StringArray>::new(
                attributes_batch
                    .column_by_name(consts::ATTRIBUTE_KEY)
                    .expect("has keys"),
            ),
            attribute_types: attributes_batch
                .column_by_name(consts::ATTRIBUTE_TYPE)
                .expect("has types")
                .as_primitive::<UInt8Type>(),
            attribute_string_keys: strings.keys(),
            attribute_string_values: strings.values(),
            attribute_ints: attributes_batch
                .column_by_name(consts::ATTRIBUTE_INT)
                .map(|c| {
                    c.as_dictionary::<UInt16Type>()
                        .downcast_dict::<PrimitiveArray<Int64Type>>()
                        .expect("Attribute ints were an unexpected type")
                }),
            attribute_doubles: attributes_batch
                .column_by_name(consts::ATTRIBUTE_DOUBLE)
                .map(|c| c.as_primitive::<Float64Type>()),
            attribute_bools: attributes_batch
                .column_by_name(consts::ATTRIBUTE_BOOL)
                .map(|c| c.as_boolean()),
            attribute_bytes_keys: bytes.map(|v| v.keys()),
            attribute_bytes_values: bytes.map(|v| v.values()),
            attribute_ser_keys: ser.map(|v| v.keys()),
            attribute_ser_values: ser.map(|v| v.values()),
        }
    }

    pub fn get_values(&self, key: &str) -> Option<Dictionary<'static>> {
        let record_count = self.ids.len();

        if let Some(value_index) = self
            .attribute_keys
            .values()
            .iter()
            .flatten()
            .position(|v| v == key)
        {
            let mut key_buffer = MutableBuffer::from_len_zeroed(record_count * 2);
            let keys = key_buffer.typed_data_mut::<u16>().as_mut_ptr();

            let mut null_buffer =
                MutableBuffer::from_len_zeroed(arrow::util::bit_util::ceil(record_count, 8));
            let nulls = null_buffer.typed_data_mut::<u8>().as_mut_ptr();
            let mut null_count = record_count;

            let mut value_lookup: AHashMap<usize, u16> = AHashMap::with_capacity(record_count);
            let mut values = Vec::with_capacity(record_count);

            let value_index = value_index as u16;

            let attribute_types = self.attribute_types.values().as_ptr();
            let attribute_parent_ids = self.parent_ids.get_ids().values().as_ptr();
            let id_to_record_index_map = self.get_id_to_record_index_map().values().as_ptr();

            for (attribute_index, key_value_index) in self.attribute_keys.key_iter().enumerate() {
                if key_value_index == value_index {
                    let attribute_type = unsafe { *attribute_types.add(attribute_index) };
                    if let Some(attribute_value) =
                        self.get_attribute_value_or_index(attribute_index, attribute_type)
                    {
                        let index = match attribute_value {
                            AttributeValueOrIndex::ValueIndex(attribute_value_index) => {
                                let lookup_key = ((attribute_type as usize) << 16)
                                    | attribute_value_index as usize;
                                match value_lookup.entry(lookup_key) {
                                    Entry::Occupied(occupied) => *occupied.get(),
                                    Entry::Vacant(vacant) => {
                                        let index = values.len();
                                        values.push(self.get_attribute_value(
                                            attribute_type,
                                            attribute_value_index,
                                        ));
                                        *vacant.insert(index as u16)
                                    }
                                }
                            }
                            AttributeValueOrIndex::Value(attribute_value) => {
                                let index = values.len() as u16;
                                values.push(attribute_value);
                                index
                            }
                        };

                        let parent_id = unsafe { *attribute_parent_ids.add(attribute_index) };
                        let record_index =
                            unsafe { *id_to_record_index_map.add(parent_id as usize) };

                        unsafe { *keys.add(record_index as usize) = index };
                        unsafe { arrow::util::bit_util::set_bit_raw(nulls, record_index as usize) };
                        null_count -= 1;
                    }
                }
            }

            let keys = if null_count > 0 {
                PrimitiveArray::<UInt16Type>::new(
                    key_buffer.into(),
                    NullBufferBuilder::new_from_buffer(null_buffer, record_count).build(),
                )
                .into()
            } else {
                PrimitiveArray::<UInt16Type>::new(key_buffer.into(), None).into()
            };

            Some(Dictionary::new(
                keys,
                DictionaryValueArray::Vec(values.into()),
            ))
        } else {
            None
        }
    }

    fn get_id_to_record_index_map(&self) -> &PrimitiveArray<UInt16Type> {
        // Note: id_map is an array of parent_ids (record identifier in the
        // attribute table) to the actual index of the record in the root table.
        self.id_to_record_index_map.get_or_init(|| {
            let ids = self.ids.get_ids();
            let mut id_map_length = ids.len();
            let mut id_map_buffer = MutableBuffer::from_len_zeroed(id_map_length * 2);
            let mut id_map = id_map_buffer.typed_data_mut::<u16>().as_mut_ptr();
            for (record_index, id) in ids.iter().enumerate() {
                if let Some(id) = id {
                    let id = id as usize;
                    if id >= id_map_length {
                        // If the data is malformed or a filter was run there
                        // could be parent ids greater than the number of
                        // records. In this case we need additional capacity to
                        // make the lookup array the correct size.
                        let additional_capacity = id - id_map_length + 1;
                        id_map_buffer.extend_zeros(additional_capacity * 2);
                        id_map_length += additional_capacity;
                        id_map = id_map_buffer.typed_data_mut::<u16>().as_mut_ptr();
                    }
                    unsafe { *id_map.add(id) = record_index as u16 };
                }
            }
            PrimitiveArray::<UInt16Type>::new(id_map_buffer.into(), None)
        })
    }

    fn get_attribute_value_or_index(
        &self,
        attribute_index: usize,
        attribute_type: u8,
    ) -> Option<AttributeValueOrIndex> {
        match attribute_type {
            EMPTY_ATTRIBUTE_VALUE_TYPE => {}
            STRING_ATTRIBUTE_VALUE_TYPE => {
                let keys = self.attribute_string_keys;
                if keys.is_valid(attribute_index) {
                    let value_index = unsafe { keys.value_unchecked(attribute_index) };
                    return Some(AttributeValueOrIndex::ValueIndex(value_index));
                }
            }
            INT_ATTRIBUTE_VALUE_TYPE => {
                return Some(
                    if let Some(ints) = self.attribute_ints
                        && ints.is_valid(attribute_index)
                    {
                        let value = unsafe { ints.value_unchecked(attribute_index) };
                        AttributeValueOrIndex::Value(ValueOrRef::Integer(value))
                    } else {
                        AttributeValueOrIndex::Value(ValueOrRef::Integer(0))
                    },
                );
            }
            DOUBLE_ATTRIBUTE_VALUE_TYPE => {
                return Some(
                    if let Some(doubles) = self.attribute_doubles
                        && doubles.is_valid(attribute_index)
                    {
                        let value = unsafe { doubles.value_unchecked(attribute_index) };
                        AttributeValueOrIndex::Value(ValueOrRef::Double(value))
                    } else {
                        AttributeValueOrIndex::Value(ValueOrRef::Double(0f64))
                    },
                );
            }
            BOOL_ATTRIBUTE_VALUE_TYPE => {
                if let Some(bools) = self.attribute_bools
                    && bools.is_valid(attribute_index)
                {
                    let value = unsafe { bools.value_unchecked(attribute_index) };
                    return Some(AttributeValueOrIndex::Value(ValueOrRef::Boolean(value)));
                }
            }
            MAP_ATTRIBUTE_VALUE_TYPE | SLICE_ATTRIBUTE_VALUE_TYPE => {
                if let Some(keys) = self.attribute_ser_keys
                    && keys.is_valid(attribute_index)
                {
                    let value_index = unsafe { keys.value_unchecked(attribute_index) };
                    return Some(AttributeValueOrIndex::ValueIndex(value_index));
                }
            }
            BYTES_ATTRIBUTE_VALUE_TYPE => {
                if let Some(keys) = self.attribute_bytes_keys
                    && keys.is_valid(attribute_index)
                {
                    let value_index = unsafe { keys.value_unchecked(attribute_index) };
                    return Some(AttributeValueOrIndex::ValueIndex(value_index));
                }
            }
            d => todo!("Attribute type '{d}' is not supported"),
        }

        None
    }

    fn get_attribute_value(
        &self,
        attribute_type: u8,
        attribute_value_index: u16,
    ) -> ValueOrRef<'static> {
        match attribute_type {
            STRING_ATTRIBUTE_VALUE_TYPE => ValueOrRef::String(StringValueOrRef::Buffer({
                let strings = self.attribute_string_values;
                let offsets = strings.value_offsets();
                let start =
                    unsafe { *offsets.get_unchecked(attribute_value_index as usize) } as usize;
                let end =
                    unsafe { *offsets.get_unchecked(attribute_value_index as usize + 1) } as usize;
                strings
                    .values()
                    .slice_with_length(start, end - start)
                    .clone()
            })),
            MAP_ATTRIBUTE_VALUE_TYPE | SLICE_ATTRIBUTE_VALUE_TYPE => {
                let value = unsafe {
                    self.attribute_ser_values
                        .unwrap()
                        .value_unchecked(attribute_value_index as usize)
                };

                // todo: Should we log deserialization failure somewhere?
                crate::serialization::from_slice(value).unwrap_or(ValueOrRef::Null)
            }
            BYTES_ATTRIBUTE_VALUE_TYPE => ValueOrRef::Array(ArrayValueOrRef::Buffer({
                let bytes = self.attribute_bytes_values.unwrap();
                let offsets = bytes.value_offsets();
                let start =
                    unsafe { *offsets.get_unchecked(attribute_value_index as usize) } as usize;
                let end =
                    unsafe { *offsets.get_unchecked(attribute_value_index as usize + 1) } as usize;
                let buffer = bytes.values().slice_with_length(start, end - start).clone();
                BufferArray::new_u8(buffer)
            })),
            d => todo!("Attribute type '{d}' is not supported"),
        }
    }
}

#[derive(Debug)]
enum AdaptiveDictionaryReader<'a, V> {
    UInt8(TypedDictionaryArray<'a, UInt8Type, V>),
    UInt16(TypedDictionaryArray<'a, UInt16Type, V>),
}

impl<'a, V: 'static> AdaptiveDictionaryReader<'a, V> {
    pub fn new(array: &'a Arc<dyn Array>) -> AdaptiveDictionaryReader<'a, V> {
        match array.data_type() {
            DataType::Dictionary(key_type, _) if key_type.as_ref() == &DataType::UInt8 => {
                AdaptiveDictionaryReader::UInt8(
                    array
                        .as_dictionary::<UInt8Type>()
                        .downcast_dict::<V>()
                        .expect("Array was an unexpected type"),
                )
            }
            DataType::Dictionary(key_type, _) if key_type.as_ref() == &DataType::UInt16 => {
                AdaptiveDictionaryReader::UInt16(
                    array
                        .as_dictionary::<UInt16Type>()
                        .downcast_dict::<V>()
                        .expect("Array was an unexpected type"),
                )
            }
            d => todo!("DataType '{d}' is not supported"),
        }
    }

    pub fn key_iter(&self) -> AdaptiveDictionaryReaderKeyIterator<'a> {
        match self {
            AdaptiveDictionaryReader::UInt8(d) => {
                AdaptiveDictionaryReaderKeyIterator::UInt8(d.keys().values().iter())
            }
            AdaptiveDictionaryReader::UInt16(d) => {
                AdaptiveDictionaryReaderKeyIterator::UInt16(d.keys().values().iter())
            }
        }
    }

    pub fn values(&self) -> &'a V {
        match self {
            AdaptiveDictionaryReader::UInt8(d) => d.values(),
            AdaptiveDictionaryReader::UInt16(d) => d.values(),
        }
    }
}

enum AdaptiveDictionaryReaderKeyIterator<'a> {
    UInt8(core::slice::Iter<'a, u8>),
    UInt16(core::slice::Iter<'a, u16>),
}

impl<'a> Iterator for AdaptiveDictionaryReaderKeyIterator<'a> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            AdaptiveDictionaryReaderKeyIterator::UInt8(i) => i.next().map(|v| *v as u16),
            AdaptiveDictionaryReaderKeyIterator::UInt16(i) => i.next().copied(),
        }
    }
}

enum AttributeValueOrIndex {
    ValueIndex(u16),
    Value(ValueOrRef<'static>),
}

#[derive(Debug)]
pub struct OtapAttributesState<'pipeline> {
    pub decoded_ids: Option<OtapDecodedIds>,
    pub id_to_record_index_map: Option<PrimitiveArray<UInt16Type>>,
    pub modified: bool,
    pub values: AHashMap<Box<str>, OtapValue<'pipeline>>,
}
