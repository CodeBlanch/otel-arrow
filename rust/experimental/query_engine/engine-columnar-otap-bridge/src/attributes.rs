// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::{OnceCell, RefCell},
    collections::hash_map::Entry,
    fmt::Display,
};

use crate::*;
use ahash::AHashMap;
use arrow::{
    array::*,
    buffer::{MutableBuffer, NullBuffer},
    datatypes::*,
};
use data_engine_columnar::*;
use otap_df_pdata::schema::consts::{self};

#[derive(Debug)]
pub struct OtapAttributes<'pipeline, 'record> {
    ids: OtapIds<'record>,
    parent_ids: OtapParentIds<'record>,
    id_to_record_index_map: OnceCell<PrimitiveArray<UInt16Type>>,
    values: RefCell<AHashMap<Box<str>, OtapValue<'pipeline>>>,
    attributes_batch: &'record RecordBatch,
    attribute_keys:
        TypedDictionaryArray<'record, UInt8Type, GenericByteArray<GenericStringType<i32>>>,
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

impl<'pipeline, 'record> OtapAttributes<'pipeline, 'record> {
    pub fn new(
        ids: OtapIds<'record>,
        attributes_batch: &'record RecordBatch,
    ) -> OtapAttributes<'pipeline, 'record> {
        Self::new_internal(
            ids,
            OtapParentIds::new(attributes_batch),
            None,
            AHashMap::new(),
            attributes_batch,
        )
    }

    pub fn from_parts(
        ids: OtapIds<'record>,
        decoded_parent_ids: Option<PrimitiveArray<UInt16Type>>,
        id_to_record_index_map: Option<PrimitiveArray<UInt16Type>>,
        values: AHashMap<Box<str>, OtapValue<'pipeline>>,
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
    }

    fn new_internal(
        ids: OtapIds<'record>,
        parent_ids: OtapParentIds<'record>,
        id_to_record_index_map: Option<PrimitiveArray<UInt16Type>>,
        values: AHashMap<Box<str>, OtapValue<'pipeline>>,
        attributes_batch: &'record RecordBatch,
    ) -> OtapAttributes<'pipeline, 'record> {
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
            values: RefCell::new(values),
            attributes_batch,
            attribute_keys: attributes_batch
                .column_by_name(consts::ATTRIBUTE_KEY)
                .expect("has keys")
                .as_dictionary::<UInt8Type>()
                .downcast_dict::<StringArray>()
                .expect("Attribute keys were an unexpected type"),
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

    pub fn into_parts(mut self) -> OtapAttributesState<'pipeline> {
        OtapAttributesState {
            decoded_ids: OtapDecodedIds {
                ids: self.ids.into_parts(),
                parent_ids: self.parent_ids.into_parts(),
            },
            id_to_record_index_map: self.id_to_record_index_map.take(),
            values: self.values.take(),
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
        /*
        pub enum AttributeValueType {
            Empty = 0,
            Str = 1,
            Int = 2,
            Double = 3,
            Bool = 4,
            Map = 5,
            Slice = 6,
            Bytes = 7,
        }
        */
        match attribute_type {
            0 => {}
            1 => {
                let keys = self.attribute_string_keys;
                if keys.is_valid(attribute_index) {
                    let value_index = unsafe { keys.value_unchecked(attribute_index) };
                    return Some(AttributeValueOrIndex::ValueIndex(value_index));
                }
            }
            2 => {
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
            3 => {
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
            4 => {
                if let Some(bools) = self.attribute_bools
                    && bools.is_valid(attribute_index)
                {
                    let value = unsafe { bools.value_unchecked(attribute_index) };
                    return Some(AttributeValueOrIndex::Value(ValueOrRef::Boolean(value)));
                }
            }
            5 | 6 => {
                if let Some(keys) = self.attribute_ser_keys
                    && keys.is_valid(attribute_index)
                {
                    let value_index = unsafe { keys.value_unchecked(attribute_index) };
                    return Some(AttributeValueOrIndex::ValueIndex(value_index));
                }
            }
            7 => {
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
        /*
        pub enum AttributeValueType {
            Empty = 0,
            Str = 1,
            Int = 2,
            Double = 3,
            Bool = 4,
            Map = 5,
            Slice = 6,
            Bytes = 7,
        }
        */
        match attribute_type {
            1 => ValueOrRef::String(StringValueOrRef::Buffer({
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
            5 | 6 => {
                let value = unsafe {
                    self.attribute_ser_values
                        .unwrap()
                        .value_unchecked(attribute_value_index as usize)
                };

                // todo: Should we log deserialization failure somewhere?
                crate::serialization::from_slice(value).unwrap_or(ValueOrRef::Null)
            }
            7 => ValueOrRef::Array(ArrayValueOrRef::Buffer({
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

impl<'pipeline, 'record> RecordTable<'pipeline> for OtapAttributes<'pipeline, 'record> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'pipeline, '_>> {
        let mut values = self.values.borrow_mut();

        if let Some(d) = values.get(key) {
            return match d {
                OtapValue::NotFound | OtapValue::Removed => None,
                OtapValue::Read(v) | OtapValue::Set(v) => {
                    Some(RecordTableValue::Dictionary(v.clone()))
                }
            };
        }

        let record_count = self.ids.len();

        let value = if let Some(value_index) = self
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

            let value_index = value_index as u8;
            let attribute_count = self.attribute_keys.len();

            let attribute_keys = self.attribute_keys.keys().values().as_ptr();
            let attribute_types = self.attribute_types.values().as_ptr();
            let attribute_parent_ids = self.parent_ids.get_ids().values().as_ptr();
            let id_to_record_index_map = self.get_id_to_record_index_map().values().as_ptr();

            for attribute_index in 0..attribute_count {
                if unsafe { *attribute_keys.add(attribute_index) } == value_index {
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
                    NullBufferBuilder::new_from_buffer(null_buffer, record_count).finish(),
                )
                .into()
            } else {
                PrimitiveArray::<UInt16Type>::new(key_buffer.into(), None).into()
            };

            Dictionary::new(keys, DictionaryValueArray::Vec(values.into()))
        } else {
            let key_buffer = MutableBuffer::from_len_zeroed(record_count * 2);

            Dictionary::new(
                PrimitiveArray::<UInt16Type>::new(
                    key_buffer.into(),
                    Some(NullBuffer::new_null(record_count)),
                )
                .into(),
                DictionaryValueArray::Vec(vec![].into()),
            )
        };

        let copy = value.clone();

        values.insert(key.into(), OtapValue::Read(value));

        Some(RecordTableValue::Dictionary(copy))
    }
}

impl Display for OtapAttributes<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Attributes(RecordCount={})",
            self.attributes_batch.num_rows()
        )
    }
}

enum AttributeValueOrIndex {
    ValueIndex(u16),
    Value(ValueOrRef<'static>),
}

#[derive(Debug)]
pub struct OtapAttributesState<'pipeline> {
    pub decoded_ids: OtapDecodedIds,
    pub id_to_record_index_map: Option<PrimitiveArray<UInt16Type>>,
    pub values: AHashMap<Box<str>, OtapValue<'pipeline>>,
}
