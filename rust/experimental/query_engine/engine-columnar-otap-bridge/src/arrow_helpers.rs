// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{hash::Hash, rc::Rc, sync::Arc};

use ahash::{AHashMap, RandomState};
use arrow::{
    array::*,
    buffer::{BooleanBuffer, MutableBuffer},
    datatypes::*,
    util::bit_util,
};
use data_engine_columnar::*;
use data_engine_expressions::*;
use indexmap::IndexSet;
use otap_df_pdata::schema::{FieldExt, consts};

use crate::*;

pub(crate) fn set_column<'a, TDiagnosticReceiver: ColumnarEngineDiagnosticReceiver<'a>>(
    diagnostic_receiver: &TDiagnosticReceiver,
    expression: &'a dyn Expression,
    batch: RecordBatch,
    column_name: &str,
    value: Dictionary,
    array_transform: fn(DictionaryKeyArray, DictionaryValueArray) -> Option<Arc<dyn Array>>,
) -> RecordBatch {
    let (keys, values) = value.into_parts();

    let transformed_values = array_transform(keys, values);

    write_column_values_to_batch(
        diagnostic_receiver,
        expression,
        batch,
        column_name,
        transformed_values,
    )
}

pub(crate) fn remove_column<'a, TDiagnosticReceiver: ColumnarEngineDiagnosticReceiver<'a>>(
    diagnostic_receiver: &TDiagnosticReceiver,
    expression: &'a dyn Expression,
    batch: RecordBatch,
    column_name: &str,
) -> RecordBatch {
    if let Some((column_id, _)) = batch.schema_ref().column_with_name(column_name) {
        diagnostic_receiver.add_diagnostic_if_enabled(
            ColumnarEngineDiagnosticLevel::Info,
            expression,
            || format!("Column '{column_name}' removed",),
        );

        let (mut schema, mut columns, count) = batch.into_parts();

        let mut schema_builder = SchemaBuilder::from(schema.fields().clone());

        schema_builder.remove(column_id);
        columns.remove(column_id);

        schema = Arc::new(schema_builder.finish());

        unsafe { RecordBatch::new_unchecked(schema, columns, count) }
    } else {
        batch
    }
}

pub(crate) fn write_column_values_to_batch<
    'a,
    TDiagnosticReceiver: ColumnarEngineDiagnosticReceiver<'a>,
>(
    diagnostic_receiver: &TDiagnosticReceiver,
    expression: &'a dyn Expression,
    batch: RecordBatch,
    column_name: &str,
    transformed_values: Option<Arc<dyn Array>>,
) -> RecordBatch {
    let values = match transformed_values {
        None => {
            return remove_column(diagnostic_receiver, expression, batch, column_name);
        }
        Some(values) => values,
    };

    if diagnostic_receiver.is_diagnostic_level_enabled(ColumnarEngineDiagnosticLevel::Info) {
        let null_count = values.null_count();

        diagnostic_receiver.add_diagnostic(ColumnarEngineDiagnostic::new(
            ColumnarEngineDiagnosticLevel::Info,
            expression,
            format!(
                "Column '{column_name}' updated [{} valid row(s), {} null row(s)]",
                values.len() - null_count,
                null_count
            ),
        ));
    }

    let (mut schema, mut columns, count) = batch.into_parts();

    let mut schema_builder = SchemaBuilder::from(schema.fields().clone());

    let field = Field::new(column_name, values.data_type().clone(), true);

    if let Some((column_id, _)) = schema.column_with_name(column_name) {
        *schema_builder.field_mut(column_id) = field.into();
        columns[column_id] = values;
    } else {
        schema_builder.push(field);
        columns.push(values);
    }

    schema = Arc::new(schema_builder.finish());

    unsafe { RecordBatch::new_unchecked(schema, columns, count) }
}

pub(crate) fn adaptive_dictionary_reader<V: Array + 'static>(
    array: &Arc<dyn Array>,
) -> Option<Dictionary<'static>> {
    Some(match array.data_type() {
        DataType::Dictionary(d, _) => match d.as_ref() {
            DataType::UInt8 => array
                .as_dictionary::<UInt8Type>()
                .downcast_dict::<V>()
                .expect("array values were an unexpected type")
                .into(),
            DataType::UInt16 => array
                .as_dictionary::<UInt16Type>()
                .downcast_dict::<V>()
                .expect("array values were an unexpected type")
                .into(),
            d => panic!("array values with '{d}' keys are not supported"),
        },
        d => panic!("array values with '{d}' keys are not supported"),
    })
}

pub(crate) fn primitive_array_reader<T: ArrowPrimitiveType>(
    array: &Arc<dyn Array>,
) -> Option<Dictionary<'static>> {
    Some(Dictionary::from_array::<UInt16Type, _>(
        array.as_primitive::<T>(),
    ))
}

pub(crate) fn adaptive_dictionary_writer<'a, T: Array + 'static, FTransform>(
    keys: DictionaryKeyArray,
    values: DictionaryValueArray<'a>,
    transform: FTransform,
) -> Option<Arc<dyn Array>>
where
    FTransform: Fn(DictionaryValueArray<'a>) -> (T, IndexLookup),
{
    let (transformed_values, lookup) = transform(values);

    Some(match transformed_values.len() {
        v if v < u8::MAX as usize => Arc::new(DictionaryArray::<UInt8Type>::new(
            keys.transform_into_key_array(lookup),
            Arc::new(transformed_values),
        )),
        _ => Arc::new(DictionaryArray::<UInt16Type>::new(
            keys.transform_into_key_array(lookup),
            Arc::new(transformed_values),
        )),
    })
}

pub(crate) fn primitive_array_writer<'a, T: ArrowPrimitiveType, FTransform>(
    keys: DictionaryKeyArray,
    values: DictionaryValueArray<'a>,
    transform: FTransform,
) -> Option<Arc<dyn Array>>
where
    T::Native: Hash + Eq + TryFrom<i64>,
    PrimitiveArray<T>: From<Vec<<T as ArrowPrimitiveType>::Native>>,
    FTransform: Fn(DictionaryValueArray<'a>) -> (PrimitiveArray<T>, IndexLookup),
{
    let (transformed_values, lookup) = transform(values);

    let key_length = keys.len();

    if transformed_values.len() == key_length {
        return Some(Arc::new(transformed_values));
    }

    let mut builder = PrimitiveBuilder::<T>::with_capacity(key_length);

    for key_index in 0..key_length {
        if let Some(value_index) = keys.get_value_index_for_key_index(key_index) {
            let transformed_value_index = match lookup.as_ref() {
                Some(lookup) => lookup.get(&value_index).and_then(|v| *v),
                None => Some(value_index),
            };

            if let Some(transformed_value_index) = transformed_value_index {
                builder.append_value(unsafe {
                    transformed_values.value_unchecked(transformed_value_index)
                });
                continue;
            }
        }

        builder.append_null();
    }

    Some(Arc::new(builder.finish()))
}

pub(crate) fn attributes_writer<'a>(
    record_count: usize,
    values: AHashMap<Box<str>, OtapValue<'a>>,
    attributes_batch: Option<OtapAttributesBatch<'_>>,
) -> Option<(Arc<dyn Array>, RecordBatch)> {
    let upsert_nulls = AHashMap::from_iter(values.iter().filter_map(|(k, v)| {
        if let OtapValue::Set(dictionary) = v {
            debug_assert!(!dictionary.is_empty() && !dictionary.is_null());
            Some((k, (dictionary.len(), dictionary.nulls())))
        } else {
            None
        }
    }));

    let upsert_attribute_count = upsert_nulls
        .iter()
        .map(|(_, (len, nulls))| len - nulls.as_ref().map_or(0, |v| v.null_count()))
        .sum();

    let attributes_builder = if let Some(attributes_batch) = attributes_batch {
        process_attributes_batch(
            record_count,
            attributes_batch,
            upsert_nulls.len(),
            upsert_nulls.keys().map(|v| v.len()).sum(),
            upsert_attribute_count,
            &values,
        )
    } else {
        None
    };

    if upsert_attribute_count > 0 {
        let mut attributes_builder = attributes_builder.unwrap_or_else(|| {
            AttributesBuilder::new(
                record_count,
                upsert_nulls.len(),
                upsert_nulls.keys().map(|v| v.len()).sum(),
                upsert_attribute_count,
            )
        });

        for (key, value) in values {
            if let OtapValue::Set(value) = value {
                debug_assert!(!value.is_empty() && !value.is_null());

                let (keys, values) = value.into_parts();
                if let DictionaryKeyArray::SingleValue {
                    data_type: _,
                    length: _,
                    value_index,
                } = keys
                {
                    if let Some(value_index) = value_index {
                        attributes_builder.push_key_value_for_all_records(
                            key.as_ref(),
                            values.get_value_at(value_index),
                        );
                    }
                    continue;
                } else {
                    let key_index = attributes_builder.push_key(&key);

                    for record_index in 0..keys.len() {
                        let value = match keys.get_value_index_for_key_index(record_index) {
                            None => continue,
                            Some(v) => values.get_value_at(v),
                        };

                        attributes_builder.push_value_for_key(record_index, key_index, value);
                    }
                }
            }
        }

        Some(attributes_builder.finish())
    } else {
        attributes_builder.map(|v| v.finish())
    }
}

fn process_attributes_batch<'a>(
    record_count: usize,
    attributes_batch: OtapAttributesBatch<'_>,
    upsert_key_count: usize,
    upsert_key_capacity: usize,
    upsert_attribute_count: usize,
    values: &AHashMap<Box<str>, OtapValue<'a>>,
) -> Option<AttributesBuilder<'a>> {
    let attribute_keys = &attributes_batch.attribute_keys;
    let attribute_key_values = attribute_keys.values();

    let attribute_key_count = attribute_key_values.len();
    let mut keys_to_skip = None;
    let mut key_capacity = 0;

    for (key_index, key) in attribute_key_values.iter().enumerate() {
        if let Some(key) = key {
            match values.get(key) {
                None | Some(OtapValue::Read(_)) => key_capacity += key.len(),
                _ => {
                    let keys_to_skip = keys_to_skip
                        .get_or_insert_with(|| MutableBuffer::new_null(attribute_key_count));
                    bit_util::set_bit(keys_to_skip, key_index);
                }
            }
        }
    }

    if key_capacity == 0 {
        // Nothing to keep from attributes batch
        return None;
    }

    if let Some(keys_to_skip) = keys_to_skip {
        // Slow path where we need to remove attributes

        let keys_to_skip = BooleanBuffer::new(keys_to_skip.into(), 0, attribute_key_count);

        let attribute_count = attributes_batch.len();

        let mut attributes_to_keep = BooleanBufferBuilder::new(attribute_count);

        let mut segment_val = false;
        let mut segment_len = 0usize;

        for key_value_index in attributes_batch.attribute_keys.key_iter() {
            let skip = keys_to_skip.value(key_value_index);

            if segment_val != skip {
                if segment_len > 0 {
                    attributes_to_keep.append_n(segment_len, !segment_val);
                }
                segment_val = skip;
                segment_len = 0;
            }

            segment_len += 1;
        }

        if segment_len > 0 {
            attributes_to_keep.append_n(segment_len, !segment_val);
        }

        let attributes_to_keep = attributes_to_keep.build();

        let mut attributes_builder = AttributesBuilder::new(
            record_count,
            upsert_key_count + attributes_batch.attribute_keys.values().len()
                - keys_to_skip.count_set_bits(),
            key_capacity + upsert_key_capacity,
            attributes_to_keep.count_set_bits() + upsert_attribute_count,
        );

        let key_mapping = attributes_builder.push_keys(&attributes_batch, &keys_to_skip);

        attributes_builder.push_existing_attributes(
            &attributes_batch,
            &attributes_to_keep,
            &key_mapping,
        );

        return Some(attributes_builder);
    }

    // Fast path where we are keeping everything

    debug_assert!(upsert_key_count > 0);
    debug_assert!(upsert_attribute_count > 0);

    let attribute_count = attributes_batch.len();

    let attributes_builder = AttributesBuilder::new_from_existing(
        record_count,
        upsert_key_count + attributes_batch.attribute_keys.values().len(),
        key_capacity + upsert_key_capacity,
        attribute_count + upsert_attribute_count,
        attributes_batch,
    );

    Some(attributes_builder)
}

#[derive(Debug)]
struct AttributesBuilder<'a> {
    pub ids_array: (MutableBuffer, Option<MutableBuffer>),
    pub types_array_buffer: MutableBuffer,
    pub parent_ids_array_buffer: MutableBuffer,
    pub keys_array_u16: bool,
    pub keys_array_buffer: MutableBuffer,
    pub keys_value_builder: StringBuilder,
    pub strings_dict: Option<(
        MutableBuffer,
        Option<MutableBuffer>,
        GenericSet<StringValueOrRef<'a>>,
    )>,
    pub ints_dict: Option<(MutableBuffer, Option<MutableBuffer>, GenericSet<i64>)>,
    pub doubles_array: Option<(MutableBuffer, Option<MutableBuffer>)>,
    pub bools_array: Option<(MutableBuffer, Option<MutableBuffer>)>,
    pub sers_dict: Option<(
        MutableBuffer,
        Option<MutableBuffer>,
        GenericSet<VecOrBuffer>,
    )>,
    pub bytes_dict: Option<(
        MutableBuffer,
        Option<MutableBuffer>,
        GenericSet<BufferWrapper<u8>>,
    )>,
    pub attribute_position: usize,
    pub next_parent_id: u16,
}

impl<'a> AttributesBuilder<'a> {
    pub fn new(
        record_count: usize,
        attribute_key_count: usize,
        attribute_key_capacity: usize,
        attribute_count: usize,
    ) -> AttributesBuilder<'a> {
        let (keys_array_buffer, keys_array_u16) = if attribute_key_count <= u8::MAX as usize {
            (MutableBuffer::from_len_zeroed(attribute_count), false)
        } else {
            (MutableBuffer::from_len_zeroed(attribute_count * 2), true)
        };

        Self {
            ids_array: (
                MutableBuffer::from_len_zeroed(record_count * 2),
                Some(MutableBuffer::new_null(record_count)),
            ),
            types_array_buffer: MutableBuffer::from_len_zeroed(attribute_count),
            parent_ids_array_buffer: MutableBuffer::from_len_zeroed(attribute_count * 2),
            keys_array_u16,
            keys_array_buffer,
            keys_value_builder: StringBuilder::with_capacity(
                attribute_key_count,
                attribute_key_capacity,
            ),
            strings_dict: None,
            ints_dict: None,
            doubles_array: None,
            bools_array: None,
            sers_dict: None,
            bytes_dict: None,
            attribute_position: 0,
            next_parent_id: 0,
        }
    }

    pub fn new_from_existing(
        record_count: usize,
        attribute_key_count: usize,
        attribute_key_capacity: usize,
        attribute_count: usize,
        attributes_batch: OtapAttributesBatch,
    ) -> AttributesBuilder<'a> {
        let existing_attributes_count = attributes_batch.len();

        let existing_ids = attributes_batch.ids.get_ids();
        let mut ids_keys = MutableBuffer::from_len_zeroed(record_count * 2);
        let mut ids_nulls = None;
        unsafe {
            AttributesBuilder::fill_from_slice_unchecked(
                existing_ids.values().to_byte_slice(),
                ids_keys.as_slice_mut(),
                0,
                record_count * 2,
            );
            if let Some(nulls) = existing_ids.nulls() {
                let mut null_buffer = MutableBuffer::new_null(record_count);
                AttributesBuilder::fill_from_slice_unchecked(
                    nulls.validity(),
                    null_buffer.as_slice_mut(),
                    0,
                    bit_util::ceil(record_count, 8),
                );
                ids_nulls = Some(null_buffer);
            }
        }

        let mut types_array_buffer = MutableBuffer::from_len_zeroed(attribute_count);
        unsafe {
            AttributesBuilder::fill_from_slice_unchecked(
                attributes_batch.attribute_types.values(),
                types_array_buffer.as_slice_mut(),
                0,
                existing_attributes_count,
            );
        };

        let (mut keys_array_buffer, keys_array_u16) = if attribute_key_count <= u8::MAX as usize {
            (MutableBuffer::from_len_zeroed(attribute_count), false)
        } else {
            (MutableBuffer::from_len_zeroed(attribute_count * 2), true)
        };

        let existing_attributes_keys = attributes_batch.attribute_keys;

        if existing_attributes_keys.keys_u16() == keys_array_u16 {
            unsafe {
                AttributesBuilder::fill_from_slice_unchecked(
                    existing_attributes_keys.keys_slice(),
                    keys_array_buffer.as_slice_mut(),
                    0,
                    if keys_array_u16 {
                        existing_attributes_count * 2
                    } else {
                        existing_attributes_count
                    },
                )
            };
        } else if keys_array_u16 {
            let keys = keys_array_buffer.typed_data_mut::<u16>().as_mut_ptr();

            for (key_index, value_index) in existing_attributes_keys.key_iter().enumerate() {
                unsafe { *keys.add(key_index) = value_index as u16 };
            }
        } else {
            let keys = keys_array_buffer.as_mut_ptr();

            for (key_index, value_index) in existing_attributes_keys.key_iter().enumerate() {
                unsafe { *keys.add(key_index) = value_index as u8 };
            }
        }

        let mut keys_value_builder =
            StringBuilder::with_capacity(attribute_key_count, attribute_key_capacity);
        keys_value_builder
            .append_array(existing_attributes_keys.values())
            .expect("success");

        let mut parent_ids_array_buffer = MutableBuffer::from_len_zeroed(attribute_count * 2);
        unsafe {
            AttributesBuilder::fill_from_slice_unchecked(
                attributes_batch
                    .parent_ids
                    .get_ids()
                    .values()
                    .to_byte_slice(),
                parent_ids_array_buffer.as_slice_mut(),
                0,
                existing_attributes_count * 2,
            )
        };
        let next_parent_id = parent_ids_array_buffer.typed_data::<u16>()
            [0..existing_attributes_count]
            .iter()
            .max()
            .map_or(0, |v| v + 1);

        let strings_dict = if let Some(strings) = attributes_batch.attribute_strings {
            let (strings_values, strings_values_lookup) = DictionaryValueArray::Array(Arc::new(
                strings.values().clone(),
            ))
            .transform_into_set(&mut |v| {
                if let ValueOrRef::String(s) = v {
                    Some(s)
                } else {
                    None
                }
            });
            let mut strings_keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
            let mut strings_nulls = None;
            match strings_values_lookup {
                None => unsafe {
                    AttributesBuilder::fill_from_slice_unchecked(
                        strings.keys().values().to_byte_slice(),
                        strings_keys.as_slice_mut(),
                        0,
                        existing_attributes_count * 2,
                    );
                    if let Some(nulls) = strings.keys().nulls() {
                        let mut null_buffer = MutableBuffer::new_null(attribute_count);
                        AttributesBuilder::fill_from_slice_unchecked(
                            nulls.validity(),
                            null_buffer.as_slice_mut(),
                            0,
                            bit_util::ceil(existing_attributes_count, 8),
                        );
                        strings_nulls = Some(null_buffer);
                    }
                },
                Some(lookup) => {
                    let keys = strings_keys.typed_data_mut::<u16>();
                    for (key_index, value_index) in strings.keys().iter().enumerate() {
                        if let Some(value_index) = value_index
                            && let Some(Some(transformed_value_index)) =
                                lookup.get(&(value_index as usize))
                        {
                            keys[key_index] = *transformed_value_index as u16;
                            continue;
                        }

                        let nulls = strings_nulls
                            .get_or_insert_with(|| MutableBuffer::new_null(attribute_count));
                        bit_util::set_bit(nulls, key_index);
                    }
                    strings_nulls = strings_nulls.map(|mut v| {
                        for v in v.as_slice_mut() {
                            *v = !*v;
                        }
                        v
                    })
                }
            }
            Some((strings_keys, strings_nulls, strings_values))
        } else {
            None
        };

        let ints_dict = if let Some(ints) = attributes_batch.attribute_ints {
            let (ints_values, ints_values_lookup) = DictionaryValueArray::Array(Arc::new(
                ints.values().clone(),
            ))
            .transform_into_set(&mut |v| {
                if let ValueOrRef::Integer(i) = v {
                    Some(i)
                } else {
                    None
                }
            });
            let mut ints_keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
            let mut ints_nulls = None;
            match ints_values_lookup {
                None => unsafe {
                    AttributesBuilder::fill_from_slice_unchecked(
                        ints.keys().values().to_byte_slice(),
                        ints_keys.as_slice_mut(),
                        0,
                        existing_attributes_count * 2,
                    );
                    if let Some(nulls) = ints.keys().nulls() {
                        let mut null_buffer = MutableBuffer::new_null(attribute_count);
                        AttributesBuilder::fill_from_slice_unchecked(
                            nulls.validity(),
                            null_buffer.as_slice_mut(),
                            0,
                            bit_util::ceil(existing_attributes_count, 8),
                        );
                        ints_nulls = Some(null_buffer);
                    }
                },
                Some(lookup) => {
                    let keys = ints_keys.typed_data_mut::<u16>();
                    for (key_index, value_index) in ints.keys().iter().enumerate() {
                        if let Some(value_index) = value_index
                            && let Some(Some(transformed_value_index)) =
                                lookup.get(&(value_index as usize))
                        {
                            keys[key_index] = *transformed_value_index as u16;
                            continue;
                        }

                        let nulls = ints_nulls
                            .get_or_insert_with(|| MutableBuffer::new_null(attribute_count));
                        bit_util::set_bit(nulls, key_index);
                    }
                    ints_nulls = ints_nulls.map(|mut v| {
                        for v in v.as_slice_mut() {
                            *v = !*v;
                        }
                        v
                    })
                }
            }
            Some((ints_keys, ints_nulls, ints_values))
        } else {
            None
        };

        let doubles_array = if let Some(doubles) = attributes_batch.attribute_doubles {
            let mut keys_buffer = MutableBuffer::from_len_zeroed(attribute_count * 8);
            let mut nulls_buffer = None;
            unsafe {
                AttributesBuilder::fill_from_slice_unchecked(
                    doubles.values().to_byte_slice(),
                    keys_buffer.as_slice_mut(),
                    0,
                    existing_attributes_count * 8,
                );
                if let Some(nulls) = doubles.nulls() {
                    let mut null_buffer = MutableBuffer::new_null(attribute_count);
                    AttributesBuilder::fill_from_slice_unchecked(
                        nulls.validity(),
                        null_buffer.as_slice_mut(),
                        0,
                        bit_util::ceil(existing_attributes_count, 8),
                    );
                    nulls_buffer = Some(null_buffer);
                }
            }
            Some((keys_buffer, nulls_buffer))
        } else {
            None
        };

        let bools_array = if let Some(bools) = attributes_batch.attribute_bools {
            let mut keys_buffer =
                MutableBuffer::from_len_zeroed(bit_util::ceil(attribute_count, 8));
            let mut nulls_buffer = None;
            unsafe {
                AttributesBuilder::fill_from_slice_unchecked(
                    bools.values().values(),
                    keys_buffer.as_slice_mut(),
                    0,
                    bit_util::ceil(existing_attributes_count, 8),
                );
                if let Some(nulls) = bools.nulls() {
                    let mut null_buffer = MutableBuffer::new_null(attribute_count);
                    AttributesBuilder::fill_from_slice_unchecked(
                        nulls.validity(),
                        null_buffer.as_slice_mut(),
                        0,
                        bit_util::ceil(existing_attributes_count, 8),
                    );
                    nulls_buffer = Some(null_buffer);
                }
            }
            Some((keys_buffer, nulls_buffer))
        } else {
            None
        };

        let sers_dict = if let Some(sers) = attributes_batch.attribute_sers {
            let (sers_values, sers_values_lookup) = sers
                .values()
                .to_set(|v| v.map(|v| VecOrBuffer::Buffer(BufferWrapper::<u8>::new(v))));
            let mut sers_keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
            let mut sers_nulls = None;
            match sers_values_lookup {
                None => unsafe {
                    AttributesBuilder::fill_from_slice_unchecked(
                        sers.keys().values().to_byte_slice(),
                        sers_keys.as_slice_mut(),
                        0,
                        existing_attributes_count * 2,
                    );
                    if let Some(nulls) = sers.keys().nulls() {
                        let mut null_buffer = MutableBuffer::new_null(attribute_count);
                        AttributesBuilder::fill_from_slice_unchecked(
                            nulls.validity(),
                            null_buffer.as_slice_mut(),
                            0,
                            bit_util::ceil(existing_attributes_count, 8),
                        );
                        sers_nulls = Some(null_buffer);
                    }
                },
                Some(lookup) => {
                    let keys = sers_keys.typed_data_mut::<u16>();
                    for (key_index, value_index) in sers.keys().iter().enumerate() {
                        if let Some(value_index) = value_index
                            && let Some(Some(transformed_value_index)) =
                                lookup.get(&(value_index as usize))
                        {
                            keys[key_index] = *transformed_value_index as u16;
                            continue;
                        }

                        let nulls = sers_nulls
                            .get_or_insert_with(|| MutableBuffer::new_null(attribute_count));
                        bit_util::set_bit(nulls, key_index);
                    }
                    sers_nulls = sers_nulls.map(|mut v| {
                        for v in v.as_slice_mut() {
                            *v = !*v;
                        }
                        v
                    })
                }
            }
            Some((sers_keys, sers_nulls, sers_values))
        } else {
            None
        };

        let bytes_dict = if let Some(bytes) = attributes_batch.attribute_bytes {
            let (bytes_values, bytes_values_lookup) =
                bytes.values().to_set(|v| v.map(BufferWrapper::<u8>::new));
            let mut bytes_keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
            let mut bytes_nulls = None;
            match bytes_values_lookup {
                None => unsafe {
                    AttributesBuilder::fill_from_slice_unchecked(
                        bytes.keys().values().to_byte_slice(),
                        bytes_keys.as_slice_mut(),
                        0,
                        existing_attributes_count * 2,
                    );
                    if let Some(nulls) = bytes.keys().nulls() {
                        let mut null_buffer = MutableBuffer::new_null(attribute_count);
                        AttributesBuilder::fill_from_slice_unchecked(
                            nulls.validity(),
                            null_buffer.as_slice_mut(),
                            0,
                            bit_util::ceil(existing_attributes_count, 8),
                        );
                        bytes_nulls = Some(null_buffer);
                    }
                },
                Some(lookup) => {
                    let keys = bytes_keys.typed_data_mut::<u16>();
                    for (key_index, value_index) in bytes.keys().iter().enumerate() {
                        if let Some(value_index) = value_index
                            && let Some(Some(transformed_value_index)) =
                                lookup.get(&(value_index as usize))
                        {
                            keys[key_index] = *transformed_value_index as u16;
                            continue;
                        }

                        let nulls = bytes_nulls
                            .get_or_insert_with(|| MutableBuffer::new_null(attribute_count));
                        bit_util::set_bit(nulls, key_index);
                    }
                    bytes_nulls = bytes_nulls.map(|mut v| {
                        for v in v.as_slice_mut() {
                            *v = !*v;
                        }
                        v
                    })
                }
            }
            Some((bytes_keys, bytes_nulls, bytes_values))
        } else {
            None
        };

        Self {
            ids_array: (ids_keys, ids_nulls),
            types_array_buffer,
            parent_ids_array_buffer,
            keys_array_u16,
            keys_array_buffer,
            keys_value_builder,
            strings_dict,
            ints_dict,
            doubles_array,
            bools_array,
            sers_dict,
            bytes_dict,
            attribute_position: existing_attributes_count,
            next_parent_id,
        }
    }

    pub fn push_value_for_key(
        &mut self,
        record_index: usize,
        key_index: usize,
        value: ValueOrRef<'a>,
    ) {
        let attribute_count = self.types_array_buffer.len();
        let attribute_index = self.attribute_position;

        debug_assert!(attribute_index < attribute_count);

        self.attribute_position = attribute_index + 1;

        if self.keys_array_u16 {
            *unsafe {
                self.keys_array_buffer
                    .typed_data_mut::<u16>()
                    .get_unchecked_mut(attribute_index)
            } = key_index as u16;
        } else {
            *unsafe { self.keys_array_buffer.get_unchecked_mut(attribute_index) } = key_index as u8;
        }

        let processed_value = self.process_value(value);

        *unsafe { self.types_array_buffer.get_unchecked_mut(attribute_index) } =
            processed_value.get_attribute_type();

        let ids = self.ids_array.0.typed_data_mut::<u16>().as_mut_ptr();
        let ids_nulls = self.ids_array.1.as_mut();
        let parent_ids = &mut self
            .parent_ids_array_buffer
            .typed_data_mut::<u16>()
            .as_mut_ptr();

        if let Some(ids_nulls) = ids_nulls {
            let parent_id = if !bit_util::get_bit(ids_nulls, record_index) {
                let parent_id = self.next_parent_id;
                self.next_parent_id = parent_id + 1;
                unsafe { *ids.add(record_index) = parent_id };
                bit_util::set_bit(ids_nulls, record_index);
                parent_id
            } else {
                unsafe { *ids.add(record_index) }
            };

            unsafe { *parent_ids.add(attribute_index) = parent_id };
        } else {
            let parent_id = unsafe { *ids.add(record_index) };

            unsafe { *parent_ids.add(attribute_index) = parent_id };
        }

        match processed_value {
            ProcessedValue::String(value_index) => {
                let strings = self.strings_dict.as_mut().expect("has strings");

                unsafe {
                    if let Some(value_index) = value_index {
                        if let Some(nulls) = strings.1.as_mut() {
                            bit_util::set_bit(nulls, attribute_index);
                        }

                        *strings
                            .0
                            .typed_data_mut::<u16>()
                            .get_unchecked_mut(attribute_index) = value_index as u16;
                    } else {
                        Self::ensure_nulls_unchecked(
                            &mut strings.1,
                            attribute_count,
                            attribute_index,
                        );
                    }

                    self.update_nulls_unchecked(
                        STRING_ATTRIBUTE_VALUE_TYPE,
                        attribute_count,
                        attribute_index,
                    );
                }
            }
            ProcessedValue::Integer(value_index) => {
                let ints = self.ints_dict.as_mut().expect("has ints");

                unsafe {
                    if let Some(value_index) = value_index {
                        if let Some(nulls) = ints.1.as_mut() {
                            bit_util::set_bit(nulls, attribute_index);
                        }

                        *ints
                            .0
                            .typed_data_mut::<u16>()
                            .get_unchecked_mut(attribute_index) = value_index as u16;
                    } else {
                        Self::ensure_nulls_unchecked(&mut ints.1, attribute_count, attribute_index);
                    }

                    self.update_nulls_unchecked(
                        INT_ATTRIBUTE_VALUE_TYPE,
                        attribute_count,
                        attribute_index,
                    );
                }
            }
            ProcessedValue::Double(value) => {
                let doubles = self.doubles_array.as_mut().expect("has doubles");

                if let Some(value) = value {
                    if let Some(nulls) = doubles.1.as_mut() {
                        bit_util::set_bit(nulls, attribute_index);
                    }

                    *unsafe {
                        doubles
                            .0
                            .typed_data_mut::<f64>()
                            .get_unchecked_mut(attribute_index)
                    } = value;
                } else {
                    unsafe {
                        Self::ensure_nulls_unchecked(
                            &mut doubles.1,
                            attribute_count,
                            attribute_index,
                        )
                    };
                }

                unsafe {
                    self.update_nulls_unchecked(
                        DOUBLE_ATTRIBUTE_VALUE_TYPE,
                        attribute_count,
                        attribute_index,
                    );
                }
            }
            ProcessedValue::Boolean(value) => {
                let bools = self.bools_array.as_mut().expect("has bools");

                if value {
                    bit_util::set_bit(bools.0.as_mut(), attribute_index)
                }

                if let Some(nulls) = bools.1.as_mut() {
                    bit_util::set_bit(nulls, attribute_index);
                }

                unsafe {
                    self.update_nulls_unchecked(
                        BOOL_ATTRIBUTE_VALUE_TYPE,
                        attribute_count,
                        attribute_index,
                    );
                }
            }
            ProcessedValue::Bytes(value_index) => {
                let bytes = self.bytes_dict.as_mut().expect("has bytes");

                unsafe {
                    if let Some(value_index) = value_index {
                        if let Some(nulls) = bytes.1.as_mut() {
                            bit_util::set_bit(nulls, attribute_index);
                        }

                        *bytes
                            .0
                            .typed_data_mut::<u16>()
                            .get_unchecked_mut(attribute_index) = value_index as u16;
                    } else {
                        Self::ensure_nulls_unchecked(
                            &mut bytes.1,
                            attribute_count,
                            attribute_index,
                        );
                    }

                    self.update_nulls_unchecked(
                        BYTES_ATTRIBUTE_VALUE_TYPE,
                        attribute_count,
                        attribute_index,
                    );
                }
            }
            ProcessedValue::Slice(value_index) | ProcessedValue::Map(value_index) => {
                let sers = self.sers_dict.as_mut().expect("has sers");

                unsafe {
                    if let Some(value_index) = value_index {
                        if let Some(nulls) = sers.1.as_mut() {
                            bit_util::set_bit(nulls, attribute_index);
                        }

                        *sers
                            .0
                            .typed_data_mut::<u16>()
                            .get_unchecked_mut(attribute_index) = value_index as u16;
                    } else {
                        Self::ensure_nulls_unchecked(&mut sers.1, attribute_count, attribute_index);
                    }

                    self.update_nulls_unchecked(
                        SLICE_ATTRIBUTE_VALUE_TYPE,
                        attribute_count,
                        attribute_index,
                    );
                }
            }
            ProcessedValue::Empty => unsafe {
                self.update_nulls_unchecked(
                    EMPTY_ATTRIBUTE_VALUE_TYPE,
                    attribute_count,
                    attribute_index,
                )
            },
        }
    }

    pub fn push_key_value_for_all_records(&mut self, key: &str, value: ValueOrRef<'a>) {
        let record_count = self.ids_array.0.len() / 2;
        let attribute_count = self.types_array_buffer.len();

        debug_assert!(self.attribute_position + record_count <= attribute_count);

        let attribute_range = self.attribute_position..(self.attribute_position + record_count);

        let key_index = self.push_key(key);

        if self.keys_array_u16 {
            let keys = &mut self.keys_array_buffer.typed_data_mut::<u16>()[attribute_range.clone()];
            keys.fill(key_index as u16);
        } else {
            let keys = &mut self.keys_array_buffer[attribute_range.clone()];
            keys.fill(key_index as u8);
        }

        let processed_value = self.process_value(value);

        self.types_array_buffer[attribute_range.clone()].fill(processed_value.get_attribute_type());

        let ids = self.ids_array.0.typed_data_mut::<u16>().as_mut_ptr();
        let ids_nulls = self.ids_array.1.as_mut();

        let parent_ids = &mut self.parent_ids_array_buffer.typed_data_mut::<u16>()
            [self.attribute_position..]
            .as_mut_ptr();

        if let Some(ids_nulls) = ids_nulls {
            for record_index in 0..record_count {
                let parent_id = if !bit_util::get_bit(ids_nulls, record_index) {
                    let parent_id = self.next_parent_id;
                    self.next_parent_id = parent_id + 1;
                    unsafe { *ids.add(record_index) = parent_id };
                    bit_util::set_bit(ids_nulls, record_index);
                    parent_id
                } else {
                    unsafe { *ids.add(record_index) }
                };

                unsafe { *parent_ids.add(record_index) = parent_id };
            }
        } else {
            for record_index in 0..record_count {
                let parent_id = unsafe { *ids.add(record_index) };

                unsafe { *parent_ids.add(record_index) = parent_id };
            }
        }

        match processed_value {
            ProcessedValue::String(value_index) => unsafe {
                Self::fill_dictionay_range_unchecked(
                    attribute_count,
                    &attribute_range,
                    value_index,
                    &mut self.strings_dict,
                );

                self.update_nulls_unchecked(
                    STRING_ATTRIBUTE_VALUE_TYPE,
                    attribute_count,
                    self.attribute_position,
                );
            },
            ProcessedValue::Integer(value_index) => unsafe {
                Self::fill_dictionay_range_unchecked(
                    attribute_count,
                    &attribute_range,
                    value_index,
                    &mut self.ints_dict,
                );

                self.update_nulls_unchecked(
                    INT_ATTRIBUTE_VALUE_TYPE,
                    attribute_count,
                    self.attribute_position,
                );
            },
            ProcessedValue::Double(value) => {
                let doubles = self.doubles_array.as_mut().expect("has doubles");
                if let Some(value) = value {
                    let keys = doubles.0.typed_data_mut::<f64>();
                    keys[attribute_range.clone()].fill(value);
                    if let Some(nulls) = &mut doubles.1 {
                        unsafe {
                            Self::fill_bit_range_unchecked(
                                nulls,
                                attribute_range.start,
                                attribute_range.end,
                            )
                        };
                    }
                } else {
                    unsafe {
                        Self::ensure_nulls_unchecked(
                            &mut doubles.1,
                            attribute_count,
                            self.attribute_position,
                        )
                    };
                }

                unsafe {
                    self.update_nulls_unchecked(
                        DOUBLE_ATTRIBUTE_VALUE_TYPE,
                        attribute_count,
                        self.attribute_position,
                    );
                }
            }
            ProcessedValue::Boolean(value) => {
                let bools = self.bools_array.as_mut().expect("has bools");

                if value {
                    unsafe {
                        Self::fill_bit_range_unchecked(
                            &mut bools.0,
                            attribute_range.start,
                            attribute_range.end,
                        )
                    };
                }

                if let Some(nulls) = &mut bools.1 {
                    unsafe {
                        Self::fill_bit_range_unchecked(
                            nulls,
                            attribute_range.start,
                            attribute_range.end,
                        )
                    };
                }

                unsafe {
                    self.update_nulls_unchecked(
                        BOOL_ATTRIBUTE_VALUE_TYPE,
                        attribute_count,
                        self.attribute_position,
                    );
                }
            }
            ProcessedValue::Bytes(value_index) => unsafe {
                Self::fill_dictionay_range_unchecked(
                    attribute_count,
                    &attribute_range,
                    value_index,
                    &mut self.bytes_dict,
                );

                self.update_nulls_unchecked(
                    BYTES_ATTRIBUTE_VALUE_TYPE,
                    attribute_count,
                    self.attribute_position,
                );
            },
            ProcessedValue::Slice(value_index) | ProcessedValue::Map(value_index) => unsafe {
                Self::fill_dictionay_range_unchecked(
                    attribute_count,
                    &attribute_range,
                    value_index,
                    &mut self.sers_dict,
                );

                self.update_nulls_unchecked(
                    SLICE_ATTRIBUTE_VALUE_TYPE,
                    attribute_count,
                    self.attribute_position,
                );
            },
            ProcessedValue::Empty => unsafe {
                self.update_nulls_unchecked(
                    EMPTY_ATTRIBUTE_VALUE_TYPE,
                    attribute_count,
                    self.attribute_position,
                );
            },
        }

        self.attribute_position += record_count;
    }

    fn process_value(&mut self, value: ValueOrRef<'a>) -> ProcessedValue {
        let attribute_count = self.types_array_buffer.len();

        match value {
            ValueOrRef::Null => ProcessedValue::Empty,
            ValueOrRef::String(s) => {
                if s.is_empty() {
                    ProcessedValue::String(None)
                } else {
                    let value_index = self
                        .strings_dict
                        .get_or_insert_with(|| {
                            let keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
                            (keys, None, IndexSet::with_hasher(RandomState::new()))
                        })
                        .2
                        .insert_full(s)
                        .0;
                    ProcessedValue::String(Some(value_index))
                }
            }
            ValueOrRef::Integer(i) => {
                if i == 0 {
                    ProcessedValue::Integer(None)
                } else {
                    let value_index = self
                        .ints_dict
                        .get_or_insert_with(|| {
                            let keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
                            (keys, None, IndexSet::with_hasher(RandomState::new()))
                        })
                        .2
                        .insert_full(i)
                        .0;
                    ProcessedValue::Integer(Some(value_index))
                }
            }
            ValueOrRef::Double(d) => {
                if d == 0f64 {
                    ProcessedValue::Double(None)
                } else {
                    self.doubles_array.get_or_insert_with(|| {
                        let keys = MutableBuffer::from_len_zeroed(attribute_count * 8);
                        (keys, None)
                    });
                    ProcessedValue::Double(Some(d))
                }
            }
            ValueOrRef::Boolean(b) => {
                self.bools_array.get_or_insert_with(|| {
                    let keys = MutableBuffer::new_null(attribute_count);
                    (keys, None)
                });
                ProcessedValue::Boolean(b)
            }
            ValueOrRef::Array(a) => match a {
                ArrayValueOrRef::Buffer(BufferArray::U8(b)) => {
                    if b.is_empty() {
                        ProcessedValue::Bytes(None)
                    } else {
                        let value_index = self
                            .bytes_dict
                            .get_or_insert_with(|| {
                                let keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
                                (keys, None, IndexSet::with_hasher(RandomState::new()))
                            })
                            .2
                            .insert_full(b)
                            .0;
                        ProcessedValue::Bytes(Some(value_index))
                    }
                }
                a => {
                    if a.is_empty() {
                        ProcessedValue::Slice(None)
                    } else {
                        match crate::serialization::to_slice(ValueOrRef::Array(a)) {
                            Ok(v) => self.process_slice_value(VecOrBuffer::Vec(v), attribute_count),
                            Err(_) => ProcessedValue::Slice(None),
                        }
                    }
                }
            },
            ValueOrRef::Map(m) => {
                if m.as_map_value().is_empty() {
                    ProcessedValue::Map(None)
                } else {
                    match crate::serialization::to_slice(ValueOrRef::Map(m)) {
                        Ok(v) => self.process_map_value(VecOrBuffer::Vec(v), attribute_count),
                        Err(_) => ProcessedValue::Map(None),
                    }
                }
            }
            v => {
                let s = v.to_value().convert_to_string();
                if s.as_ref().is_empty() {
                    ProcessedValue::String(None)
                } else {
                    let value_index = self
                        .strings_dict
                        .get_or_insert_with(|| {
                            let keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
                            (keys, None, IndexSet::with_hasher(RandomState::new()))
                        })
                        .2
                        .insert_full(StringValueOrRef::Owned(Rc::new(s.into())))
                        .0;
                    ProcessedValue::String(Some(value_index))
                }
            }
        }
    }
}

impl AttributesBuilder<'_> {
    pub fn push_keys(
        &mut self,
        attributes_batch: &OtapAttributesBatch,
        keys_to_skip: &BooleanBuffer,
    ) -> Vec<Option<usize>> {
        let mut key_mappings = vec![None; keys_to_skip.len()];

        let keys = attributes_batch.attribute_keys.values();

        for (key_index, skip) in keys_to_skip.iter().enumerate() {
            if skip {
                continue;
            }

            let new_key_index = self.keys_value_builder.len();
            self.keys_value_builder
                .append_value(unsafe { keys.value_unchecked(key_index) });
            *unsafe { key_mappings.get_unchecked_mut(key_index) } = Some(new_key_index);
        }

        key_mappings
    }

    pub fn push_existing_attributes(
        &mut self,
        attributes_batch: &OtapAttributesBatch,
        attributes_to_keep: &BooleanBuffer,
        key_mapping: &[Option<usize>],
    ) {
        let attributes_to_add_count = attributes_to_keep.count_set_bits();
        let attribute_count = self.types_array_buffer.len();

        let attribute_types: &[u8] = attributes_batch.attribute_types.values();
        let attribute_keys = &attributes_batch.attribute_keys;
        let attribute_parent_ids = attributes_batch.parent_ids.get_ids().values().as_ptr();
        let id_to_record_index_map = attributes_batch
            .get_id_to_record_index_map()
            .values()
            .as_ptr();

        debug_assert!(self.attribute_position + attributes_to_add_count <= attribute_types.len());

        for (start, end) in attributes_to_keep.set_slices() {
            unsafe {
                Self::fill_from_slice_unchecked(
                    &attribute_types[start..end],
                    self.types_array_buffer.as_mut(),
                    self.attribute_position,
                    end - start,
                );
            }

            for existing_attribute_index in start..end {
                let attribute_index = self.attribute_position;
                self.attribute_position = attribute_index + 1;

                let ids_array = self.ids_array.0.typed_data_mut::<u16>();
                let parent_ids_array = self.parent_ids_array_buffer.typed_data_mut::<u16>();

                let key_index = unsafe {
                    key_mapping
                        .get_unchecked(
                            attribute_keys
                                .get_value_index_for_key_index(existing_attribute_index)
                                .expect("has key"),
                        )
                        .expect("has key mapping")
                };

                if self.keys_array_u16 {
                    let keys = self.keys_array_buffer.typed_data_mut::<u16>();
                    *unsafe { keys.get_unchecked_mut(attribute_index) } = key_index as u16;
                } else {
                    let keys = &mut self.keys_array_buffer;
                    *unsafe { keys.get_unchecked_mut(attribute_index) } = key_index as u8;
                }

                let existing_parent_id =
                    unsafe { *attribute_parent_ids.add(existing_attribute_index) };
                let record_index =
                    unsafe { *id_to_record_index_map.add(existing_parent_id as usize) } as usize;

                let new_parent_id = if let Some(ids_nulls) = self.ids_array.1.as_mut()
                    && !bit_util::get_bit(ids_nulls.as_slice(), record_index)
                {
                    let new_parent_id = self.next_parent_id;
                    *unsafe { ids_array.get_unchecked_mut(record_index) } = new_parent_id;
                    self.next_parent_id = new_parent_id + 1;
                    bit_util::set_bit(ids_nulls, record_index);
                    new_parent_id
                } else {
                    *unsafe { ids_array.get_unchecked(record_index) }
                };

                *unsafe { parent_ids_array.get_unchecked_mut(attribute_index) } = new_parent_id;

                let processed_value = match *unsafe {
                    attribute_types.get_unchecked(existing_attribute_index)
                } {
                    EMPTY_ATTRIBUTE_VALUE_TYPE => ProcessedValue::Empty,
                    STRING_ATTRIBUTE_VALUE_TYPE => {
                        if let Some(strings) = attributes_batch.attribute_strings
                            && let Some(value_index) =
                                strings.get_value_index_null_safe(existing_attribute_index)
                        {
                            let buffer = unsafe {
                                get_generic_byte_array_buffer_value_unchecked(
                                    strings.values(),
                                    value_index,
                                )
                            };

                            self.process_value(ValueOrRef::String(StringValueOrRef::Buffer(buffer)))
                        } else {
                            ProcessedValue::Empty
                        }
                    }
                    INT_ATTRIBUTE_VALUE_TYPE => {
                        if let Some(ints) = attributes_batch.attribute_ints
                            && let Some(value_index) =
                                ints.get_value_index_null_safe(existing_attribute_index)
                        {
                            self.process_value(ValueOrRef::Integer(unsafe {
                                ints.values().value_unchecked(value_index)
                            }))
                        } else {
                            ProcessedValue::Empty
                        }
                    }
                    DOUBLE_ATTRIBUTE_VALUE_TYPE => {
                        if let Some(doubles) = attributes_batch.attribute_doubles
                            && doubles.is_valid(existing_attribute_index)
                        {
                            self.process_value(ValueOrRef::Double(unsafe {
                                doubles.value_unchecked(existing_attribute_index)
                            }))
                        } else {
                            ProcessedValue::Empty
                        }
                    }
                    BOOL_ATTRIBUTE_VALUE_TYPE => {
                        if let Some(bools) = attributes_batch.attribute_bools
                            && bools.is_valid(existing_attribute_index)
                        {
                            self.process_value(ValueOrRef::Boolean(unsafe {
                                bools.value_unchecked(existing_attribute_index)
                            }))
                        } else {
                            ProcessedValue::Empty
                        }
                    }
                    SLICE_ATTRIBUTE_VALUE_TYPE => {
                        if let Some(sers) = attributes_batch.attribute_sers
                            && let Some(value_index) =
                                sers.get_value_index_null_safe(existing_attribute_index)
                        {
                            let buffer = unsafe {
                                get_generic_byte_array_buffer_value_unchecked(
                                    sers.values(),
                                    value_index,
                                )
                            };

                            self.process_slice_value(
                                VecOrBuffer::Buffer(BufferWrapper::new(buffer)),
                                attribute_count,
                            )
                        } else {
                            ProcessedValue::Empty
                        }
                    }
                    MAP_ATTRIBUTE_VALUE_TYPE => {
                        if let Some(sers) = attributes_batch.attribute_sers
                            && let Some(value_index) = sers.get_value_index_null_safe(key_index)
                        {
                            let buffer = unsafe {
                                get_generic_byte_array_buffer_value_unchecked(
                                    sers.values(),
                                    value_index,
                                )
                            };

                            self.process_map_value(
                                VecOrBuffer::Buffer(BufferWrapper::new(buffer)),
                                attribute_count,
                            )
                        } else {
                            ProcessedValue::Empty
                        }
                    }
                    BYTES_ATTRIBUTE_VALUE_TYPE => {
                        if let Some(bytes) = attributes_batch.attribute_bytes
                            && let Some(value_index) = bytes.get_value_index_null_safe(key_index)
                        {
                            let buffer = unsafe {
                                get_generic_byte_array_buffer_value_unchecked(
                                    bytes.values(),
                                    value_index,
                                )
                            };

                            self.process_value(ValueOrRef::Array(ArrayValueOrRef::Buffer(
                                BufferArray::new_u8(buffer),
                            )))
                        } else {
                            ProcessedValue::Empty
                        }
                    }
                    t => panic!("Attribute type '{t}' is not supported"),
                };

                match processed_value {
                    ProcessedValue::String(value_index) => unsafe {
                        if let Some(value_index) = value_index {
                            let strings = self.strings_dict.as_mut().expect("has strings");

                            if let Some(nulls) = strings.1.as_mut() {
                                bit_util::set_bit(nulls, attribute_index);
                            }

                            *strings
                                .0
                                .typed_data_mut::<u16>()
                                .get_unchecked_mut(attribute_index) = value_index as u16;
                        } else if let Some(strings) = self.strings_dict.as_mut() {
                            Self::ensure_nulls_unchecked(
                                &mut strings.1,
                                attribute_count,
                                attribute_index,
                            );
                        }

                        self.update_nulls_unchecked(
                            STRING_ATTRIBUTE_VALUE_TYPE,
                            attribute_count,
                            attribute_index,
                        );
                    },
                    ProcessedValue::Integer(value_index) => unsafe {
                        if let Some(value_index) = value_index {
                            let ints = self.ints_dict.as_mut().expect("has ints");

                            if let Some(nulls) = ints.1.as_mut() {
                                bit_util::set_bit(nulls, attribute_index);
                            }

                            *ints
                                .0
                                .typed_data_mut::<u16>()
                                .get_unchecked_mut(attribute_index) = value_index as u16;
                        } else if let Some(ints) = self.ints_dict.as_mut() {
                            Self::ensure_nulls_unchecked(
                                &mut ints.1,
                                attribute_count,
                                attribute_index,
                            );
                        }

                        self.update_nulls_unchecked(
                            INT_ATTRIBUTE_VALUE_TYPE,
                            attribute_count,
                            attribute_index,
                        );
                    },
                    ProcessedValue::Double(value) => {
                        if let Some(value) = value {
                            let doubles = self.doubles_array.as_mut().expect("has doubles");

                            if let Some(nulls) = doubles.1.as_mut() {
                                bit_util::set_bit(nulls, attribute_index);
                            }

                            *unsafe {
                                doubles
                                    .0
                                    .typed_data_mut::<f64>()
                                    .get_unchecked_mut(attribute_index)
                            } = value;
                        } else if let Some(doubles) = self.doubles_array.as_mut() {
                            unsafe {
                                Self::ensure_nulls_unchecked(
                                    &mut doubles.1,
                                    attribute_count,
                                    attribute_index,
                                )
                            };
                        }

                        unsafe {
                            self.update_nulls_unchecked(
                                DOUBLE_ATTRIBUTE_VALUE_TYPE,
                                attribute_count,
                                attribute_index,
                            );
                        }
                    }
                    ProcessedValue::Boolean(value) => {
                        let bools = self.bools_array.as_mut().expect("has bools");

                        if value {
                            bit_util::set_bit(bools.0.as_mut(), attribute_index)
                        }

                        if let Some(nulls) = bools.1.as_mut() {
                            bit_util::set_bit(nulls, attribute_index);
                        }

                        unsafe {
                            self.update_nulls_unchecked(
                                BOOL_ATTRIBUTE_VALUE_TYPE,
                                attribute_count,
                                attribute_index,
                            );
                        }
                    }
                    ProcessedValue::Bytes(value_index) => unsafe {
                        if let Some(value_index) = value_index {
                            let bytes = self.bytes_dict.as_mut().expect("has bytes");

                            if let Some(nulls) = bytes.1.as_mut() {
                                bit_util::set_bit(nulls, attribute_index);
                            }

                            *bytes
                                .0
                                .typed_data_mut::<u16>()
                                .get_unchecked_mut(attribute_index) = value_index as u16;
                        } else if let Some(bytes) = self.bytes_dict.as_mut() {
                            Self::ensure_nulls_unchecked(
                                &mut bytes.1,
                                attribute_count,
                                attribute_index,
                            );
                        }

                        self.update_nulls_unchecked(
                            BYTES_ATTRIBUTE_VALUE_TYPE,
                            attribute_count,
                            attribute_index,
                        );
                    },
                    ProcessedValue::Slice(value_index) | ProcessedValue::Map(value_index) => unsafe {
                        if let Some(value_index) = value_index {
                            let sers = self.sers_dict.as_mut().expect("has sers");

                            if let Some(nulls) = sers.1.as_mut() {
                                bit_util::set_bit(nulls, attribute_index);
                            }

                            *sers
                                .0
                                .typed_data_mut::<u16>()
                                .get_unchecked_mut(attribute_index) = value_index as u16;
                        } else if let Some(sers) = self.sers_dict.as_mut() {
                            Self::ensure_nulls_unchecked(
                                &mut sers.1,
                                attribute_count,
                                attribute_index,
                            );
                        }

                        self.update_nulls_unchecked(
                            SLICE_ATTRIBUTE_VALUE_TYPE,
                            attribute_count,
                            attribute_index,
                        );
                    },
                    ProcessedValue::Empty => unsafe {
                        self.update_nulls_unchecked(
                            EMPTY_ATTRIBUTE_VALUE_TYPE,
                            attribute_count,
                            attribute_index,
                        )
                    },
                }
            }
        }
    }

    pub fn push_key(&mut self, key: &str) -> usize {
        let key_index = self.keys_value_builder.len();
        self.keys_value_builder.append_value(key);
        key_index
    }

    fn process_slice_value(
        &mut self,
        value: VecOrBuffer,
        attribute_count: usize,
    ) -> ProcessedValue {
        if value.is_empty() {
            ProcessedValue::Slice(None)
        } else {
            let value_index = self
                .sers_dict
                .get_or_insert_with(|| {
                    let keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
                    (keys, None, IndexSet::with_hasher(RandomState::new()))
                })
                .2
                .insert_full(value)
                .0;
            ProcessedValue::Slice(Some(value_index))
        }
    }

    fn process_map_value(&mut self, value: VecOrBuffer, attribute_count: usize) -> ProcessedValue {
        if value.is_empty() {
            ProcessedValue::Map(None)
        } else {
            let value_index = self
                .sers_dict
                .get_or_insert_with(|| {
                    let keys = MutableBuffer::from_len_zeroed(attribute_count * 2);
                    (keys, None, IndexSet::with_hasher(RandomState::new()))
                })
                .2
                .insert_full(value)
                .0;
            ProcessedValue::Map(Some(value_index))
        }
    }

    unsafe fn update_nulls_unchecked(
        &mut self,
        except_attribute_type: u8,
        attribute_count: usize,
        attribute_position: usize,
    ) {
        unsafe {
            if except_attribute_type != STRING_ATTRIBUTE_VALUE_TYPE
                && let Some(strings) = self.strings_dict.as_mut()
            {
                Self::ensure_nulls_unchecked(&mut strings.1, attribute_count, attribute_position)
            }
            if except_attribute_type != INT_ATTRIBUTE_VALUE_TYPE
                && let Some(ints) = self.ints_dict.as_mut()
            {
                Self::ensure_nulls_unchecked(&mut ints.1, attribute_count, attribute_position)
            }
            if except_attribute_type != DOUBLE_ATTRIBUTE_VALUE_TYPE
                && let Some(doubles) = self.doubles_array.as_mut()
            {
                Self::ensure_nulls_unchecked(&mut doubles.1, attribute_count, attribute_position)
            }
            if except_attribute_type != BOOL_ATTRIBUTE_VALUE_TYPE
                && let Some(bools) = self.bools_array.as_mut()
            {
                Self::ensure_nulls_unchecked(&mut bools.1, attribute_count, attribute_position)
            }
            if except_attribute_type != BYTES_ATTRIBUTE_VALUE_TYPE
                && let Some(bytes) = self.bytes_dict.as_mut()
            {
                Self::ensure_nulls_unchecked(&mut bytes.1, attribute_count, attribute_position)
            }
            if except_attribute_type != MAP_ATTRIBUTE_VALUE_TYPE
                && except_attribute_type != SLICE_ATTRIBUTE_VALUE_TYPE
                && let Some(sers) = self.sers_dict.as_mut()
            {
                Self::ensure_nulls_unchecked(&mut sers.1, attribute_count, attribute_position)
            }
        }
    }

    unsafe fn fill_dictionay_range_unchecked<T>(
        attribute_count: usize,
        attribute_range: &std::ops::Range<usize>,
        value_index: Option<usize>,
        dictionary: &mut Option<(MutableBuffer, Option<MutableBuffer>, T)>,
    ) {
        if let Some(value_index) = value_index {
            let dictionary = dictionary.as_mut().expect("has dictionary");

            let keys = dictionary.0.typed_data_mut::<u16>();
            keys[attribute_range.clone()].fill(value_index as u16);

            if let Some(nulls) = &mut dictionary.1 {
                unsafe {
                    Self::fill_bit_range_unchecked(
                        nulls,
                        attribute_range.start,
                        attribute_range.end,
                    )
                };
            }
        } else if let Some(nulls) = dictionary.as_mut().map(|v| &mut v.1)
            && nulls.is_none()
        {
            let mut buffer = MutableBuffer::new_null(attribute_count);

            unsafe { Self::fill_bits_from_start_unchecked(&mut buffer, attribute_range.start) };

            *nulls = Some(buffer);
        }
    }

    unsafe fn ensure_nulls_unchecked(
        nulls: &mut Option<MutableBuffer>,
        attribute_count: usize,
        fill_count: usize,
    ) {
        if nulls.is_none() {
            let mut buffer = MutableBuffer::new_null(attribute_count);
            unsafe { Self::fill_bits_from_start_unchecked(&mut buffer, fill_count) };
            *nulls = Some(buffer);
        }
    }

    unsafe fn fill_bits_from_start_unchecked(buffer: &mut MutableBuffer, count: usize) {
        debug_assert!(count <= buffer.len() * 8);

        if count == 0 {
            return;
        }

        let full_bytes = count / 8;
        let remainder_bits = count % 8;

        // 1. Fill all completely filled bytes with 1s
        if full_bytes > 0 {
            buffer[0..full_bytes].fill(0xFF);
        }

        // 2. Set only the targeted lower bits (LSB) in the final partial byte
        if remainder_bits > 0 {
            let mask = (1 << remainder_bits) - 1;
            buffer[full_bytes] |= mask;
        }
    }

    unsafe fn fill_bit_range_unchecked(
        buffer: &mut [u8],
        start_bit_inclusive: usize,
        end_bit_exclusive: usize,
    ) {
        debug_assert!(start_bit_inclusive < end_bit_exclusive);
        debug_assert!(end_bit_exclusive <= buffer.len() * 8);

        let start_byte = start_bit_inclusive / 8;
        let end_byte = (end_bit_exclusive - 1) / 8;

        let start_bit = start_bit_inclusive % 8;
        let end_bit = (end_bit_exclusive - 1) % 8;

        if start_byte == end_byte {
            let mask = (((1u16 << (end_bit - start_bit + 1)) - 1) << start_bit) as u8;
            buffer[start_byte] |= mask;
            return;
        }

        // First partial byte (LSB)
        buffer[start_byte] |= !0u8 << start_bit;

        // Full bytes in the middle
        buffer[start_byte + 1..end_byte].fill(0xff);

        // Last partial byte (LSB)
        buffer[end_byte] |= (!0u8) >> (7 - end_bit);
    }

    unsafe fn fill_from_slice_unchecked(
        source: &[u8],
        destination: &mut [u8],
        destination_offset: usize,
        count: usize,
    ) {
        unsafe {
            let src = source.as_ptr();
            let dst = destination.as_mut_ptr().add(destination_offset);
            std::ptr::copy_nonoverlapping(src, dst, count)
        }
    }

    pub fn finish(mut self) -> (Arc<dyn Array>, RecordBatch) {
        let ids_array = self.ids_array;

        let record_count = ids_array.0.len() / 2;
        let attribute_count = self.types_array_buffer.len();

        let ids = PrimitiveArray::<UInt16Type>::new(
            ids_array.0.into(),
            ids_array
                .1
                .and_then(|b| NullBufferBuilder::new_from_buffer(b, record_count).build()),
        );

        let parent_ids =
            PrimitiveArray::<UInt16Type>::new(self.parent_ids_array_buffer.into(), None);

        let keys: Arc<dyn Array> = if self.keys_array_u16 {
            Arc::new(DictionaryArray::<UInt16Type>::new(
                PrimitiveArray::new(self.keys_array_buffer.into(), None),
                Arc::new(self.keys_value_builder.finish()),
            ))
        } else {
            Arc::new(DictionaryArray::<UInt8Type>::new(
                PrimitiveArray::new(self.keys_array_buffer.into(), None),
                Arc::new(self.keys_value_builder.finish()),
            ))
        };

        let types = PrimitiveArray::<UInt8Type>::new(self.types_array_buffer.into(), None);

        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(9);
        let mut fields = Vec::with_capacity(9);

        fields.push(
            Field::new(consts::PARENT_ID, parent_ids.data_type().clone(), false)
                .with_plain_encoding(),
        );
        columns.push(Arc::new(parent_ids));

        fields.push(Field::new(
            consts::ATTRIBUTE_KEY,
            keys.data_type().clone(),
            false,
        ));
        columns.push(keys);

        fields.push(Field::new(
            consts::ATTRIBUTE_TYPE,
            types.data_type().clone(),
            false,
        ));
        columns.push(Arc::new(types));

        if let Some((strings_keys, strings_nulls, strings_values)) = self.strings_dict {
            let mut values = StringBuilder::with_capacity(
                strings_values.len(),
                strings_values.iter().map(|v| v.get_value().len()).sum(),
            );

            for value in strings_values.into_iter() {
                values.append_value(value);
            }

            let strings = DictionaryArray::new(
                PrimitiveArray::<UInt16Type>::new(
                    strings_keys.into(),
                    strings_nulls.and_then(|b| {
                        NullBufferBuilder::new_from_buffer(b, attribute_count).build()
                    }),
                ),
                Arc::new(values.finish()),
            );

            fields.push(Field::new(
                consts::ATTRIBUTE_STR,
                strings.data_type().clone(),
                true,
            ));
            columns.push(Arc::new(strings));
        }

        if let Some((ints_keys, ints_nulls, ints_values)) = self.ints_dict {
            let values = PrimitiveArray::<Int64Type>::from_iter(ints_values);

            let ints = DictionaryArray::new(
                PrimitiveArray::<UInt16Type>::new(
                    ints_keys.into(),
                    ints_nulls.and_then(|b| {
                        NullBufferBuilder::new_from_buffer(b, attribute_count).build()
                    }),
                ),
                Arc::new(values),
            );

            fields.push(Field::new(
                consts::ATTRIBUTE_INT,
                ints.data_type().clone(),
                true,
            ));
            columns.push(Arc::new(ints));
        }

        if let Some((doubles_keys, doubles_nulls)) = self.doubles_array {
            let doubles = PrimitiveArray::<Float64Type>::new(
                doubles_keys.into(),
                doubles_nulls
                    .and_then(|b| NullBufferBuilder::new_from_buffer(b, attribute_count).build()),
            );

            fields.push(Field::new(
                consts::ATTRIBUTE_DOUBLE,
                doubles.data_type().clone(),
                true,
            ));
            columns.push(Arc::new(doubles));
        }

        if let Some((bool_keys, bool_nulls)) = self.bools_array {
            let bools = BooleanArray::new(
                BooleanBuffer::new(bool_keys.into(), 0, attribute_count),
                bool_nulls
                    .and_then(|b| NullBufferBuilder::new_from_buffer(b, attribute_count).build()),
            );

            fields.push(Field::new(
                consts::ATTRIBUTE_BOOL,
                bools.data_type().clone(),
                true,
            ));
            columns.push(Arc::new(bools));
        }

        if let Some((bytes_keys, bytes_nulls, bytes_values)) = self.bytes_dict {
            let bytes = DictionaryArray::new(
                PrimitiveArray::<UInt16Type>::new(
                    bytes_keys.into(),
                    bytes_nulls.and_then(|b| {
                        NullBufferBuilder::new_from_buffer(b, attribute_count).build()
                    }),
                ),
                Arc::new(BinaryArray::from(
                    bytes_values
                        .iter()
                        .map(|v| v.as_ref())
                        .collect::<Vec<&[u8]>>(),
                )),
            );

            fields.push(Field::new(
                consts::ATTRIBUTE_BYTES,
                bytes.data_type().clone(),
                true,
            ));
            columns.push(Arc::new(bytes));
        }

        if let Some((sers_keys, sers_nulls, sers_values)) = self.sers_dict {
            let sers = DictionaryArray::new(
                PrimitiveArray::<UInt16Type>::new(
                    sers_keys.into(),
                    sers_nulls.and_then(|b| {
                        NullBufferBuilder::new_from_buffer(b, attribute_count).build()
                    }),
                ),
                Arc::new(BinaryArray::from(
                    sers_values
                        .iter()
                        .map(|v| match v {
                            VecOrBuffer::Vec(v) => v,
                            VecOrBuffer::Buffer(b) => b.as_ref(),
                        })
                        .collect::<Vec<&[u8]>>(),
                )),
            );

            fields.push(Field::new(
                consts::ATTRIBUTE_SER,
                sers.data_type().clone(),
                true,
            ));
            columns.push(Arc::new(sers));
        }

        (
            Arc::new(ids),
            RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid batch"),
        )
    }
}

#[derive(Clone)]
enum ProcessedValue {
    Empty,
    String(Option<usize>),
    Integer(Option<usize>),
    Double(Option<f64>),
    Boolean(bool),
    Slice(Option<usize>),
    Map(Option<usize>),
    Bytes(Option<usize>),
}

impl ProcessedValue {
    pub fn get_attribute_type(&self) -> u8 {
        match self {
            ProcessedValue::Empty => EMPTY_ATTRIBUTE_VALUE_TYPE,
            ProcessedValue::String(_) => STRING_ATTRIBUTE_VALUE_TYPE,
            ProcessedValue::Integer(_) => INT_ATTRIBUTE_VALUE_TYPE,
            ProcessedValue::Double(_) => DOUBLE_ATTRIBUTE_VALUE_TYPE,
            ProcessedValue::Boolean(_) => BOOL_ATTRIBUTE_VALUE_TYPE,
            ProcessedValue::Slice(_) => SLICE_ATTRIBUTE_VALUE_TYPE,
            ProcessedValue::Map(_) => MAP_ATTRIBUTE_VALUE_TYPE,
            ProcessedValue::Bytes(_) => BYTES_ATTRIBUTE_VALUE_TYPE,
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq)]
enum VecOrBuffer {
    Vec(Vec<u8>),
    Buffer(BufferWrapper<u8>),
}

impl VecOrBuffer {
    pub fn is_empty(&self) -> bool {
        match self {
            VecOrBuffer::Vec(items) => items.is_empty(),
            VecOrBuffer::Buffer(buffer_wrapper) => buffer_wrapper.is_empty(),
        }
    }
}

pub(crate) trait ArrowTypedDictionaryValueIndexAccessor {
    fn get_value_index_null_safe(&self, key_index: usize) -> Option<usize>;
}

impl<K: ArrowDictionaryKeyType, V: Array> ArrowTypedDictionaryValueIndexAccessor
    for TypedDictionaryArray<'_, K, V>
{
    fn get_value_index_null_safe(&self, key_index: usize) -> Option<usize> {
        let keys = self.keys();
        if keys.is_null(key_index) {
            return None;
        }

        let value_index = K::Native::as_usize(unsafe { keys.value_unchecked(key_index) });

        if let Some(value_nulls) = self.values().nulls()
            && value_nulls.is_null(value_index)
        {
            return None;
        }

        Some(value_index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fill_bit_range_unchecked() {
        let mut buffer = MutableBuffer::from_len_zeroed(8);

        unsafe { AttributesBuilder::fill_bit_range_unchecked(&mut buffer, 0, 1) };

        assert_eq!(&[0x01, 0, 0, 0, 0, 0, 0, 0], buffer.to_byte_slice());

        unsafe { AttributesBuilder::fill_bit_range_unchecked(&mut buffer, 0, 2) };

        assert_eq!(&[0x03, 0, 0, 0, 0, 0, 0, 0], buffer.to_byte_slice());

        unsafe { AttributesBuilder::fill_bit_range_unchecked(&mut buffer, 7, 8) };

        assert_eq!(&[0x83, 0, 0, 0, 0, 0, 0, 0], buffer.to_byte_slice());

        unsafe { AttributesBuilder::fill_bit_range_unchecked(&mut buffer, 8, 17) };

        assert_eq!(&[0x83, 0xFF, 0x01, 0, 0, 0, 0, 0], buffer.to_byte_slice());

        unsafe { AttributesBuilder::fill_bit_range_unchecked(&mut buffer, 25, 26) };

        assert_eq!(
            &[0x83, 0xFF, 0x01, 0x02, 0, 0, 0, 0],
            buffer.to_byte_slice()
        );

        unsafe { AttributesBuilder::fill_bit_range_unchecked(&mut buffer, 33, 35) };

        assert_eq!(
            &[0x83, 0xFF, 0x01, 0x02, 0x06, 0, 0, 0],
            buffer.to_byte_slice()
        );

        unsafe { AttributesBuilder::fill_bit_range_unchecked(&mut buffer, 41, 63) };

        assert_eq!(
            &[0x83, 0xFF, 0x01, 0x02, 0x06, 0xFE, 0xFF, 0x7F],
            buffer.to_byte_slice()
        );
    }
}
