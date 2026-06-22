// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{collections::hash_map::Entry, hash::Hash, marker::PhantomData, rc::Rc, sync::Arc};

use ahash::AHashMap;
use arrow::{
    array::*,
    buffer::{BooleanBuffer, Buffer, MutableBuffer},
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
    FTransform: Fn(DictionaryValueArray<'a>) -> (T, Option<AHashMap<usize, Option<usize>>>),
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
    FTransform:
        Fn(DictionaryValueArray<'a>) -> (PrimitiveArray<T>, Option<AHashMap<usize, Option<usize>>>),
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

enum ValueOrBuffer<'a> {
    Value(ValueOrRef<'a>),
    Buffer(Buffer)
}

#[derive(Hash, PartialEq, Eq)]
enum VecOrBuffer {
    Vec(Vec<u8>),
    Buffer(BufferWrapper<u8>)
}

pub(crate) fn attributes_writer<'a>(
    record_count: usize,
    values: AHashMap<Box<str>, OtapValue<'a>>,
    attributes_batch: Option<OtapAttributesBatch<'_>>,
) -> Option<(Arc<dyn Array>, RecordBatch)> {
    if values.is_empty() {
        return None;
    }

    let mut mapping = Vec::with_capacity(u16::MAX as usize);
    let mut key_values = StringBuilder::new();
    let mut types = Vec::with_capacity(u16::MAX as usize);
    let mut string_values: IndexSet<StringValueOrRef<'a>, ahash::RandomState> =
        IndexSet::with_capacity_and_hasher(u16::MAX as usize, ahash::RandomState::new());
    let mut int_values = None;
    let mut double_values = None;
    let mut sers_values = None;
    let mut bytes_values = None;

    let mut lookup: AHashMap<(u8, usize), (u8, Option<usize>)> = AHashMap::new();

    if let Some(attributes_batch) = attributes_batch {
        let attribute_keys = attributes_batch.get_keys();
        let attribute_key_values = attribute_keys.values();

        let mut keys_to_process = AHashMap::with_capacity(attribute_key_values.len());

        for (value_index, key) in attribute_key_values.iter().enumerate() {
            if let Some(key) = key
                && !values.contains_key(key)
            {
                let new_key_index = key_values.len();
                key_values.append_value(key);

                keys_to_process.insert(value_index, new_key_index);
            }
        }

        if !keys_to_process.is_empty() {
            let attributes_types = attributes_batch.get_attribute_types().values().as_ptr();
            let attribute_parent_ids = attributes_batch
                .get_attribute_parent_ids()
                .values()
                .as_ptr();
            let id_to_record_index_map = attributes_batch
                .get_id_to_record_index_map()
                .values()
                .as_ptr();

            for (key_index, key_value_index) in attribute_keys.key_iter().enumerate() {
                if let Some(new_key_index) = keys_to_process.get(&key_value_index) {
                    let attribute_type = unsafe { *attributes_types.add(key_index) };

                    if let Some(value) =
                        attributes_batch.get_attribute_value_or_index(key_index, attribute_type)
                    {
                        let parent_id = unsafe { *attribute_parent_ids.add(key_index) };
                        let record_index =
                            unsafe { *id_to_record_index_map.add(parent_id as usize) } as usize;

                        let (value, value_index) = match value {
                            AttributeValueOrIndex::ValueIndex(i) => {
                                if let Some((_, new_value_index)) =
                                    lookup.get(&(attribute_type, i as usize))
                                {
                                    types.push(attribute_type);
                                    mapping.push((
                                        record_index,
                                        *new_key_index,
                                        attribute_type,
                                        *new_value_index,
                                    ));
                                    continue;
                                }

                                (
                                    ValueOrBuffer::Buffer(attributes_batch.get_attribute_value_buffer(attribute_type, i)),
                                    Some(i),
                                )
                            }
                            AttributeValueOrIndex::Value(v) => (ValueOrBuffer::Value(v), None),
                        };

                        let new_value_index = match value {
                            ValueOrBuffer::Value(v) => match v {
                                ValueOrRef::Null => continue,
                                ValueOrRef::String(s) => {
                                    if s.is_empty() {
                                        None
                                    } else {
                                        let (value_index, _) = string_values.insert_full(s);
                                        Some(value_index)
                                    }
                                }
                                ValueOrRef::Integer(i) => {
                                    if i == 0 {
                                        None
                                    } else {
                                        let ints = int_values.get_or_insert_with(|| {
                                            IndexSet::with_capacity_and_hasher(
                                                u16::MAX as usize,
                                                ahash::RandomState::new(),
                                            )
                                        });
                                        let (value_index, _) = ints.insert_full(i);
                                        Some(value_index)
                                    }
                                }
                                ValueOrRef::Double(d) => {
                                    if d == 0f64 {
                                        None
                                    } else {
                                        let doubles = double_values.get_or_insert_with(|| {
                                            Vec::with_capacity(u16::MAX as usize)
                                        });
                                        let value_index = doubles.len();
                                        doubles.push(d);
                                        Some(value_index)
                                    }
                                }
                                ValueOrRef::Boolean(b) => Some(if b { 1 } else { 0 }),
                                v => panic!("Value type '{}' is not supported", v.get_value_type()),
                            }
                            ValueOrBuffer::Buffer(b) => {
                                if b.is_empty() {
                                    None
                                } else {
                                    match attribute_type {
                                        STRING_ATTRIBUTE_VALUE_TYPE => {
                                            let (value_index, _) = string_values.insert_full(StringValueOrRef::Buffer(b));
                                            Some(value_index)
                                        }
                                        MAP_ATTRIBUTE_VALUE_TYPE | SLICE_ATTRIBUTE_VALUE_TYPE => {
                                            let sers = sers_values.get_or_insert_with(|| {
                                                IndexSet::with_capacity_and_hasher(
                                                    u16::MAX as usize,
                                                    ahash::RandomState::new(),
                                                )
                                            });
                                            let (value_index, _) = sers.insert_full(VecOrBuffer::Buffer(BufferWrapper::<u8>::new(b)));
                                            Some(value_index)
                                        }
                                        BYTES_ATTRIBUTE_VALUE_TYPE => {
                                            let bytes = bytes_values.get_or_insert_with(|| {
                                                IndexSet::with_capacity_and_hasher(
                                                    u16::MAX as usize,
                                                    ahash::RandomState::new(),
                                                )
                                            });
                                            let (value_index, _) = bytes.insert_full(BufferWrapper::<u8>::new(b));
                                            Some(value_index)
                                        }
                                        t => panic!("Attribute type '{t}' is not supported"),
                                    }
                                }
                            }
                        };

                        types.push(attribute_type);
                        mapping.push((
                            record_index,
                            *new_key_index,
                            attribute_type,
                            new_value_index,
                        ));

                        if let Some(value_index) = value_index {
                            lookup.insert(
                                (attribute_type, value_index as usize),
                                (attribute_type, new_value_index),
                            );
                        }
                    }
                }
            }
        }
    }

    for (key, value) in values {
        match value {
            OtapValue::NotFound | OtapValue::Removed => continue,
            OtapValue::Read(value) | OtapValue::Set(value) => {
                let (keys, values) = value.into_parts();
                if keys.is_empty() || keys.is_null() {
                    continue;
                }

                let new_key_index = key_values.len();
                key_values.append_value(key);

                lookup.clear();

                for record_index in 0..keys.len() {
                    let value_index = match keys.get_value_index_for_key_index(record_index) {
                        None => continue,
                        Some(v) => v,
                    };

                    let (attribute_type, new_value_index) = match lookup
                        .entry((u8::MAX, value_index))
                    {
                        Entry::Occupied(occupied) => occupied.into_mut(),
                        Entry::Vacant(vacant) => match values.get_value_at(value_index) {
                            ValueOrRef::Null => continue,
                            ValueOrRef::String(s) => {
                                if s.is_empty() {
                                    vacant.insert((STRING_ATTRIBUTE_VALUE_TYPE, None))
                                } else {
                                    let (value_index, _) = string_values.insert_full(s);
                                    vacant.insert((STRING_ATTRIBUTE_VALUE_TYPE, Some(value_index)))
                                }
                            }
                            ValueOrRef::Integer(i) => {
                                if i == 0 {
                                    vacant.insert((INT_ATTRIBUTE_VALUE_TYPE, None))
                                } else {
                                    let ints = int_values.get_or_insert_with(|| {
                                        IndexSet::with_capacity_and_hasher(
                                            u16::MAX as usize,
                                            ahash::RandomState::new(),
                                        )
                                    });
                                    let (value_index, _) = ints.insert_full(i);
                                    vacant.insert((INT_ATTRIBUTE_VALUE_TYPE, Some(value_index)))
                                }
                            }
                            ValueOrRef::Double(d) => {
                                if d == 0f64 {
                                    vacant.insert((DOUBLE_ATTRIBUTE_VALUE_TYPE, None))
                                } else {
                                    let doubles = double_values.get_or_insert_with(|| {
                                        Vec::with_capacity(u16::MAX as usize)
                                    });
                                    let value_index = doubles.len();
                                    doubles.push(d);
                                    vacant.insert((DOUBLE_ATTRIBUTE_VALUE_TYPE, Some(value_index)))
                                }
                            }
                            ValueOrRef::Boolean(b) => vacant
                                .insert((BOOL_ATTRIBUTE_VALUE_TYPE, Some(if b { 1 } else { 0 }))),
                            ValueOrRef::Array(a) => {
                                match a {
                                    ArrayValueOrRef::Buffer(BufferArray::U8(b)) => {
                                        if b.is_empty() {
                                            vacant.insert((BYTES_ATTRIBUTE_VALUE_TYPE, None))
                                        } else {
                                            let bytes = bytes_values.get_or_insert_with(|| {
                                                IndexSet::with_capacity_and_hasher(
                                                    u16::MAX as usize,
                                                    ahash::RandomState::new(),
                                                )
                                            });
                                            let (value_index, _) = bytes.insert_full(b);
                                            vacant.insert((BYTES_ATTRIBUTE_VALUE_TYPE, Some(value_index)))
                                        }
                                    }
                                    a => {
                                        if a.is_empty() {
                                            vacant.insert((SLICE_ATTRIBUTE_VALUE_TYPE, None))
                                        } else {
                                            match crate::serialization::to_slice(ValueOrRef::Array(a)) {
                                                Ok(v) => {
                                                    let sers = sers_values.get_or_insert_with(|| {
                                                        IndexSet::with_capacity_and_hasher(
                                                            u16::MAX as usize,
                                                            ahash::RandomState::new(),
                                                        )
                                                    });
                                                    let (value_index, _) = sers.insert_full(VecOrBuffer::Vec(v));
                                                    vacant.insert((SLICE_ATTRIBUTE_VALUE_TYPE, Some(value_index)))
                                                }
                                                Err(_) => vacant.insert((SLICE_ATTRIBUTE_VALUE_TYPE, None)),
                                            }
                                        }
                                    }
                                }
                            }
                            ValueOrRef::Map(m) => {
                                if m.as_map_value().is_empty() {
                                    vacant.insert((MAP_ATTRIBUTE_VALUE_TYPE, None))
                                } else {
                                    match crate::serialization::to_slice(ValueOrRef::Map(m)) {
                                        Ok(v) => {
                                            let sers = sers_values.get_or_insert_with(|| {
                                                IndexSet::with_capacity_and_hasher(
                                                    u16::MAX as usize,
                                                    ahash::RandomState::new(),
                                                )
                                            });
                                            let (value_index, _) = sers.insert_full(VecOrBuffer::Vec(v));
                                            vacant.insert((MAP_ATTRIBUTE_VALUE_TYPE, Some(value_index)))
                                        }
                                        Err(_) => vacant.insert((MAP_ATTRIBUTE_VALUE_TYPE, None)),
                                    }
                                }
                            }
                            v => {
                                let s = v.to_value().convert_to_string();
                                if !s.as_ref().is_empty() {
                                    let (value_index, _) = string_values.insert_full(StringValueOrRef::Owned(Rc::new(s.into())));
                                    vacant.insert((STRING_ATTRIBUTE_VALUE_TYPE, Some(value_index)))
                                } else {
                                    vacant.insert((STRING_ATTRIBUTE_VALUE_TYPE, None))
                                }

                            }
                        },
                    };

                    types.push(*attribute_type);
                    mapping.push((
                        record_index,
                        new_key_index,
                        *attribute_type,
                        *new_value_index,
                    ));
                }
            }
        }
    }

    let attribute_count = mapping.len();

    let mut ids_buffer = MutableBuffer::from_len_zeroed(record_count * 2);
    let mut ids_null_buffer = MutableBuffer::new_null(record_count);
    let ids = ids_buffer.typed_data_mut::<u16>().as_mut_ptr();
    let ids_null = &mut ids_null_buffer;

    let mut parent_ids_buffer = MutableBuffer::from_len_zeroed(attribute_count * 2);
    let parent_ids = parent_ids_buffer.typed_data_mut::<u16>().as_mut_ptr();

    let mut keys_buffer = AdaptiveDictionaryWriter::new(attribute_count, key_values.len());
    let mut keys = keys_buffer.get_writer();

    let mut strings_buffer = MutableBuffer::from_len_zeroed(attribute_count * 2);
    let mut strings_null_buffer = MutableBuffer::new_null(attribute_count);
    let strings = strings_buffer.typed_data_mut::<u16>().as_mut_ptr();
    let strings_null = &mut strings_null_buffer;

    let mut ints = None;
    let mut doubles = None;
    let mut bools = None;
    let mut sers = None;
    let mut bytes = None;

    lookup.clear();
    let mut current_parent_id = 0;

    for (new_attribute_index, (record_index, new_key_index, attribute_type, new_value_index)) in
        mapping.into_iter().enumerate()
    {
        let (_, parent_id) = match lookup.entry((u8::MAX, record_index)) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                unsafe { *ids.add(record_index) = current_parent_id as u16 };
                bit_util::set_bit(ids_null, record_index);
                let r = vacant.insert((u8::MAX, Some(current_parent_id)));
                current_parent_id += 1;
                r
            }
        };

        unsafe { *parent_ids.add(new_attribute_index) = parent_id.expect("has parent id") as u16 };
        unsafe { keys.set_value_index_unchecked(new_attribute_index, new_key_index) };

        if let Some(value_index) = new_value_index {
            match attribute_type {
                STRING_ATTRIBUTE_VALUE_TYPE => {
                    unsafe { *strings.add(new_attribute_index) = value_index as u16 };
                    bit_util::set_bit(strings_null, new_attribute_index);
                }
                INT_ATTRIBUTE_VALUE_TYPE => {
                    let ints = ints.get_or_insert_with(|| {
                        AttributeArrayBuilder::<UInt16Type>::new(attribute_count)
                    });

                    unsafe {
                        ints.get_writer().set_value_index_typed_unchecked(
                            new_attribute_index,
                            value_index as u16,
                        )
                    };
                }
                DOUBLE_ATTRIBUTE_VALUE_TYPE => {
                    let doubles = doubles.get_or_insert_with(|| {
                        AttributeArrayBuilder::<Float64Type>::new(attribute_count)
                    });

                    unsafe {
                        doubles.get_writer().set_value_index_typed_unchecked(
                            new_attribute_index,
                            *double_values
                                .as_ref()
                                .expect("has doubles")
                                .get_unchecked(value_index),
                        )
                    };
                }
                BOOL_ATTRIBUTE_VALUE_TYPE => {
                    let bools = bools
                        .get_or_insert_with(|| AttributeBooleanArrayBuilder::new(attribute_count));

                    bools.set_bit(new_attribute_index, value_index > 0);
                }
                MAP_ATTRIBUTE_VALUE_TYPE | SLICE_ATTRIBUTE_VALUE_TYPE => {
                    let sers = sers.get_or_insert_with(|| {
                        AttributeArrayBuilder::<UInt16Type>::new(attribute_count)
                    });

                    unsafe {
                        sers.get_writer().set_value_index_typed_unchecked(
                            new_attribute_index,
                            value_index as u16,
                        )
                    };
                }
                BYTES_ATTRIBUTE_VALUE_TYPE => {
                    let bytes = bytes.get_or_insert_with(|| {
                        AttributeArrayBuilder::<UInt16Type>::new(attribute_count)
                    });

                    unsafe {
                        bytes.get_writer().set_value_index_typed_unchecked(
                            new_attribute_index,
                            value_index as u16,
                        )
                    };
                }
                t => panic!("Attribute type '{t}' is not supported"),
            }
        }
    }

    let ids = PrimitiveArray::<UInt16Type>::new(
        ids_buffer.into(),
        NullBufferBuilder::new_from_buffer(ids_null_buffer, record_count).build(),
    );

    let parent_ids = PrimitiveArray::<UInt16Type>::new(parent_ids_buffer.into(), None);

    let keys = keys_buffer.finish(Arc::new(key_values.finish()));

    let types = PrimitiveArray::<UInt8Type>::new(types.into(), None);

    let strings = DictionaryArray::new(
        PrimitiveArray::<UInt16Type>::new(
            strings_buffer.into(),
            NullBufferBuilder::new_from_buffer(strings_null_buffer, attribute_count).build(),
        ),
        Arc::new(StringArray::from(
            string_values
                .iter()
                .map(|v| v.as_ref())
                .collect::<Vec<&str>>(),
        )),
    );

    let mut columns: Vec<Arc<dyn Array>> = vec![];
    let mut fields = vec![];

    fields.push(
        Field::new(consts::PARENT_ID, parent_ids.data_type().clone(), false).with_plain_encoding(),
    );
    columns.push(Arc::new(parent_ids));

    fields.push(Field::new(
        consts::ATTRIBUTE_KEY,
        keys.data_type().clone(),
        false,
    ));
    columns.push(Arc::new(keys));

    fields.push(Field::new(
        consts::ATTRIBUTE_TYPE,
        types.data_type().clone(),
        false,
    ));
    columns.push(Arc::new(types));

    fields.push(Field::new(
        consts::ATTRIBUTE_STR,
        strings.data_type().clone(),
        true,
    ));
    columns.push(Arc::new(strings));

    if let (Some(int_keys), Some(int_values)) = (ints, int_values) {
        let ints = DictionaryArray::new(
            int_keys.finish(),
            Arc::new(PrimitiveArray::<Int64Type>::from(
                int_values.into_iter().collect::<Vec<i64>>(),
            )),
        );

        fields.push(Field::new(
            consts::ATTRIBUTE_INT,
            ints.data_type().clone(),
            true,
        ));
        columns.push(Arc::new(ints));
    }

    if let Some(doubles) = doubles {
        let doubles = doubles.finish();

        fields.push(Field::new(
            consts::ATTRIBUTE_DOUBLE,
            doubles.data_type().clone(),
            true,
        ));
        columns.push(Arc::new(doubles));
    }

    if let Some(bools) = bools {
        let bools = bools.finish();

        fields.push(Field::new(
            consts::ATTRIBUTE_BOOL,
            bools.data_type().clone(),
            true,
        ));
        columns.push(Arc::new(bools));
    }

    if let (Some(bytes_keys), Some(bytes_values)) = (bytes, bytes_values) {
        let bytes = DictionaryArray::new(
            bytes_keys.finish(),
            Arc::new(BinaryArray::from(
                bytes_values.iter().map(|v| v.as_ref()).collect::<Vec<&[u8]>>(),
            )),
        );

        fields.push(Field::new(
            consts::ATTRIBUTE_BYTES,
            bytes.data_type().clone(),
            true,
        ));
        columns.push(Arc::new(bytes));
    }

    if let (Some(sers_keys), Some(sers_values)) = (sers, sers_values) {
        let sers = DictionaryArray::new(
            sers_keys.finish(),
            Arc::new(BinaryArray::from(
                sers_values.iter().map(|v| match v {
                    VecOrBuffer::Vec(v) => v,
                    VecOrBuffer::Buffer(b) => b.as_ref(),
                }).collect::<Vec<&[u8]>>(),
            )),
        );

        fields.push(Field::new(
            consts::ATTRIBUTE_SER,
            sers.data_type().clone(),
            true,
        ));
        columns.push(Arc::new(sers));
    }

    Some((
        Arc::new(ids),
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid batch"),
    ))
}

enum AdaptiveDictionaryWriter {
    UInt8(MutableBuffer),
    UInt16(MutableBuffer),
}

impl AdaptiveDictionaryWriter {
    pub fn new(record_count: usize, value_count: usize) -> AdaptiveDictionaryWriter {
        if value_count <= u8::MAX as usize {
            AdaptiveDictionaryWriter::UInt8(MutableBuffer::from_len_zeroed(record_count))
        } else {
            AdaptiveDictionaryWriter::UInt16(MutableBuffer::from_len_zeroed(record_count * 2))
        }
    }

    pub fn get_writer(&'_ mut self) -> AdaptiveDictionaryKeyWriter<'_> {
        match self {
            AdaptiveDictionaryWriter::UInt8(b) => AdaptiveDictionaryKeyWriter::UInt8(
                b.typed_data_mut::<u8>().as_mut_ptr(),
                Default::default(),
            ),
            AdaptiveDictionaryWriter::UInt16(b) => AdaptiveDictionaryKeyWriter::UInt16(
                b.typed_data_mut::<u16>().as_mut_ptr(),
                Default::default(),
            ),
        }
    }

    pub fn finish(self, values: Arc<dyn Array>) -> Arc<dyn Array> {
        match self {
            AdaptiveDictionaryWriter::UInt8(b) => Arc::new(DictionaryArray::new(
                PrimitiveArray::<UInt8Type>::new(b.into(), None),
                values,
            )),
            AdaptiveDictionaryWriter::UInt16(b) => Arc::new(DictionaryArray::new(
                PrimitiveArray::<UInt16Type>::new(b.into(), None),
                values,
            )),
        }
    }
}

enum AdaptiveDictionaryKeyWriter<'a> {
    UInt8(*mut u8, PhantomData<&'a usize>),
    UInt16(*mut u16, PhantomData<&'a usize>),
}

impl<'a> AdaptiveDictionaryKeyWriter<'a> {
    pub unsafe fn set_value_index_unchecked(&mut self, key_index: usize, value_index: usize) {
        unsafe {
            match self {
                AdaptiveDictionaryKeyWriter::UInt8(b, _) => *b.add(key_index) = value_index as u8,
                AdaptiveDictionaryKeyWriter::UInt16(b, _) => *b.add(key_index) = value_index as u16,
            }
        }
    }
}

pub struct AttributeArrayBuilder<K: ArrowPrimitiveType> {
    key_length: usize,
    key_buffer: MutableBuffer,
    null_buffer: MutableBuffer,
    marker: PhantomData<K>,
}

impl<K: ArrowPrimitiveType> AttributeArrayBuilder<K> {
    pub fn new(key_length: usize) -> AttributeArrayBuilder<K> {
        Self {
            key_length,
            key_buffer: MutableBuffer::from_len_zeroed(size_of::<K::Native>() * key_length),
            null_buffer: MutableBuffer::new_null(key_length),
            marker: Default::default(),
        }
    }

    pub fn get_writer(&mut self) -> AttributeArrayWriter<'_, K> {
        AttributeArrayWriter {
            key_builder: self.key_buffer.typed_data_mut::<K::Native>().as_mut_ptr(),
            null_buffer: self.null_buffer.typed_data_mut::<u8>().as_mut_ptr(),
            marker: Default::default(),
        }
    }

    pub fn finish(self) -> PrimitiveArray<K> {
        PrimitiveArray::<K>::new(
            self.key_buffer.into(),
            NullBufferBuilder::new_from_buffer(self.null_buffer, self.key_length).build(),
        )
    }
}

pub struct AttributeArrayWriter<'a, K: ArrowPrimitiveType> {
    key_builder: *mut K::Native,
    null_buffer: *mut u8,
    marker: PhantomData<&'a usize>,
}

impl<'a, K: ArrowPrimitiveType> AttributeArrayWriter<'a, K> {
    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*.
    pub unsafe fn set_value_index_typed_unchecked(
        &mut self,
        key_index: usize,
        value_index: K::Native,
    ) {
        unsafe { *self.key_builder.add(key_index) = value_index }

        let i = key_index / 8;
        let b = 1 << (key_index % 8);

        unsafe { *self.null_buffer.add(i) |= b };
    }
}

pub struct AttributeBooleanArrayBuilder {
    key_length: usize,
    key_buffer: MutableBuffer,
    null_buffer: MutableBuffer,
}

impl AttributeBooleanArrayBuilder {
    pub fn new(key_length: usize) -> AttributeBooleanArrayBuilder {
        Self {
            key_length,
            key_buffer: MutableBuffer::from_len_zeroed(bit_util::ceil(key_length, 8)),
            null_buffer: MutableBuffer::new_null(key_length),
        }
    }

    pub fn set_bit(&mut self, key_index: usize, value: bool) {
        let i = key_index / 8;
        let b = 1 << (key_index % 8);

        if value {
            self.key_buffer[i] |= b
        }

        self.null_buffer[i] |= b;
    }

    pub fn finish(self) -> BooleanArray {
        BooleanArray::new(
            BooleanBuffer::new(self.key_buffer.into(), 0, self.key_length),
            NullBufferBuilder::new_from_buffer(self.null_buffer, self.key_length).build(),
        )
    }
}
