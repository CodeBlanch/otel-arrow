// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{collections::hash_map::Entry, hash::Hash, sync::Arc};

use ahash::AHashMap;
use arrow::{array::*, buffer::MutableBuffer, datatypes::*, util::bit_util};
use data_engine_columnar::*;
use data_engine_expressions::*;
use indexmap::IndexSet;
use otap_df_pdata::schema::{FieldExt, consts};

use crate::{OtapDecodedIds, OtapValue};

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

pub(crate) fn attributes_writer(
    record_count: usize,
    decoded_ids: Option<OtapDecodedIds>,
    id_to_record_index_map: Option<PrimitiveArray<UInt16Type>>,
    values: AHashMap<Box<str>, OtapValue<'_>>,
) -> Option<(Arc<dyn Array>, RecordBatch)> {
    if values.is_empty() {
        return None;
    }

    let mut mapping = Vec::with_capacity(record_count);
    let mut key_values =
        IndexSet::with_capacity_and_hasher(record_count, ahash::RandomState::new());
    let mut types = Vec::with_capacity(record_count);
    let mut string_values =
        IndexSet::with_capacity_and_hasher(record_count, ahash::RandomState::new());

    let mut lookup = AHashMap::new();

    for (key, value) in values {
        match value {
            OtapValue::NotFound | OtapValue::Removed => continue,
            OtapValue::Read(v) | OtapValue::Set(v) => {
                let (keys, values) = v.into_parts();
                if keys.is_empty() || keys.is_null() {
                    continue;
                }

                let (key_index, _) = key_values.insert_full(key);

                lookup.clear();

                for record_key_index in 0..keys.len() {
                    let value_index = match keys.get_value_index_for_key_index(record_key_index) {
                        None => continue,
                        Some(v) => v,
                    };

                    let value_index = match lookup.entry(value_index) {
                        Entry::Occupied(occupied) => occupied.into_mut(),
                        Entry::Vacant(vacant) => match values.get_value_at(value_index) {
                            ValueOrRef::Null => continue,
                            ValueOrRef::String(s) => {
                                let (value_index, _) = string_values.insert_full(s);

                                vacant.insert(value_index)
                            }
                            v => todo!(),
                        },
                    };

                    types.push(1u8);
                    mapping.push((record_key_index, key_index, 1u8, *value_index));
                }

                //println!("appended key: {key}");
            }
        }
    }

    let attribute_count = mapping.len();

    lookup.clear();

    let mut ids_buffer = MutableBuffer::from_len_zeroed(record_count * 2);
    let mut ids_null_buffer = MutableBuffer::new_null(record_count);
    let ids = ids_buffer.typed_data_mut::<u16>();
    let ids_null = ids_null_buffer.typed_data_mut::<u8>();

    let mut parent_ids_buffer = MutableBuffer::from_len_zeroed(attribute_count * 2);
    let parent_ids = parent_ids_buffer.typed_data_mut::<u16>();

    let mut keys_buffer = MutableBuffer::from_len_zeroed(attribute_count * 2);
    let keys = keys_buffer.typed_data_mut::<u16>();

    let mut strings_buffer = MutableBuffer::from_len_zeroed(attribute_count * 2);
    let mut strings_null_buffer = MutableBuffer::new_null(attribute_count);
    let strings = strings_buffer.typed_data_mut::<u16>();
    let strings_null = strings_null_buffer.typed_data_mut::<u8>();

    let mut current_parent_id = 0;

    for (attribute_index, (record_key_index, key_index, value_type, value_index)) in
        mapping.into_iter().enumerate()
    {
        let parent_id = match lookup.entry(record_key_index) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                ids[record_key_index] = current_parent_id as u16;
                bit_util::set_bit(ids_null, record_key_index);
                let r = vacant.insert(current_parent_id);
                current_parent_id += 1;
                r
            }
        };

        parent_ids[attribute_index] = *parent_id as u16;
        keys[attribute_index] = key_index as u16;

        match value_type {
            0 => continue,
            1 => {
                strings[attribute_index] = value_index as u16;
                bit_util::set_bit(strings_null, attribute_index);
            }
            _ => todo!(),
        }
    }

    let ids = PrimitiveArray::<UInt16Type>::new(
        ids_buffer.into(),
        NullBufferBuilder::new_from_buffer(ids_null_buffer, record_count).finish(),
    );

    let parent_ids = PrimitiveArray::<UInt16Type>::new(parent_ids_buffer.into(), None);

    let keys = DictionaryArray::new(
        PrimitiveArray::<UInt16Type>::new(keys_buffer.into(), None),
        Arc::new(StringArray::from(
            key_values.iter().map(|v| v.as_ref()).collect::<Vec<&str>>(),
        )),
    );

    let types = PrimitiveArray::<UInt8Type>::new(types.into(), None);

    let strings = DictionaryArray::new(
        PrimitiveArray::<UInt16Type>::new(
            strings_buffer.into(),
            NullBufferBuilder::new_from_buffer(strings_null_buffer, attribute_count).finish(),
        ),
        Arc::new(StringArray::from(
            string_values
                .iter()
                .map(|v| v.as_ref())
                .collect::<Vec<&str>>(),
        )),
    );

    /*println!("ids: {ids:?}");
    println!("parent_ids: {parent_ids:?}");
    println!("keys: {keys:?}");
    println!("types: {types:?}");
    println!("strings: {strings:?}");*/

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

    Some((
        Arc::new(ids),
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid batch"),
    ))
}
