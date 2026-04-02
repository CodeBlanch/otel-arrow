// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::{OnceCell, RefCell},
    collections::hash_map::Entry,
    fmt::Display,
};

use ahash::AHashMap;
use arrow::{
    array::*,
    buffer::{MutableBuffer, NullBuffer},
    compute::kernels::filter,
    datatypes::*,
};
use data_engine_columnar::*;

use crate::filter::{IdBitmap, filter_child_batch};

pub struct OtapLogRecordBatchFactory {
    diagnostic_level: Option<ColumnarEngineDiagnosticLevel>,
}

impl OtapLogRecordBatchFactory {
    #[cfg(test)]
    pub fn new() -> OtapLogRecordBatchFactory {
        Self::new_with_options(None)
    }

    pub fn new_with_options(
        diagnostic_level: Option<ColumnarEngineDiagnosticLevel>,
    ) -> OtapLogRecordBatchFactory {
        Self { diagnostic_level }
    }
}

impl ColumnarRecordsFactory<4> for OtapLogRecordBatchFactory {
    type Records<'a> = OtapLogRecordBatch<'a>;

    fn create<'a>(&self, batches: &'a [Option<RecordBatch>]) -> OtapLogRecordBatch<'a> {
        if let Some(logs) = batches[2].as_ref() {
            let logs_schema = logs.schema_ref();

            let attributes = if let Some(id_column) = logs_schema.column_with_name("id")
                && let Some(attributes_batch) = batches[3].as_ref()
            {
                let ids = logs.column(id_column.0).as_primitive::<UInt16Type>();

                Some(OtapAttributes::new(ids, attributes_batch))
            } else {
                None
            };

            OtapLogRecordBatch::new(self.diagnostic_level.clone(), logs, logs_schema, attributes)
        } else {
            OtapLogRecordBatch::new_empty()
        }
    }

    fn filter(
        &self,
        batch: &OtapLogRecordBatch,
        filter: &BooleanArray,
    ) -> [Option<RecordBatch>; 4] {
        let filter_true_count = filter.true_count();

        if let Some(logs) = batch.logs
            && filter_true_count > 0
        {
            let number_of_logs_before_filter = logs.num_rows();
            if filter_true_count == number_of_logs_before_filter {
                return [
                    batch.resource.as_ref().map(|r| r.clone()),
                    batch.scope.as_ref().map(|s| s.clone()),
                    Some(logs.clone()),
                    batch.attributes.as_ref().map(|a| a.batch.clone()),
                ];
            }

            let filtered_logs = filter::filter_record_batch(logs, filter).unwrap();

            let number_of_logs_after_filter = filtered_logs.num_rows();
            if number_of_logs_after_filter > 0 {
                if number_of_logs_before_filter == number_of_logs_after_filter {
                    return [
                        batch.resource.as_ref().map(|r| r.clone()),
                        batch.scope.as_ref().map(|s| s.clone()),
                        Some(filtered_logs),
                        batch.attributes.as_ref().map(|a| a.batch.clone()),
                    ];
                }

                let mut ids = IdBitmap::new();

                if let Some(id_column) = filtered_logs.schema_ref().column_with_name("id") {
                    ids.populate(
                        filtered_logs
                            .column(id_column.0)
                            .as_primitive::<UInt16Type>()
                            .iter()
                            .flatten()
                            .map(|i| i.into()),
                    );
                }

                if ids.is_empty() {
                    return [None, None, Some(filtered_logs), None];
                }

                let resource = batch
                    .resource
                    .as_ref()
                    .and_then(|v| filter_child_batch(&ids, v));

                let scope = batch
                    .scope
                    .as_ref()
                    .and_then(|v| filter_child_batch(&ids, v));

                let attributes = batch
                    .attributes
                    .as_ref()
                    .and_then(|v| filter_child_batch(&ids, v.batch));

                return [resource, scope, Some(filtered_logs), attributes];
            }
        }

        [None, None, None, None]
    }
}

#[derive(Debug)]
pub struct OtapLogRecordBatch<'record> {
    diagnostic_level: Option<ColumnarEngineDiagnosticLevel>,
    logs: Option<&'record RecordBatch>,
    logs_schema: Option<&'record SchemaRef>,
    attributes: Option<OtapAttributes<'record>>,
    resource: Option<RecordBatch>,
    scope: Option<RecordBatch>,
}

impl<'record> OtapLogRecordBatch<'record> {
    pub fn new(
        diagnostic_level: Option<ColumnarEngineDiagnosticLevel>,
        logs: &'record RecordBatch,
        logs_schema: &'record SchemaRef,
        attributes: Option<OtapAttributes<'record>>,
    ) -> OtapLogRecordBatch<'record> {
        Self {
            diagnostic_level,
            logs: Some(logs),
            logs_schema: Some(logs_schema),
            attributes,
            resource: None,
            scope: None,
        }
    }

    pub fn new_empty() -> OtapLogRecordBatch<'record> {
        Self {
            diagnostic_level: None,
            logs: None,
            logs_schema: None,
            attributes: None,
            resource: None,
            scope: None,
        }
    }
}

impl ColumnarRecords for OtapLogRecordBatch<'_> {
    fn get_diagnostic_level(&self) -> Option<ColumnarEngineDiagnosticLevel> {
        self.diagnostic_level.clone()
    }

    fn len(&self) -> usize {
        self.logs.map_or(0, |v| v.num_rows())
    }
}

impl<'record> RecordTable for OtapLogRecordBatch<'record> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>> {
        if key == "Attributes" || key == "attributes" {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        if let Some(logs) = self.logs
            && let Some(logs_schema) = self.logs_schema
        {
            if key == "SeverityText" || key == "severity_text" {
                if let Some(severity_text_column) = logs_schema.column_with_name("severity_text") {
                    let severity_text_array = logs
                        .column(severity_text_column.0)
                        .as_dictionary::<UInt8Type>()
                        .downcast_dict::<StringArray>()
                        .expect("severity_text values were an unexpected type");

                    return Some(RecordTableValue::Dictionary(severity_text_array.into()));
                } else {
                    return None;
                }
            }

            if key == "SeverityNumber" || key == "severity_number" {
                if let Some(severity_number_column) =
                    logs_schema.column_with_name("severity_number")
                {
                    let severity_number_array = logs
                        .column(severity_number_column.0)
                        .as_dictionary::<UInt8Type>()
                        .downcast_dict::<Int64Array>()
                        .expect("severity_number values were an unexpected type");

                    return Some(RecordTableValue::Dictionary(severity_number_array.into()));
                } else {
                    return None;
                }
            }

            if key == "Body" || key == "body" {
                // todo: Look at body type
                todo!()
            }
        }

        None
    }
}

impl Display for OtapLogRecordBatch<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Logs(RecordCount={})", self.len())
    }
}

#[derive(Debug)]
pub struct OtapAttributes<'record> {
    batch: &'record RecordBatch,
    ids: &'record PrimitiveArray<UInt16Type>,
    id_to_record_index_map: OnceCell<PrimitiveArray<UInt16Type>>,
    cache: RefCell<AHashMap<Box<str>, Dictionary<'record>>>,
    attribute_parent_ids: &'record PrimitiveArray<UInt16Type>,
    attribute_keys:
        TypedDictionaryArray<'record, UInt8Type, GenericByteArray<GenericStringType<i32>>>,
    attribute_types: &'record PrimitiveArray<UInt8Type>,
    attribute_string_keys: &'record PrimitiveArray<UInt16Type>,
    attribute_string_values: &'record GenericByteArray<GenericStringType<i32>>,
    attribute_int_keys: Option<&'record PrimitiveArray<UInt16Type>>,
    attribute_int_values: Option<&'record PrimitiveArray<Int64Type>>,
    attribute_doubles:
        Option<TypedDictionaryArray<'record, UInt16Type, PrimitiveArray<Float64Type>>>,
    attribute_bools: Option<TypedDictionaryArray<'record, UInt16Type, BooleanArray>>,
}

impl<'record> OtapAttributes<'record> {
    pub fn new(
        ids: &'record PrimitiveArray<UInt16Type>,
        attributes_batch: &'record RecordBatch,
    ) -> OtapAttributes<'record> {
        let strings = attributes_batch
            .column(3)
            .as_dictionary::<UInt16Type>()
            .downcast_dict::<StringArray>()
            .expect("Attribute strings were an unexpected type");

        let ints = attributes_batch.column_by_name("int").map(|c| {
            c.as_dictionary::<UInt16Type>()
                .downcast_dict::<PrimitiveArray<Int64Type>>()
                .expect("Attribute ints were an unexpected type")
        });

        Self {
            batch: attributes_batch,
            ids,
            id_to_record_index_map: OnceCell::new(),
            cache: RefCell::new(AHashMap::new()),
            attribute_parent_ids: attributes_batch.column(0).as_primitive::<UInt16Type>(),
            attribute_keys: attributes_batch
                .column(1)
                .as_dictionary::<UInt8Type>()
                .downcast_dict::<StringArray>()
                .expect("Attribute keys were an unexpected type"),
            attribute_types: attributes_batch.column(2).as_primitive::<UInt8Type>(),
            attribute_string_keys: strings.keys(),
            attribute_string_values: strings.values(),
            attribute_int_keys: ints.map(|v| v.keys()),
            attribute_int_values: ints.map(|v| v.values()),
            attribute_doubles: attributes_batch.column_by_name("double").map(|c| {
                c.as_dictionary::<UInt16Type>()
                    .downcast_dict::<PrimitiveArray<Float64Type>>()
                    .expect("Attribute doubles were an unexpected type")
            }),
            attribute_bools: attributes_batch.column_by_name("bool").map(|c| {
                c.as_dictionary::<UInt16Type>()
                    .downcast_dict::<BooleanArray>()
                    .expect("Attribute bools were an unexpected type")
            }),
        }
    }

    fn get_id_to_record_index_map(&self) -> &PrimitiveArray<UInt16Type> {
        // Note: id_map is an array of parent_ids (record identifier in the
        // attribute table) to the actual index of the record in the root table.
        self.id_to_record_index_map.get_or_init(|| {
            let ids = self.ids;
            let mut id_map_length = ids.len();
            let mut id_map_buffer = MutableBuffer::from_len_zeroed(id_map_length * 2);
            let mut id_map = id_map_buffer.typed_data_mut::<u16>().as_mut_ptr();
            for (record_index, id) in ids.iter().flatten().enumerate() {
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
            PrimitiveArray::<UInt16Type>::new(id_map_buffer.into(), None)
        })
    }
    /*
    fn get_attribute_value(
        &self,
        attribute_index: usize,
    ) -> Option<(u8, u16, ValueOrRef<'record>)> {
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
        match self.attribute_types.value(attribute_index) {
            0 => {}
            1 => {
                let strings = &self.attribute_strings;
                if strings.is_valid(attribute_index) {
                    let value_index = unsafe { strings.keys().value_unchecked(attribute_index) };
                    return Some((
                        1,
                        value_index,
                        ValueOrRef::StringRef(unsafe {
                            strings.values().value_unchecked(value_index as usize)
                        }),
                    ));
                }
            }
            2 => {
                if let Some(ints) = self.attribute_ints
                    && ints.is_valid(attribute_index)
                {
                    let value_index = unsafe { ints.keys().value_unchecked(attribute_index) };
                    return Some((
                        2,
                        value_index,
                        ValueOrRef::IntegerOwned(unsafe {
                            ints.values().value_unchecked(value_index as usize)
                        }),
                    ));
                }
            }
            3 => {
                if let Some(doubles) = self.attribute_doubles
                    && doubles.is_valid(attribute_index)
                {
                    let value_index = unsafe { doubles.keys().value_unchecked(attribute_index) };
                    return Some((
                        3,
                        value_index,
                        ValueOrRef::DoubleOwned(unsafe {
                            doubles.values().value_unchecked(value_index as usize)
                        }),
                    ));
                }
            }
            4 => {
                if let Some(bools) = self.attribute_bools
                    && bools.is_valid(attribute_index)
                {
                    let value_index = unsafe { bools.keys().value_unchecked(attribute_index) };
                    return Some((
                        4,
                        value_index,
                        ValueOrRef::BooleanOwned(unsafe {
                            bools.values().value_unchecked(value_index as usize)
                        }),
                    ));
                }
            }
            _ => todo!(),
        }

        None
    }
    */

    /*
    fn get_attribute_type_and_value_index(
        &self,
        attribute_index: usize,
    ) -> Option<(u8, u16)> {
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
        match unsafe { self.attribute_types.value_unchecked(attribute_index) } {
            0 => {}
            1 => {
                let strings = &self.attribute_strings;
                //if strings.is_valid(attribute_index) {
                    let value_index = unsafe { strings.keys().value_unchecked(attribute_index) };
                    return Some((
                        1,
                        value_index
                    ));
                //}
            }
            2 => {
                if let Some(ints) = self.attribute_ints
                    && ints.is_valid(attribute_index)
                {
                    let value_index = unsafe { ints.keys().value_unchecked(attribute_index) };
                    return Some((
                        2,
                        value_index,
                    ));
                }
            }
            3 => {
                if let Some(doubles) = self.attribute_doubles
                    && doubles.is_valid(attribute_index)
                {
                    let value_index = unsafe { doubles.keys().value_unchecked(attribute_index) };
                    return Some((
                        3,
                        value_index,
                    ));
                }
            }
            4 => {
                if let Some(bools) = self.attribute_bools
                    && bools.is_valid(attribute_index)
                {
                    let value_index = unsafe { bools.keys().value_unchecked(attribute_index) };
                    return Some((
                        4,
                        value_index,
                    ));
                }
            }
            _ => todo!(),
        }

        None
    }
    */

    fn get_attribute_value_index(&self, attribute_index: usize, attribute_type: u8) -> Option<u16> {
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
                    return Some(value_index);
                }
            }
            2 => {
                if let Some(keys) = self.attribute_int_keys
                    && keys.is_valid(attribute_index)
                {
                    let value_index = unsafe { keys.value_unchecked(attribute_index) };
                    return Some(value_index);
                }
            }
            3 => {
                if let Some(doubles) = self.attribute_doubles
                    && doubles.is_valid(attribute_index)
                {
                    let value_index = unsafe { doubles.keys().value_unchecked(attribute_index) };
                    return Some(value_index);
                }
            }
            4 => {
                if let Some(bools) = self.attribute_bools
                    && bools.is_valid(attribute_index)
                {
                    let value_index = unsafe { bools.keys().value_unchecked(attribute_index) };
                    return Some(value_index);
                }
            }
            _ => todo!(),
        }

        None
    }

    fn get_attribute_value(
        &self,
        attribute_type: u8,
        attribute_value_index: u16,
    ) -> ValueOrRef<'record> {
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
            1 => ValueOrRef::StringRef(unsafe {
                self.attribute_string_values
                    .value_unchecked(attribute_value_index as usize)
            }),
            2 => ValueOrRef::IntegerOwned(unsafe {
                self.attribute_int_values
                    .unwrap()
                    .value_unchecked(attribute_value_index as usize)
            }),
            3 => ValueOrRef::DoubleOwned(unsafe {
                self.attribute_doubles
                    .unwrap()
                    .values()
                    .value_unchecked(attribute_value_index as usize)
            }),
            4 => ValueOrRef::BooleanOwned(unsafe {
                self.attribute_bools
                    .unwrap()
                    .values()
                    .value_unchecked(attribute_value_index as usize)
            }),
            _ => unreachable!(),
        }
    }
}

impl<'record> RecordTable for OtapAttributes<'record> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>> {
        let mut cache = self.cache.borrow_mut();

        if let Some(d) = cache.get(key) {
            return Some(RecordTableValue::Dictionary(d.clone()));
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
            let attribute_parent_ids = self.attribute_parent_ids.values().as_ptr();
            let id_to_record_index_map = self.get_id_to_record_index_map().values().as_ptr();

            for attribute_index in 0..attribute_count {
                if unsafe { *attribute_keys.add(attribute_index) } == value_index {
                    let attribute_type = unsafe { *attribute_types.add(attribute_index) };
                    if let Some(attribute_value_index) =
                        self.get_attribute_value_index(attribute_index, attribute_type)
                    {
                        let lookup_key =
                            ((attribute_type as usize) << 16) | attribute_value_index as usize;
                        let index = match value_lookup.entry(lookup_key) {
                            Entry::Occupied(occupied) => occupied.into_mut(),
                            Entry::Vacant(vacant) => {
                                let index = values.len();
                                values.push(
                                    self.get_attribute_value(attribute_type, attribute_value_index),
                                );
                                vacant.insert(index as u16)
                            }
                        };

                        let parent_id = unsafe { *attribute_parent_ids.add(attribute_index) };
                        let record_index =
                            unsafe { *id_to_record_index_map.add(parent_id as usize) };

                        unsafe { *keys.add(record_index as usize) = *index };
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

            Dictionary::new(keys, DictionaryValueArray::VecAnyOwned(values))
        } else {
            let key_buffer = MutableBuffer::from_len_zeroed(record_count * 2);

            Dictionary::new(
                PrimitiveArray::<UInt16Type>::new(
                    key_buffer.into(),
                    Some(NullBuffer::new_null(record_count)),
                )
                .into(),
                DictionaryValueArray::VecAnyOwned(vec![]),
            )
        };

        let copy = value.clone();

        cache.insert(key.into(), value);

        Some(RecordTableValue::Dictionary(copy))
    }
}

impl Display for OtapAttributes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Attributes(RecordCount={})",
            self.attribute_parent_ids.len()
        )
    }
}
