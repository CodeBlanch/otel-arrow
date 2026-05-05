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

use crate::{
    filter::{IdBitmap, filter_child_batch},
    *,
};

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

            let resource = if let Some(resource_column) = logs_schema.column_with_name("resource")
                && let Some(resource_struct) = logs.column(resource_column.0).as_struct_opt()
            {
                if let Some(resource_ids) = resource_struct.column_by_name("id")
                    && let Some(resource_attributes_batch) = batches[0].as_ref()
                {
                    let ids = resource_ids.as_primitive::<UInt16Type>();

                    Some(OtapResource {
                        resource_struct,
                        attributes: Some(OtapAttributes::new(ids, resource_attributes_batch)),
                    })
                } else {
                    Some(OtapResource {
                        resource_struct,
                        attributes: None,
                    })
                }
            } else {
                None
            };

            let scope = if let Some(scope_column) = logs_schema.column_with_name("scope")
                && let Some(scope_struct) = logs.column(scope_column.0).as_struct_opt()
            {
                if let Some(scope_ids) = scope_struct.column_by_name("id")
                    && let Some(scope_attributes_batch) = batches[1].as_ref()
                {
                    let ids = scope_ids.as_primitive::<UInt16Type>();

                    Some(OtapScope {
                        scope_struct,
                        attributes: Some(OtapAttributes::new(ids, scope_attributes_batch)),
                    })
                } else {
                    Some(OtapScope {
                        scope_struct,
                        attributes: None,
                    })
                }
            } else {
                None
            };

            OtapLogRecordBatch::new(
                self.diagnostic_level,
                logs,
                logs_schema,
                attributes,
                resource,
                scope,
            )
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
                    batch
                        .resource
                        .as_ref()
                        .and_then(|r| r.attributes.as_ref())
                        .map(|v| v.batch.clone()),
                    batch
                        .scope
                        .as_ref()
                        .and_then(|s| s.attributes.as_ref())
                        .map(|v| v.batch.clone()),
                    Some(logs.clone()),
                    batch.attributes.as_ref().map(|a| a.batch.clone()),
                ];
            }

            let filtered_logs_batch = filter::filter_record_batch(logs, filter).unwrap();

            let number_of_logs_after_filter = filtered_logs_batch.num_rows();
            if number_of_logs_after_filter > 0 {
                if number_of_logs_before_filter == number_of_logs_after_filter {
                    return [
                        batch
                            .resource
                            .as_ref()
                            .and_then(|r| r.attributes.as_ref())
                            .map(|v| v.batch.clone()),
                        batch
                            .scope
                            .as_ref()
                            .and_then(|s| s.attributes.as_ref())
                            .map(|v| v.batch.clone()),
                        Some(filtered_logs_batch),
                        batch.attributes.as_ref().map(|a| a.batch.clone()),
                    ];
                }

                let mut ids = IdBitmap::new();

                if let Some(id_column) = filtered_logs_batch.schema_ref().column_with_name("id") {
                    ids.populate(
                        filtered_logs_batch
                            .column(id_column.0)
                            .as_primitive::<UInt16Type>()
                            .iter()
                            .flatten()
                            .map(|i| i.into()),
                    );
                }

                let attributes_batch = if ids.is_empty() {
                    None
                } else {
                    batch
                        .attributes
                        .as_ref()
                        .and_then(|v| filter_child_batch(&ids, v.batch))
                };

                let resource_attributes_batch = if let Some(resource) = batch.resource.as_ref()
                    && let Some(resource_attributes) = resource.attributes.as_ref()
                {
                    ids.clear();

                    if let Some(resource_column) = filtered_logs_batch
                        .schema_ref()
                        .column_with_name("resource")
                        && let Some(resource_struct) = filtered_logs_batch
                            .column(resource_column.0)
                            .as_struct_opt()
                        && let Some(resource_ids) = resource_struct.column_by_name("id")
                    {
                        ids.populate(
                            resource_ids
                                .as_primitive::<UInt16Type>()
                                .iter()
                                .flatten()
                                .map(|i| i.into()),
                        );
                    }

                    if ids.is_empty() {
                        None
                    } else {
                        filter_child_batch(&ids, resource_attributes.batch)
                    }
                } else {
                    None
                };

                let scope_attributes_batch = if let Some(scope) = batch.scope.as_ref()
                    && let Some(scope_attributes) = scope.attributes.as_ref()
                {
                    ids.clear();

                    if let Some(scope_column) =
                        filtered_logs_batch.schema_ref().column_with_name("scope")
                        && let Some(scope_struct) =
                            filtered_logs_batch.column(scope_column.0).as_struct_opt()
                        && let Some(scope_ids) = scope_struct.column_by_name("id")
                    {
                        ids.populate(
                            scope_ids
                                .as_primitive::<UInt16Type>()
                                .iter()
                                .flatten()
                                .map(|i| i.into()),
                        );
                    }

                    if ids.is_empty() {
                        None
                    } else {
                        filter_child_batch(&ids, scope_attributes.batch)
                    }
                } else {
                    None
                };

                return [
                    resource_attributes_batch,
                    scope_attributes_batch,
                    Some(filtered_logs_batch),
                    attributes_batch,
                ];
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
    resource: Option<OtapResource<'record>>,
    scope: Option<OtapScope<'record>>,
    body: OnceCell<Option<RecordTableDictionary>>,
}

impl<'record> OtapLogRecordBatch<'record> {
    pub fn new(
        diagnostic_level: Option<ColumnarEngineDiagnosticLevel>,
        logs: &'record RecordBatch,
        logs_schema: &'record SchemaRef,
        attributes: Option<OtapAttributes<'record>>,
        resource: Option<OtapResource<'record>>,
        scope: Option<OtapScope<'record>>,
    ) -> OtapLogRecordBatch<'record> {
        Self {
            diagnostic_level,
            logs: Some(logs),
            logs_schema: Some(logs_schema),
            attributes,
            resource,
            scope,
            body: OnceCell::new(),
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
            body: OnceCell::new(),
        }
    }
}

impl ColumnarRecords for OtapLogRecordBatch<'_> {
    fn get_diagnostic_level(&self) -> Option<ColumnarEngineDiagnosticLevel> {
        self.diagnostic_level
    }

    fn get_key_data_type(&self) -> DataType {
        DataType::UInt16
    }

    fn len(&self) -> usize {
        self.logs.map_or(0, |v| v.num_rows())
    }

    fn get_attached_records(&self, name: &str) -> Option<&dyn RecordTable> {
        match name {
            "resource" | "Resource" => self.resource.as_ref().map(|v| v as &dyn RecordTable),
            "scope"
            | "Scope"
            | "instrumentation_scope"
            | "InstrumentationScope"
            | "instrumentationScope" => self.scope.as_ref().map(|v| v as &dyn RecordTable),
            _ => None,
        }
    }
}

impl RecordTable for OtapLogRecordBatch<'_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>> {
        let key = get_log_record_schema().normalize_key(key);

        if key == "attributes" {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        if let Some(logs) = self.logs
            && let Some(logs_schema) = self.logs_schema
        {
            let values = match key {
                "time_unix_nano"
                    if let Some(time_unix_nano_column) =
                        logs_schema.column_with_name("time_unix_nano") =>
                {
                    RecordTableDictionary::from_array::<UInt16Type, _>(
                        logs.column(time_unix_nano_column.0)
                            .as_primitive::<TimestampNanosecondType>(),
                    )
                }
                "observed_time_unix_nano"
                    if let Some(observed_time_unix_nano_column) =
                        logs_schema.column_with_name("observed_time_unix_nano") =>
                {
                    RecordTableDictionary::from_array::<UInt16Type, _>(
                        logs.column(observed_time_unix_nano_column.0)
                            .as_primitive::<TimestampNanosecondType>(),
                    )
                }
                "severity_number"
                    if let Some(severity_number_column) =
                        logs_schema.column_with_name("severity_number") =>
                {
                    logs.column(severity_number_column.0)
                        .as_dictionary::<UInt8Type>()
                        .downcast_dict::<Int64Array>()
                        .expect("severity_number values were an unexpected type")
                        .into()
                }
                "severity_text"
                    if let Some(severity_text_column) =
                        logs_schema.column_with_name("severity_text") =>
                {
                    logs.column(severity_text_column.0)
                        .as_dictionary::<UInt8Type>()
                        .downcast_dict::<StringArray>()
                        .expect("severity_text values were an unexpected type")
                        .into()
                }
                "body"
                    if let Some(body) = self
                        .body
                        .get_or_init(|| build_logs_body_dictionary(logs, logs_schema)) =>
                {
                    body.clone()
                }
                "trace_id"
                    if let Some(trace_id_column) = logs_schema.column_with_name("trace_id") =>
                {
                    logs.column(trace_id_column.0)
                        .as_dictionary::<UInt8Type>()
                        .downcast_dict::<FixedSizeBinaryArray>()
                        .expect("trace_id values were an unexpected type")
                        .into()
                }
                "span_id" if let Some(span_id_column) = logs_schema.column_with_name("span_id") => {
                    logs.column(span_id_column.0)
                        .as_dictionary::<UInt8Type>()
                        .downcast_dict::<FixedSizeBinaryArray>()
                        .expect("span_id values were an unexpected type")
                        .into()
                }
                "flags" if let Some(flags_column) = logs_schema.column_with_name("flags") => {
                    RecordTableDictionary::from_array::<UInt16Type, _>(
                        logs.column(flags_column.0).as_primitive::<UInt32Type>(),
                    )
                }
                "event_name"
                    if let Some(event_name_column) = logs_schema.column_with_name("event_name") =>
                {
                    logs.column(event_name_column.0)
                        .as_dictionary::<UInt8Type>()
                        .downcast_dict::<StringArray>()
                        .expect("event_name values were an unexpected type")
                        .into()
                }
                _ => return None,
            };

            return Some(RecordTableValue::Dictionary(values));
        }

        None
    }

    fn get_child_table_mut(&mut self, _key: &str) -> Option<&mut dyn RecordTable> {
        todo!()
    }

    fn set_values(&mut self, _key: &str, _values: Dictionary<'_>) {
        todo!()
    }
}

impl Display for OtapLogRecordBatch<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Logs(RecordCount={})", self.len())
    }
}

#[derive(Debug)]
pub struct OtapResource<'record> {
    resource_struct: &'record StructArray,
    attributes: Option<OtapAttributes<'record>>,
}

impl RecordTable for OtapResource<'_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>> {
        if key == "attributes" || key == "Attributes" {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        None
    }

    fn get_child_table_mut(&mut self, _key: &str) -> Option<&mut dyn RecordTable> {
        todo!()
    }

    fn set_values(&mut self, _key: &str, _values: Dictionary<'_>) {
        todo!()
    }
}

impl Display for OtapResource<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Resource(RecordCount={})", self.resource_struct.len())
    }
}

#[derive(Debug)]
pub struct OtapScope<'record> {
    scope_struct: &'record StructArray,
    attributes: Option<OtapAttributes<'record>>,
}

impl RecordTable for OtapScope<'_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>> {
        if key == "attributes" || key == "Attributes" {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        let values = match key {
            "name" | "Name" if let Some(name_column) = self.scope_struct.column_by_name("name") => {
                name_column
                    .as_dictionary::<UInt8Type>()
                    .downcast_dict::<StringArray>()
                    .expect("scope name values were an unexpected type")
                    .into()
            }
            "version" | "Version"
                if let Some(version_column) = self.scope_struct.column_by_name("version") =>
            {
                version_column
                    .as_dictionary::<UInt8Type>()
                    .downcast_dict::<StringArray>()
                    .expect("scope version values were an unexpected type")
                    .into()
            }
            _ => return None,
        };

        Some(RecordTableValue::Dictionary(values))
    }

    fn get_child_table_mut(&mut self, _key: &str) -> Option<&mut dyn RecordTable> {
        todo!()
    }

    fn set_values(&mut self, _key: &str, _values: Dictionary<'_>) {
        todo!()
    }
}

impl Display for OtapScope<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scope(RecordCount={})", self.scope_struct.len())
    }
}

#[derive(Debug)]
pub struct OtapAttributes<'record> {
    batch: &'record RecordBatch,
    ids: &'record PrimitiveArray<UInt16Type>,
    id_to_record_index_map: OnceCell<PrimitiveArray<UInt16Type>>,
    cache: RefCell<AHashMap<Box<str>, RecordTableDictionary>>,
    attribute_parent_ids: &'record PrimitiveArray<UInt16Type>,
    attribute_keys:
        TypedDictionaryArray<'record, UInt8Type, GenericByteArray<GenericStringType<i32>>>,
    attribute_types: &'record PrimitiveArray<UInt8Type>,
    attribute_string_keys: &'record PrimitiveArray<UInt16Type>,
    attribute_string_values: &'record GenericByteArray<GenericStringType<i32>>,
    attribute_int_keys: Option<&'record PrimitiveArray<UInt16Type>>,
    attribute_int_values: Option<&'record PrimitiveArray<Int64Type>>,
    attribute_doubles: Option<&'record PrimitiveArray<Float64Type>>,
    attribute_bools: Option<&'record BooleanArray>,
    attribute_bytes_keys: Option<&'record PrimitiveArray<UInt16Type>>,
    attribute_bytes_values: Option<&'record GenericBinaryArray<i32>>,
    attribute_ser_keys: Option<&'record PrimitiveArray<UInt16Type>>,
    attribute_ser_values: Option<&'record GenericBinaryArray<i32>>,
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

        let bytes = attributes_batch.column_by_name("bytes").map(|c| {
            c.as_dictionary::<UInt16Type>()
                .downcast_dict::<BinaryArray>()
                .expect("Attribute bytes were an unexpected type")
        });

        let ser = attributes_batch.column_by_name("ser").map(|c| {
            c.as_dictionary::<UInt16Type>()
                .downcast_dict::<BinaryArray>()
                .expect("Attribute ser was an unexpected type")
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
            attribute_doubles: attributes_batch
                .column_by_name("double")
                .map(|c| c.as_primitive::<Float64Type>()),
            attribute_bools: attributes_batch
                .column_by_name("bool")
                .map(|c| c.as_boolean()),
            attribute_bytes_keys: bytes.map(|v| v.keys()),
            attribute_bytes_values: bytes.map(|v| v.values()),
            attribute_ser_keys: ser.map(|v| v.keys()),
            attribute_ser_values: ser.map(|v| v.values()),
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
                if let Some(keys) = self.attribute_int_keys
                    && keys.is_valid(attribute_index)
                {
                    let value_index = unsafe { keys.value_unchecked(attribute_index) };
                    return Some(AttributeValueOrIndex::ValueIndex(value_index));
                }
            }
            3 => {
                if let Some(doubles) = self.attribute_doubles
                    && doubles.is_valid(attribute_index)
                {
                    let value = unsafe { doubles.value_unchecked(attribute_index) };
                    return Some(AttributeValueOrIndex::Value(ValueOrRef::Double(value)));
                }
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
            2 => ValueOrRef::Integer(unsafe {
                self.attribute_int_values
                    .unwrap()
                    .value_unchecked(attribute_value_index as usize)
            }),
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

            RecordTableDictionary::new(keys, RecordTableDictionaryValueArray::Vec(values.into()))
        } else {
            let key_buffer = MutableBuffer::from_len_zeroed(record_count * 2);

            RecordTableDictionary::new(
                PrimitiveArray::<UInt16Type>::new(
                    key_buffer.into(),
                    Some(NullBuffer::new_null(record_count)),
                )
                .into(),
                RecordTableDictionaryValueArray::Vec(vec![].into()),
            )
        };

        let copy = value.clone();

        cache.insert(key.into(), value);

        Some(RecordTableValue::Dictionary(copy))
    }

    fn get_child_table_mut(&mut self, _key: &str) -> Option<&mut dyn RecordTable> {
        None
    }

    fn set_values(&mut self, _key: &str, _values: Dictionary<'_>) {
        todo!()
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

enum AttributeValueOrIndex {
    ValueIndex(u16),
    Value(ValueOrRef<'static>),
}

fn push_null(null_buffer: &mut Option<MutableBuffer>, index: usize, key_bit_length: usize) {
    if let Some(buffer) = null_buffer {
        let ptr = buffer.typed_data_mut::<u8>().as_mut_ptr();

        let i = index / 8;
        let b = 1 << (index % 8);

        unsafe { *ptr.add(i) &= !b };
    } else {
        let mut buffer = MutableBuffer::new(key_bit_length).with_bitset(key_bit_length, true);

        let ptr = buffer.typed_data_mut::<u8>().as_mut_ptr();

        let i = index / 8;
        let b = 1 << (index % 8);

        unsafe { *ptr.add(i) &= !b };

        *null_buffer = Some(buffer);
    }
}

fn build_logs_body_dictionary(
    logs: &RecordBatch,
    logs_schema: &Schema,
) -> Option<RecordTableDictionary> {
    if let Some(body_column) = logs_schema.column_with_name("body") {
        let body_struct = logs.column(body_column.0).as_struct();

        if let Some(body_type) = body_struct.column_by_name("type") {
            let body_types = body_type.as_primitive::<UInt8Type>();

            let record_count = body_types.len();

            let mut key_buffer = MutableBuffer::from_len_zeroed(2 * record_count);
            let key_builder = key_buffer.typed_data_mut::<u16>().as_mut_ptr();

            let key_bit_length = arrow::util::bit_util::ceil(record_count, 8);
            let mut null_buffer = None;

            let mut value_lookup: AHashMap<usize, u16> = AHashMap::with_capacity(record_count);
            let mut values = Vec::with_capacity(record_count);

            let body_strings = OnceCell::new();
            let body_ints = OnceCell::new();
            let body_doubles = OnceCell::new();
            let body_bools = OnceCell::new();
            let body_bytes = OnceCell::new();
            let body_ser = OnceCell::new();

            for (key_index, body_type) in body_types.values().iter().enumerate() {
                match *body_type {
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
                    0 => {}
                    1 => {
                        if let Some(body_strings) = body_strings.get_or_init(|| {
                            body_struct.column_by_name("str").map(|v| {
                                v.as_dictionary::<UInt16Type>()
                                    .downcast_dict::<StringArray>()
                                    .expect("body string values were an unexpected type")
                            })
                        }) {
                            let value_index = body_strings.keys().value(key_index) as usize;

                            let lookup_key = (1 << 16) | value_index;
                            let index = match value_lookup.entry(lookup_key) {
                                Entry::Occupied(occupied) => occupied.into_mut(),
                                Entry::Vacant(vacant) => {
                                    let index = values.len();
                                    values.push(ValueOrRef::String(StringValueOrRef::Buffer({
                                        let strings = body_strings.values();
                                        let offsets = strings.value_offsets();
                                        let end =
                                            unsafe { *offsets.get_unchecked(index + 1) } as usize;
                                        let start =
                                            unsafe { *offsets.get_unchecked(index) } as usize;
                                        strings.values().slice_with_length(start, end - start)
                                    })));
                                    vacant.insert(index as u16)
                                }
                            };
                            unsafe { *key_builder.add(key_index) = *index };
                            continue;
                        }
                    }
                    2 => {
                        if let Some(body_ints) = body_ints.get_or_init(|| {
                            body_struct.column_by_name("int").map(|v| {
                                v.as_dictionary::<UInt16Type>()
                                    .downcast_dict::<Int64Array>()
                                    .expect("body int values were an unexpected type")
                            })
                        }) {
                            let value_index = body_ints.keys().value(key_index) as usize;

                            let lookup_key = (2 << 16) | value_index;
                            let index = match value_lookup.entry(lookup_key) {
                                Entry::Occupied(occupied) => occupied.into_mut(),
                                Entry::Vacant(vacant) => {
                                    let index = values.len();
                                    values.push(ValueOrRef::Integer(
                                        body_ints.values().value(value_index),
                                    ));
                                    vacant.insert(index as u16)
                                }
                            };
                            unsafe { *key_builder.add(key_index) = *index };
                            continue;
                        }
                    }
                    3 => {
                        if let Some(body_doubles) = body_doubles.get_or_init(|| {
                            body_struct
                                .column_by_name("double")
                                .map(|v| v.as_primitive::<Float64Type>())
                        }) {
                            let index = values.len() as u16;
                            values.push(ValueOrRef::Double(body_doubles.value(key_index)));

                            unsafe { *key_builder.add(key_index) = index };
                            continue;
                        }
                    }
                    4 => {
                        if let Some(body_bools) = body_bools.get_or_init(|| {
                            body_struct.column_by_name("bool").map(|v| v.as_boolean())
                        }) {
                            let index = values.len() as u16;
                            values.push(ValueOrRef::Boolean(body_bools.value(key_index)));

                            unsafe { *key_builder.add(key_index) = index };
                            continue;
                        }
                    }
                    5 => {
                        if let Some(body_ser) = body_ser.get_or_init(|| {
                            body_struct.column_by_name("ser").map(|v| {
                                v.as_dictionary::<UInt16Type>()
                                    .downcast_dict::<BinaryArray>()
                                    .expect("body ser values were an unexpected type")
                            })
                        }) {
                            let value_index = body_ser.keys().value(key_index) as usize;

                            let lookup_key = (5 << 16) | value_index;
                            let index = match value_lookup.entry(lookup_key) {
                                Entry::Occupied(occupied) => occupied.into_mut(),
                                Entry::Vacant(vacant) => {
                                    let index = values.len();
                                    values.push(
                                        crate::serialization::from_slice(
                                            body_ser.values().value(value_index),
                                        )
                                        .unwrap_or(ValueOrRef::Null),
                                    );
                                    vacant.insert(index as u16)
                                }
                            };
                            unsafe { *key_builder.add(key_index) = *index };
                            continue;
                        }
                    }
                    6 => {
                        if let Some(body_ser) = body_ser.get_or_init(|| {
                            body_struct.column_by_name("ser").map(|v| {
                                v.as_dictionary::<UInt16Type>()
                                    .downcast_dict::<BinaryArray>()
                                    .expect("body ser values were an unexpected type")
                            })
                        }) {
                            let value_index = body_ser.keys().value(key_index) as usize;

                            let lookup_key = (6 << 16) | value_index;
                            let index = match value_lookup.entry(lookup_key) {
                                Entry::Occupied(occupied) => occupied.into_mut(),
                                Entry::Vacant(vacant) => {
                                    let index = values.len();
                                    values.push(
                                        crate::serialization::from_slice(
                                            body_ser.values().value(value_index),
                                        )
                                        .unwrap_or(ValueOrRef::Null),
                                    );
                                    vacant.insert(index as u16)
                                }
                            };
                            unsafe { *key_builder.add(key_index) = *index };
                            continue;
                        }
                    }
                    7 => {
                        if let Some(body_bytes) = body_bytes.get_or_init(|| {
                            body_struct.column_by_name("bytes").map(|v| {
                                v.as_dictionary::<UInt16Type>()
                                    .downcast_dict::<BinaryArray>()
                                    .expect("body byte values were an unexpected type")
                            })
                        }) {
                            let value_index = body_bytes.keys().value(key_index) as usize;

                            let lookup_key = (7 << 16) | value_index;
                            let index = match value_lookup.entry(lookup_key) {
                                Entry::Occupied(occupied) => occupied.into_mut(),
                                Entry::Vacant(vacant) => {
                                    let index = values.len();
                                    values.push(ValueOrRef::Array(ArrayValueOrRef::Buffer({
                                        let bytes = body_bytes.values();
                                        let offsets = bytes.value_offsets();
                                        let start =
                                            unsafe { *offsets.get_unchecked(index) } as usize;
                                        let end =
                                            unsafe { *offsets.get_unchecked(index + 1) } as usize;
                                        let buffer = bytes
                                            .values()
                                            .slice_with_length(start, end - start)
                                            .clone();
                                        BufferArray::new_u8(buffer)
                                    })));
                                    vacant.insert(index as u16)
                                }
                            };
                            unsafe { *key_builder.add(key_index) = *index };
                            continue;
                        }
                    }
                    d => todo!("Body type '{d}' is not supported"),
                }

                push_null(&mut null_buffer, key_index, key_bit_length);
            }

            return Some(RecordTableDictionary::new(
                PrimitiveArray::<UInt16Type>::new(
                    key_buffer.into(),
                    null_buffer
                        .and_then(|v| NullBufferBuilder::new_from_buffer(v, record_count).finish()),
                )
                .into(),
                RecordTableDictionaryValueArray::Vec(values.into()),
            ));
        }
    }

    None
}
