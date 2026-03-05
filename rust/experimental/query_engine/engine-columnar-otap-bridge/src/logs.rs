// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{cell::{Ref, RefCell}, collections::{HashMap, HashSet}, fmt::Display};

use arrow::{
    array::*,
    buffer::{MutableBuffer, NullBuffer},
    compute::{FilterPredicate, kernels::filter},
    datatypes::*,
};
use data_engine_columnar::*;
use indexmap::IndexMap;

pub(crate) struct OtapLogRecordBatchFactory {}

impl ColumnarRecordsFactory<4> for OtapLogRecordBatchFactory {
    type Records<'a> = OtapLogRecordBatch<'a>;

    fn create<'a>(batches: &'a [Option<RecordBatch>]) -> OtapLogRecordBatch<'a> {
        let logs = batches[2].as_ref().unwrap();
        let logs_schema = logs.schema_ref();

        let attributes = if let Some(id_column) = logs_schema.column_with_name("id") {
            let ids = logs.column(id_column.0).as_primitive::<UInt16Type>();

            Some(OtapAttributes::new(ids, batches[3].as_ref().unwrap()))
        } else {
            None
        };

        OtapLogRecordBatch::new(logs, logs_schema, attributes)
    }

    fn filter(batch: &OtapLogRecordBatch, filter: &BooleanArray) -> [Option<RecordBatch>; 4] {
        let filtered_logs = filter::filter_record_batch(batch.logs, filter).unwrap();
        let number_of_logs_after_filter = filtered_logs.num_rows();
        if number_of_logs_after_filter > 0 {
            let ids = if let Some(id_column) = filtered_logs.schema_ref().column_with_name("id") {
                let mut ids = HashSet::with_capacity(number_of_logs_after_filter);

                for id in filtered_logs.column(id_column.0).as_primitive::<UInt16Type>() {
                    if let Some(id) = id {
                        ids.insert(id);
                    }
                }

                ids
            } else {
                HashSet::new()
            };

            let resource = batch.resource.as_ref().map(|v| filter_child_batch(&ids, v));

            let scope = batch.scope.as_ref().map(|v| filter_child_batch(&ids, v));

            let attributes = batch.attributes.as_ref().map(|v| filter_child_batch(&ids, v.batch));

            let batches = [
                resource,
                scope,
                Some(filtered_logs),
                attributes,
            ];

            return batches;
        }

        [None, None, None, None]
    }
}

fn filter_child_batch(ids: &HashSet<u16>, child_batch: &RecordBatch) -> RecordBatch {
    let mut filter = BooleanBuilder::with_capacity(child_batch.num_rows());

    for parent_id in child_batch.column(0).as_primitive::<UInt16Type>() {
        if let Some(parent_id) = parent_id && ids.contains(&parent_id) {
            filter.append_value(true);
        } else {
            filter.append_value(false);
        }
    }

    filter::filter_record_batch(child_batch, &filter.finish()).unwrap()
}

#[derive(Debug)]
pub(crate) struct OtapLogRecordBatch<'record> {
    logs: &'record RecordBatch,
    logs_schema: &'record SchemaRef,
    attributes: Option<OtapAttributes<'record>>,
    resource: Option<RecordBatch>,
    scope: Option<RecordBatch>
}

impl<'record> OtapLogRecordBatch<'record> {
    pub fn new(
        logs: &'record RecordBatch,
        logs_schema: &'record SchemaRef,
        attributes: Option<OtapAttributes<'record>>,
    ) -> OtapLogRecordBatch<'record> {
        Self {
            logs,
            logs_schema,
            attributes,
            resource: None,
            scope: None
        }
    }
}

impl ColumnarRecords for OtapLogRecordBatch<'_> {
    fn get_diagnostic_level(&self) -> Option<ColumnarEngineDiagnosticLevel> {
        None
    }

    fn len(&self) -> usize {
        self.logs.num_rows()
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

        if key == "SeverityText" || key == "severity_text" {
            if let Some(severity_text_column) = self.logs_schema.column_with_name("severity_text") {
                let severity_text_array = self
                    .logs
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
                self.logs_schema.column_with_name("severity_number")
            {
                let severity_number_array = self
                    .logs
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

        None
    }
}

impl Display for OtapLogRecordBatch<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Logs(RecordCount={})", self.logs.num_rows())
    }
}

#[derive(Debug)]
pub(crate) struct OtapAttributes<'record> {
    batch: &'record RecordBatch,
    number_of_records: usize,
    id_map: HashMap<u16, usize>,
    cache: RefCell<HashMap<Box<str>, Dictionary<'record>>>,
    attribute_parent_ids: &'record PrimitiveArray<UInt16Type>,
    attribute_keys:
        TypedDictionaryArray<'record, UInt8Type, GenericByteArray<GenericStringType<i32>>>,
    attribute_types: &'record PrimitiveArray<UInt8Type>,
    attribute_strings:
        TypedDictionaryArray<'record, UInt16Type, GenericByteArray<GenericStringType<i32>>>,
    attribute_ints: Option<TypedDictionaryArray<'record, UInt16Type, PrimitiveArray<Int64Type>>>,
    attribute_doubles:
        Option<TypedDictionaryArray<'record, UInt16Type, PrimitiveArray<Float64Type>>>,
    attribute_bools: Option<TypedDictionaryArray<'record, UInt16Type, BooleanArray>>,
}

impl<'record> OtapAttributes<'record> {
    pub fn new(
        ids: &PrimitiveArray<UInt16Type>,
        attributes_batch: &'record RecordBatch,
    ) -> OtapAttributes<'record> {
        let number_of_records = ids.len();
        let mut id_map = HashMap::with_capacity(number_of_records);
        let mut index = 0;
        for id in ids {
            if let Some(id) = id {
                id_map.insert(id, index);
            }
            index += 1;
        }
        id_map.shrink_to_fit();

        Self {
            batch: attributes_batch,
            number_of_records,
            id_map,
            cache: RefCell::new(HashMap::new()),
            attribute_parent_ids: attributes_batch.column(0).as_primitive::<UInt16Type>(),
            attribute_keys: attributes_batch
                .column(1)
                .as_dictionary::<UInt8Type>()
                .downcast_dict::<StringArray>()
                .expect("Attribute keys were an unexpected type"),
            attribute_types: attributes_batch.column(2).as_primitive::<UInt8Type>(),
            attribute_strings: attributes_batch
                .column(3)
                .as_dictionary::<UInt16Type>()
                .downcast_dict::<StringArray>()
                .expect("Attribute strings were an unexpected type"),
            attribute_ints: attributes_batch.column_by_name("int").map(|c| {
                c.as_dictionary::<UInt16Type>()
                    .downcast_dict::<PrimitiveArray<Int64Type>>()
                    .expect("Attribute ints were an unexpected type")
            }),
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
}

impl<'record> RecordTable for OtapAttributes<'record> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>> {
        let mut cache = self.cache.borrow_mut();

        if let Some(d) = cache.get(key) {
            return Some(RecordTableValue::Dictionary(d.clone()))
        }

        let search_key = Some(key);

        let record_count = self.number_of_records;

        let value = if let Some(value_index) = self
            .attribute_keys
            .values()
            .iter()
            .position(|v| v == search_key)
        {
            let mut key_buffer = MutableBuffer::from_len_zeroed(record_count * 2);
            let keys = key_buffer.typed_data_mut::<u16>();

            let mut values: IndexMap<(u8, u16), ValueOrRef> = IndexMap::with_capacity(record_count);

            let mut nulls = BooleanBufferBuilder::new(record_count);
            nulls.advance(record_count);

            let value_index = Some(value_index as u8);
            for (id_index, id_value) in self.attribute_keys.keys().iter().enumerate() {
                if value_index == id_value {
                    let parent_id = self.attribute_parent_ids.value(id_index);
                    let record_index = self.id_map.get(&parent_id).unwrap();

                    if let Some((attribute_type, value_index, value)) =
                        self.get_attribute_value(id_index)
                    {
                        let index = match values.entry((attribute_type, value_index)) {
                            indexmap::map::Entry::Occupied(occupied_entry) => {
                                occupied_entry.index()
                            }
                            indexmap::map::Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert_entry(value).index()
                            }
                        };

                        keys[*record_index as usize] = index as u16;
                        nulls.set_bit(*record_index, true);
                    }
                }
            }

            Dictionary::new(
                PrimitiveArray::<UInt16Type>::new(
                    key_buffer.into(),
                    Some(NullBuffer::new(nulls.finish())),
                )
                .into(),
                DictionaryValueArray::AnyOwned(values.into_values().collect()),
            )
        } else {
            let key_buffer = MutableBuffer::from_len_zeroed(record_count * 2);

            let mut nulls = BooleanBufferBuilder::new(record_count);
            nulls.advance(record_count);

            Dictionary::new(
                PrimitiveArray::<UInt16Type>::new(
                    key_buffer.into(),
                    Some(NullBuffer::new(nulls.finish())),
                )
                .into(),
                DictionaryValueArray::AnyOwned(vec![]),
            )
        };

        let copy = value.clone();

        cache.insert(key.into(), value);

        Some(RecordTableValue::Dictionary(copy))
    }
}

impl Display for OtapAttributes<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Attributes(RecordCount={})", self.attribute_parent_ids.len())
    }
}