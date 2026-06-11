// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::{OnceCell, RefCell},
    collections::hash_map::Entry,
    fmt::Display,
    hash::Hash,
    ops::Deref,
    rc::Rc,
    sync::Arc,
};

use ahash::AHashMap;
use arrow::{
    array::*,
    buffer::{MutableBuffer, NullBuffer},
    compute::kernels::filter,
    datatypes::*,
};
use data_engine_columnar::*;
use data_engine_expressions::*;
use otap_df_pdata::{
    encode::record::logs::LogsBodyBuilder,
    otap::{
        raw_batch_store::POSITION_LOOKUP,
        transform::{materialize_parent_id_for_attributes, remove_delta_encoding_from_column},
    },
    proto::opentelemetry::arrow::v1::ArrowPayloadType,
    schema::consts::{self, metadata},
};
use roaring::RoaringBitmap;
use strum::*;

use crate::{
    filter::{IdBitmap, filter_child_batch},
    *,
};

static LOGS_BATCH_POSITION: usize = POSITION_LOOKUP[ArrowPayloadType::Logs as usize];
static LOG_ATTRIBUTES_BATCH_POSITION: usize = POSITION_LOOKUP[ArrowPayloadType::LogAttrs as usize];
static RESOURCE_ATTRIBUTES_BATCH_POSITION: usize =
    POSITION_LOOKUP[ArrowPayloadType::ResourceAttrs as usize];
static SCOPE_ATTRIBUTES_BATCH_POSITION: usize =
    POSITION_LOOKUP[ArrowPayloadType::ScopeAttrs as usize];

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
    type Records<'pipeline, 'record> = OtapLogRecordBatch<'pipeline, 'record>;
    type State<'pipeline> = OtapLogRecordState<'pipeline>;

    fn create<'pipeline, 'record>(
        &self,
        state: Option<Self::State<'pipeline>>,
        batches: &'record [Option<RecordBatch>; 4],
    ) -> OtapLogRecordBatch<'pipeline, 'record> {
        if let Some(logs) = batches[LOGS_BATCH_POSITION].as_ref() {
            let logs_schema = logs.schema_ref();

            let attributes = if let Some(id_column) = logs_schema.column_with_name(consts::ID)
                && let Some(attributes_batch) = batches[LOG_ATTRIBUTES_BATCH_POSITION].as_ref()
            {
                Some(OtapAttributes::new(
                    OtapIds::new(
                        logs.column(id_column.0).as_primitive::<UInt16Type>(),
                        id_column
                            .1
                            .metadata()
                            .get(metadata::COLUMN_ENCODING)
                            .map(|v| v.as_str()),
                    ),
                    attributes_batch,
                ))
            } else {
                None
            };

            let resource = if let Some(resource_column) =
                logs_schema.column_with_name(consts::RESOURCE)
                && let Some(resource_struct) = logs.column(resource_column.0).as_struct_opt()
            {
                if let DataType::Struct(fields) = resource_struct.data_type()
                    && let Some(id_column) = fields.find(consts::ID)
                    && let Some(resource_attributes_batch) =
                        batches[RESOURCE_ATTRIBUTES_BATCH_POSITION].as_ref()
                {
                    Some(OtapResource {
                        resource_struct,
                        attributes: Some(OtapAttributes::new(
                            OtapIds::new(
                                resource_struct
                                    .column(id_column.0)
                                    .as_primitive::<UInt16Type>(),
                                id_column
                                    .1
                                    .metadata()
                                    .get(metadata::COLUMN_ENCODING)
                                    .map(|v| v.as_str()),
                            ),
                            resource_attributes_batch,
                        )),
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

            let scope = if let Some(scope_column) = logs_schema.column_with_name(consts::SCOPE)
                && let Some(scope_struct) = logs.column(scope_column.0).as_struct_opt()
            {
                if let DataType::Struct(fields) = scope_struct.data_type()
                    && let Some(id_column) = fields.find(consts::ID)
                    && let Some(scope_attributes_batch) =
                        batches[SCOPE_ATTRIBUTES_BATCH_POSITION].as_ref()
                {
                    Some(OtapScope {
                        scope_struct,
                        attributes: Some(OtapAttributes::new(
                            OtapIds::new(
                                scope_struct
                                    .column(id_column.0)
                                    .as_primitive::<UInt16Type>(),
                                id_column
                                    .1
                                    .metadata()
                                    .get(metadata::COLUMN_ENCODING)
                                    .map(|v| v.as_str()),
                            ),
                            scope_attributes_batch,
                        )),
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
                attributes,
                resource,
                scope,
                state,
            )
        } else {
            OtapLogRecordBatch::new_empty()
        }
    }

    fn filter<'pipeline>(
        &self,
        state: &mut OtapLogRecordState,
        batches: &mut [Option<RecordBatch>; 4],
        filter: &BooleanArray,
    ) {
        let filter_true_count = filter.true_count();

        if let Some(mut logs) = batches[LOGS_BATCH_POSITION].take()
            && filter_true_count > 0
        {
            let number_of_logs_before_filter = logs.num_rows();
            if filter_true_count == number_of_logs_before_filter {
                batches[LOGS_BATCH_POSITION] = Some(logs);
                return;
            }

            let mut decoded_ids = [
                (consts::ID, None),
                (consts::SCOPE, None),
                (consts::RESOURCE, None),
            ];
            let mut decode = false;
            if let Some(ids) = state.decoded_attribute_ids.ids.take() {
                decoded_ids[0].1 = Some(ids);
                decode = true;
            }
            if let Some(scope_ids) = state.decoded_scope_ids.ids.take() {
                decoded_ids[1].1 = Some(scope_ids);
                decode = true;
            }
            if let Some(resource_ids) = state.decoded_resource_ids.ids.take() {
                decoded_ids[2].1 = Some(resource_ids);
                decode = true;
            }
            if decode {
                logs = replace_id_columns_in_batch(decoded_ids, logs);
            }

            let filtered_logs_batch = filter::filter_record_batch(&logs, filter).unwrap();

            let number_of_logs_after_filter = filtered_logs_batch.num_rows();
            if number_of_logs_after_filter > 0 {
                let mut ids = IdBitmap::new();

                if let Some(attributes_batch) = batches[LOG_ATTRIBUTES_BATCH_POSITION].take() {
                    if let Some(id_column) = filtered_logs_batch
                        .schema_ref()
                        .column_with_name(consts::ID)
                    {
                        ids.populate(
                            filtered_logs_batch
                                .column(id_column.0)
                                .as_primitive::<UInt16Type>()
                                .iter()
                                .flatten()
                                .map(|i| i.into()),
                        );
                    }

                    if !ids.is_empty() {
                        batches[LOG_ATTRIBUTES_BATCH_POSITION] = filter_child_batch(
                            &ids,
                            state.decoded_attribute_ids.parent_ids.take(),
                            &attributes_batch,
                        );
                    }
                }

                if let Some(resource_attributes) =
                    batches[RESOURCE_ATTRIBUTES_BATCH_POSITION].take()
                {
                    ids.clear();

                    if let Some(resource_column) = filtered_logs_batch
                        .schema_ref()
                        .column_with_name(consts::RESOURCE)
                        && let Some(resource_struct) = filtered_logs_batch
                            .column(resource_column.0)
                            .as_struct_opt()
                        && let Some(resource_ids) = resource_struct.column_by_name(consts::ID)
                    {
                        ids.populate(
                            resource_ids
                                .as_primitive::<UInt16Type>()
                                .iter()
                                .flatten()
                                .map(|i| i.into()),
                        );
                    }

                    if !ids.is_empty() {
                        batches[RESOURCE_ATTRIBUTES_BATCH_POSITION] = filter_child_batch(
                            &ids,
                            state.decoded_resource_ids.parent_ids.take(),
                            &resource_attributes,
                        );
                    }
                }

                if let Some(scope_attributes) = batches[SCOPE_ATTRIBUTES_BATCH_POSITION].take() {
                    ids.clear();

                    if let Some(scope_column) = filtered_logs_batch
                        .schema_ref()
                        .column_with_name(consts::SCOPE)
                        && let Some(scope_struct) =
                            filtered_logs_batch.column(scope_column.0).as_struct_opt()
                        && let Some(scope_ids) = scope_struct.column_by_name(consts::ID)
                    {
                        ids.populate(
                            scope_ids
                                .as_primitive::<UInt16Type>()
                                .iter()
                                .flatten()
                                .map(|i| i.into()),
                        );
                    }

                    if !ids.is_empty() {
                        batches[SCOPE_ATTRIBUTES_BATCH_POSITION] = filter_child_batch(
                            &ids,
                            state.decoded_scope_ids.parent_ids.take(),
                            &scope_attributes,
                        );
                    };
                }

                batches[LOGS_BATCH_POSITION] = Some(filtered_logs_batch);

                return;
            }
        }

        *batches = [None, None, None, None];
    }

    fn set<'pipeline, T: ColumnarEngineDiagnosticReceiver<'pipeline>>(
        &self,
        diagnostic_receiver: &T,
        expression: &'pipeline dyn Expression,
        state: &mut OtapLogRecordState<'pipeline>,
        batches: &mut [Option<RecordBatch>; 4],
        root: &ColumnarEngineSelectionPath<'pipeline>,
        path: &[ColumnarEngineSelectionPath<'pipeline>],
        key_filter: Option<&RoaringBitmap>,
        value: Dictionary<'pipeline>,
    ) -> ColumnarRecordsWriteResult {
        let logs = batches[LOGS_BATCH_POSITION]
            .as_ref()
            .expect("has log records");

        match root {
            ColumnarEngineSelectionPath::Key {
                expression: key_expression,
                value: root_key,
            } => {
                let field = match get_log_record_schema().normalize_key(root_key.get_value()) {
                    consts::ATTRIBUTES => {
                        todo!()
                    }
                    consts::BODY => OtapLogRecordField::Body,
                    consts::TIME_UNIX_NANO => OtapLogRecordField::TimeUnixNano,
                    consts::OBSERVED_TIME_UNIX_NANO => OtapLogRecordField::ObservedTimeUnixNano,
                    consts::SEVERITY_NUMBER => OtapLogRecordField::SeverityNumber,
                    consts::SEVERITY_TEXT => OtapLogRecordField::SeverityText,
                    consts::TRACE_ID => OtapLogRecordField::TraceId,
                    consts::SPAN_ID => OtapLogRecordField::SpanId,
                    consts::FLAGS => OtapLogRecordField::Flags,
                    consts::EVENT_NAME => OtapLogRecordField::EventName,
                    f => {
                        diagnostic_receiver.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Warn,
                            *key_expression,
                            || format!("Field '{f}' does not exist on log record"),
                        );
                        return ColumnarRecordsWriteResult::NotFound;
                    }
                };

                process_log_record_field_update(
                    diagnostic_receiver,
                    expression,
                    field,
                    &mut state.fields,
                    logs,
                    *key_expression,
                    path,
                    key_filter,
                    &value,
                )
            }
            ColumnarEngineSelectionPath::Dictionary {
                expression: keys_expression,
                value: root_keys,
            } => {
                let key_length = root_keys.len();

                let mut plan: AHashMap<StringValueOrRef, RoaringBitmap> =
                    AHashMap::with_capacity(key_length);

                match key_filter {
                    Some(v) => build_plan(root_keys, &mut plan, v.iter().map(|v| v as usize)),
                    None => build_plan(root_keys, &mut plan, 0..key_length),
                }

                let mut written_data_count = 0;
                let plan_count = plan.len();

                for (key, key_filter) in plan.into_iter() {
                    let field = match get_log_record_schema().normalize_key(key.get_value()) {
                        consts::BODY => OtapLogRecordField::Body,
                        consts::TIME_UNIX_NANO => OtapLogRecordField::TimeUnixNano,
                        consts::OBSERVED_TIME_UNIX_NANO => OtapLogRecordField::ObservedTimeUnixNano,
                        consts::SEVERITY_NUMBER => OtapLogRecordField::SeverityNumber,
                        consts::SEVERITY_TEXT => OtapLogRecordField::SeverityText,
                        consts::TRACE_ID => OtapLogRecordField::TraceId,
                        consts::SPAN_ID => OtapLogRecordField::SpanId,
                        consts::FLAGS => OtapLogRecordField::Flags,
                        consts::EVENT_NAME => OtapLogRecordField::EventName,
                        f => {
                            diagnostic_receiver.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                *keys_expression,
                                || format!("Field '{f}' does not exist on log record"),
                            );
                            continue;
                        }
                    };

                    let logs = batches[LOGS_BATCH_POSITION]
                        .as_ref()
                        .expect("has log records");

                    if process_log_record_field_update(
                        diagnostic_receiver,
                        expression,
                        field,
                        &mut state.fields,
                        logs,
                        *keys_expression,
                        path,
                        Some(&key_filter),
                        &value,
                    ) != ColumnarRecordsWriteResult::NotFound
                    {
                        written_data_count += 1;
                    }
                }

                if written_data_count == 0 {
                    ColumnarRecordsWriteResult::NotFound
                } else if written_data_count == plan_count {
                    ColumnarRecordsWriteResult::Success
                } else {
                    ColumnarRecordsWriteResult::PartialSuccess
                }
            }
            ColumnarEngineSelectionPath::Index {
                expression,
                value: _,
            } => {
                diagnostic_receiver.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    *expression,
                    || "Log record cannot be accessed by array index".into(),
                );
                ColumnarRecordsWriteResult::NotFound
            }
        }
    }

    fn apply<'pipeline, T: ColumnarEngineDiagnosticReceiver<'pipeline>>(
        &self,
        diagnostic_receiver: &T,
        expression: &'pipeline dyn Expression,
        state: &mut OtapLogRecordState<'pipeline>,
        batches: &mut [Option<RecordBatch>; 4],
    ) {
        let mut logs = batches[LOGS_BATCH_POSITION].take().expect("has logs");

        for field in OtapLogRecordField::VARIANTS {
            if let Some(value) = state.fields.take_if_modified(*field) {
                let (column_name, transform) = field.get_column_name_and_transform();

                match value {
                    OtapLogRecordModifiedField::Removed => {
                        logs = remove_column(diagnostic_receiver, expression, logs, column_name);
                    }
                    OtapLogRecordModifiedField::Set(v) => {
                        logs = set_column(
                            diagnostic_receiver,
                            expression,
                            logs,
                            column_name,
                            v,
                            transform,
                        );
                    }
                }
            }
        }

        batches[LOGS_BATCH_POSITION] = Some(logs);
    }
}

#[cfg(test)]
impl Default for OtapLogRecordBatchFactory {
    fn default() -> Self {
        Self::new()
    }
}

fn process_log_record_field_update<'pipeline, T: ColumnarEngineDiagnosticReceiver<'pipeline>>(
    diagnostic_receiver: &T,
    expression: &'pipeline dyn Expression,
    field: OtapLogRecordField,
    fields: &mut OtapLogRecordFields<'pipeline>,
    logs: &RecordBatch,
    root_expression: &'pipeline dyn Expression,
    path: &[ColumnarEngineSelectionPath<'pipeline>],
    key_filter: Option<&RoaringBitmap>,
    value: &Dictionary<'pipeline>,
) -> ColumnarRecordsWriteResult {
    let path_length = path.len();

    let value = if path_length > 0 {
        if !field.supports_access_by_path() {
            diagnostic_receiver.add_diagnostic_if_enabled(
                ColumnarEngineDiagnosticLevel::Warn,
                root_expression,
                || format!("Cannot access into field '{field:?}'"),
            );
            return ColumnarRecordsWriteResult::NotFound;
        }

        match fields.take(field, logs) {
            OtapValue::NotFound | OtapValue::Removed => {
                diagnostic_receiver.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    root_expression,
                    || format!("Cannot access into empty '{field:?}'"),
                );
                return ColumnarRecordsWriteResult::NotFound;
            }
            OtapValue::Read(v) | OtapValue::Set(v) => {
                update_dictionary_values_for_path(v, key_filter, &path[0], &path[1..], value)
            }
        }
    } else if let Some(key_filter) = key_filter {
        let existing_values = match fields.take(field, logs) {
            OtapValue::Read(v) | OtapValue::Set(v) => v,
            OtapValue::Removed | OtapValue::NotFound => {
                let key_length = logs.num_rows();
                Dictionary::new_null_with_data_type(key_length, DataType::UInt16)
            }
        };

        existing_values.with_values(Some(key_filter), value)
    } else {
        value.clone()
    };

    diagnostic_receiver.add_diagnostic_if_enabled(
        ColumnarEngineDiagnosticLevel::Verbose,
        expression,
        || format!("Field '{field:?}' set to: {value}"),
    );

    fields.set(field, OtapValue::Set(value));

    ColumnarRecordsWriteResult::Success
}

fn build_plan<'pipeline, T: Iterator<Item = usize>>(
    root_keys: &Dictionary<'pipeline>,
    plan: &mut AHashMap<StringValueOrRef<'pipeline>, RoaringBitmap>,
    key_iter: T,
) {
    for key_index in key_iter {
        match root_keys.get_value(key_index) {
            ValueOrRef::String(key) => {
                match plan.entry(key) {
                    Entry::Occupied(mut o) => {
                        o.get_mut()
                            .try_push(key_index as u32)
                            .expect("key_index pushed");
                    }
                    Entry::Vacant(v) => {
                        v.insert(RoaringBitmap::from([key_index as u32]));
                    }
                };
            }
            v => {
                // todo: Log
            }
        }
    }
}

fn replace_id_columns_in_batch<const SIZE: usize>(
    decoded_ids: [(&str, Option<PrimitiveArray<UInt16Type>>); SIZE],
    batch: RecordBatch,
) -> RecordBatch {
    let (schema, mut columns, _) = batch.into_parts();

    let mut schema_builder: SchemaBuilder = schema.as_ref().into();

    for (column_name, decoded_ids) in decoded_ids.into_iter() {
        if let Some(decoded_ids) = decoded_ids {
            let (column_id, field) = schema.column_with_name(column_name).expect("has ids");

            if let DataType::Struct(_) = field.data_type() {
                let (struct_fields, mut struct_columns, struct_nulls) =
                    columns[column_id].as_struct().clone().into_parts();

                let (struct_column_id, field) = struct_fields.find(consts::ID).expect("has ids");

                let mut field = field.as_ref().clone();

                field.metadata_mut().insert(
                    metadata::COLUMN_ENCODING.into(),
                    metadata::encodings::PLAIN.into(),
                );

                let mut struct_fields = struct_fields.to_vec();

                struct_fields[struct_column_id] = Arc::new(field);

                struct_columns[struct_column_id] = Arc::new(decoded_ids);

                columns[column_id] = Arc::new(StructArray::new(
                    struct_fields.into(),
                    struct_columns,
                    struct_nulls,
                ));
            } else {
                let mut field = field.clone();

                field.metadata_mut().insert(
                    metadata::COLUMN_ENCODING.into(),
                    metadata::encodings::PLAIN.into(),
                );

                *schema_builder.field_mut(column_id) = Arc::new(field);

                columns[column_id] = Arc::new(decoded_ids);
            }
        }
    }

    RecordBatch::try_new(Arc::new(schema_builder.finish()), columns).unwrap()
}

fn update_dictionary_values_for_path<'a>(
    source: Dictionary<'a>,
    key_filter: Option<&RoaringBitmap>,
    current_path: &ColumnarEngineSelectionPath<'a>,
    remaining_path: &[ColumnarEngineSelectionPath<'a>],
    value: &Dictionary<'a>,
) -> Dictionary<'a> {
    let (source_keys, source_values) = source.into_parts();

    let (mut source_values, source_value_lookup) =
        Into::<DictionaryValueArray>::into(source_values).into_set();

    let key_length = source_keys.len();

    let mut visited_values = AHashMap::with_capacity(key_length);

    let mut key_builder = DictionaryKeyArrayBuilder::<UInt16Type>::new(key_length);
    let mut key_writer = key_builder.get_writer();

    for key_index in 0..key_length {
        let value_index = source_keys
            .get_value_index_for_key_index(key_index)
            .and_then(|v| match source_value_lookup.as_ref() {
                Some(l) => l.get(&v).and_then(|v| *v),
                None => Some(v),
            });

        if let Some(value_index) = value_index {
            if let Some(key_filter) = &key_filter
                && !key_filter.contains(key_index as u32)
            {
                unsafe { key_writer.set_value_index_unchecked(key_index, value_index) };
                continue;
            }

            let path_value = match current_path {
                ColumnarEngineSelectionPath::Key { expression, value } => {
                    ValueOrRef::String(value.clone())
                }
                ColumnarEngineSelectionPath::Index { expression, value } => {
                    ValueOrRef::Integer(*value)
                }
                ColumnarEngineSelectionPath::Dictionary { expression, value } => {
                    value.get_value(key_index)
                }
            };

            let value_index = match visited_values.entry((path_value.clone(), value_index)) {
                Entry::Occupied(occupied_entry) => *occupied_entry.get(),
                Entry::Vacant(vacant_entry) => {
                    let source_value = &source_values[value_index];

                    if matches!(source_value, ValueOrRef::Null) {
                        vacant_entry.insert(None);
                        None
                    } else {
                        let inserted_index = match path_value {
                            ValueOrRef::String(key) => {
                                if let ValueOrRef::Map(MapValueOrRef::Owned(map)) = source_value {
                                    let mut map = map.deref().clone();
                                    update_map_value_for_path(
                                        key_index,
                                        map.get_values_mut(),
                                        key.get_value(),
                                        remaining_path,
                                        value.get_value(key_index),
                                    );
                                    let (index, _) = source_values.insert_full(ValueOrRef::Map(
                                        MapValueOrRef::Owned(map.into()),
                                    ));
                                    Some(index)
                                } else {
                                    // todo log
                                    None
                                }
                            }
                            ValueOrRef::Integer(index) => {
                                if let ValueOrRef::Array(ArrayValueOrRef::Owned(array)) =
                                    source_value
                                {
                                    let mut array = array.deref().clone();
                                    update_array_value_for_path(
                                        key_index,
                                        array.get_values_mut(),
                                        index,
                                        remaining_path,
                                        value.get_value(key_index),
                                    );
                                    let (index, _) = source_values.insert_full(ValueOrRef::Array(
                                        ArrayValueOrRef::Owned(array.into()),
                                    ));
                                    Some(index)
                                } else {
                                    // todo log
                                    None
                                }
                            }
                            v => {
                                // todo log
                                None
                            }
                        };

                        let final_index = inserted_index.unwrap_or(value_index);

                        vacant_entry.insert(Some(final_index));
                        Some(final_index)
                    }
                }
            };

            if let Some(value_index) = value_index {
                unsafe { key_writer.set_value_index_unchecked(key_index, value_index) };
                continue;
            }
        }

        unsafe { key_writer.set_null_unchecked(key_index) }
    }

    Dictionary::new(key_builder.finish().into(), source_values.into())
}

fn update_map_value_for_path<'a>(
    key_index: usize,
    map: &mut AHashMap<Box<str>, ValueOrRef<'a>>,
    current_key: &str,
    remaining_path: &[ColumnarEngineSelectionPath<'a>],
    value: ValueOrRef<'a>,
) {
    if let Some(current_path) = remaining_path.first() {
        let path_value = match current_path {
            ColumnarEngineSelectionPath::Key { expression, value } => {
                ValueOrRef::String(value.clone())
            }
            ColumnarEngineSelectionPath::Index { expression, value } => ValueOrRef::Integer(*value),
            ColumnarEngineSelectionPath::Dictionary { expression, value } => {
                value.get_value(key_index)
            }
        };

        if let Entry::Occupied(mut o) = map.entry(current_key.into()) {
            let value_for_key = o.insert(ValueOrRef::Null);

            o.insert(update_any_value_for_path(
                key_index,
                value_for_key,
                path_value,
                remaining_path,
                value,
            ));
        }
    } else {
        match value {
            ValueOrRef::Null => {
                map.remove(current_key);
            }
            v => {
                map.insert(current_key.into(), v);
            }
        }
    }
}

fn update_array_value_for_path<'a>(
    key_index: usize,
    array: &mut [ValueOrRef<'a>],
    mut current_index: i64,
    remaining_path: &[ColumnarEngineSelectionPath<'a>],
    value: ValueOrRef<'a>,
) {
    let len = array.len();

    if current_index < 0 {
        current_index += len as i64;
    }
    if current_index < 0 || current_index >= len as i64 {
        // todo: Log
        return;
    }

    if let Some(current_path) = remaining_path.first() {
        let path_value = match current_path {
            ColumnarEngineSelectionPath::Key { expression, value } => {
                ValueOrRef::String(value.clone())
            }
            ColumnarEngineSelectionPath::Index { expression, value } => ValueOrRef::Integer(*value),
            ColumnarEngineSelectionPath::Dictionary { expression, value } => {
                value.get_value(key_index)
            }
        };

        let value_for_index =
            std::mem::replace(&mut array[current_index as usize], ValueOrRef::Null);

        array[current_index as usize] = update_any_value_for_path(
            key_index,
            value_for_index,
            path_value,
            remaining_path,
            value,
        );
    } else {
        array[current_index as usize] = value
    }
}

fn update_any_value_for_path<'a>(
    key_index: usize,
    any_value: ValueOrRef<'a>,
    current_path: ValueOrRef<'a>,
    remaining_path: &[ColumnarEngineSelectionPath<'a>],
    value: ValueOrRef<'a>,
) -> ValueOrRef<'a> {
    match current_path {
        ValueOrRef::String(path_key) => {
            if let ValueOrRef::Map(MapValueOrRef::Owned(inner_map)) = any_value {
                let mut inner_map = Rc::unwrap_or_clone(inner_map);
                update_map_value_for_path(
                    key_index,
                    inner_map.get_values_mut(),
                    path_key.get_value(),
                    &remaining_path[1..],
                    value,
                );
                ValueOrRef::Map(MapValueOrRef::Owned(inner_map.into()))
            } else {
                //todo: log
                any_value
            }
        }
        ValueOrRef::Integer(index) => {
            if let ValueOrRef::Array(ArrayValueOrRef::Owned(inner_array)) = any_value {
                let mut inner_array = Rc::unwrap_or_clone(inner_array);
                update_array_value_for_path(
                    key_index,
                    inner_array.get_values_mut(),
                    index,
                    &remaining_path[1..],
                    value,
                );
                ValueOrRef::Array(ArrayValueOrRef::Owned(inner_array.into()))
            } else {
                //todo: log
                any_value
            }
        }
        v => {
            //todo: log?
            any_value
        }
    }
}

fn set_column<'a, TDiagnosticReceiver: ColumnarEngineDiagnosticReceiver<'a>>(
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

fn remove_column<'a, TDiagnosticReceiver: ColumnarEngineDiagnosticReceiver<'a>>(
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

fn write_column_values_to_batch<'a, TDiagnosticReceiver: ColumnarEngineDiagnosticReceiver<'a>>(
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

fn adaptive_dictionary_reader<V: Array + 'static>(
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

fn primitive_array_reader<T: ArrowPrimitiveType>(
    array: &Arc<dyn Array>,
) -> Option<Dictionary<'static>> {
    Some(Dictionary::from_array::<UInt16Type, _>(
        array.as_primitive::<T>(),
    ))
}

fn adaptive_dictionary_writer<'a, T: Array + 'static, FTransform>(
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

fn primitive_array_writer<'a, T: ArrowPrimitiveType, FTransform>(
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

fn body_writer(
    keys: DictionaryKeyArray,
    values: DictionaryValueArray<'_>,
) -> Option<Arc<dyn Array>> {
    let mut builder = LogsBodyBuilder::new();

    for key_index in 0..keys.len() {
        match keys
            .get_value_index_for_key_index(key_index)
            .map(|value_index| values.get_value_at(value_index))
            .unwrap_or(ValueOrRef::Null)
        {
            ValueOrRef::Null => builder.append_null(),
            ValueOrRef::Boolean(b) => builder.append_bool(b),
            ValueOrRef::Double(d) => builder.append_double(d),
            ValueOrRef::Integer(i) => builder.append_int(i),
            ValueOrRef::String(s) => builder.append_str(s.get_value().as_bytes()),
            ValueOrRef::DateTime(d) => match Value::DateTime(&d).convert_to_integer() {
                Some(v) => builder.append_int(v),
                None => builder.append_null(),
            },
            ValueOrRef::TimeSpan(t) => match Value::TimeSpan(&t).convert_to_integer() {
                Some(v) => builder.append_int(v),
                None => builder.append_null(),
            },
            ValueOrRef::Regex(r) => builder.append_str(r.get_value().as_str().as_bytes()),
            ValueOrRef::Array(a) => match a {
                ArrayValueOrRef::Buffer(BufferArray::U8(values)) => {
                    builder.append_bytes(values.get_buffer().as_slice())
                }
                a => match crate::serialization::to_slice(ValueOrRef::Array(a)) {
                    Ok(v) => builder.append_slice(&v),
                    Err(_) => builder.append_null(),
                },
            },
            ValueOrRef::Map(m) => match crate::serialization::to_slice(ValueOrRef::Map(m)) {
                Ok(v) => builder.append_map(&v),
                Err(_) => builder.append_null(),
            },
        }
    }

    match builder.finish() {
        Some(Ok(v)) => Some(Arc::new(v)),
        _ => None,
    }
}

#[derive(Debug)]
pub struct OtapLogRecordBatch<'pipeline, 'record> {
    diagnostic_level: Option<ColumnarEngineDiagnosticLevel>,
    logs: Option<&'record RecordBatch>,
    attributes: Option<OtapAttributes<'pipeline, 'record>>,
    resource: Option<OtapResource<'pipeline, 'record>>,
    scope: Option<OtapScope<'pipeline, 'record>>,
    fields: OtapLogRecordFields<'pipeline>,
}

impl<'pipeline, 'record> OtapLogRecordBatch<'pipeline, 'record> {
    pub fn new(
        diagnostic_level: Option<ColumnarEngineDiagnosticLevel>,
        logs: &'record RecordBatch,
        attributes: Option<OtapAttributes<'pipeline, 'record>>,
        resource: Option<OtapResource<'pipeline, 'record>>,
        scope: Option<OtapScope<'pipeline, 'record>>,
        state: Option<OtapLogRecordState<'pipeline>>,
    ) -> OtapLogRecordBatch<'pipeline, 'record> {
        let fields = match state {
            Some(state) => state.fields,
            None => OtapLogRecordFields::new(),
        };

        Self {
            diagnostic_level,
            logs: Some(logs),
            attributes,
            resource,
            scope,
            fields,
        }
    }

    pub fn new_empty() -> OtapLogRecordBatch<'pipeline, 'record> {
        Self {
            diagnostic_level: None,
            logs: None,
            attributes: None,
            resource: None,
            scope: None,
            fields: OtapLogRecordFields::new(),
        }
    }
}

impl<'pipeline> ColumnarRecords<'pipeline> for OtapLogRecordBatch<'pipeline, '_> {
    fn get_diagnostic_level(&self) -> Option<ColumnarEngineDiagnosticLevel> {
        self.diagnostic_level
    }

    fn get_key_data_type(&self) -> DataType {
        DataType::UInt16
    }

    fn len(&self) -> usize {
        self.logs.map_or(0, |v| v.num_rows())
    }

    fn get_attached_records(&self, name: &str) -> Option<&dyn RecordTable<'pipeline>> {
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

impl<'pipeline> From<OtapLogRecordBatch<'pipeline, '_>> for OtapLogRecordState<'pipeline> {
    fn from(val: OtapLogRecordBatch<'pipeline, '_>) -> Self {
        OtapLogRecordState {
            decoded_attribute_ids: val.attributes.map(|v| v.into_parts()).unwrap_or_default(),
            decoded_scope_ids: val
                .scope
                .and_then(|v| v.attributes.map(|v| v.into_parts()))
                .unwrap_or_default(),
            decoded_resource_ids: val
                .resource
                .and_then(|v| v.attributes.map(|v| v.into_parts()))
                .unwrap_or_default(),
            fields: val.fields,
            attributes: OnceCell::new(),
        }
    }
}

impl<'pipeline> RecordTable<'pipeline> for OtapLogRecordBatch<'pipeline, '_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'pipeline, '_>> {
        let key = get_log_record_schema().normalize_key(key);

        if key == consts::ATTRIBUTES {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        if let Some(logs) = self.logs {
            let value = match key {
                consts::TIME_UNIX_NANO => self.fields.get(OtapLogRecordField::TimeUnixNano, logs),
                consts::OBSERVED_TIME_UNIX_NANO => self
                    .fields
                    .get(OtapLogRecordField::ObservedTimeUnixNano, logs),
                consts::SEVERITY_NUMBER => {
                    self.fields.get(OtapLogRecordField::SeverityNumber, logs)
                }
                consts::SEVERITY_TEXT => self.fields.get(OtapLogRecordField::SeverityText, logs),
                consts::BODY => self.fields.get(OtapLogRecordField::Body, logs),
                consts::TRACE_ID => self.fields.get(OtapLogRecordField::TraceId, logs),
                consts::SPAN_ID => self.fields.get(OtapLogRecordField::SpanId, logs),
                consts::FLAGS => self.fields.get(OtapLogRecordField::Flags, logs),
                consts::EVENT_NAME => self.fields.get(OtapLogRecordField::EventName, logs),
                _ => return None,
            };

            return match value {
                OtapValue::Read(v) | OtapValue::Set(v) => {
                    Some(RecordTableValue::Dictionary(v.clone()))
                }
                _ => None,
            };
        }

        None
    }
}

impl Display for OtapLogRecordBatch<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Logs(RecordCount={})", self.len())
    }
}

#[derive(Debug)]
pub struct OtapResource<'pipeline, 'record> {
    resource_struct: &'record StructArray,
    attributes: Option<OtapAttributes<'pipeline, 'record>>,
}

impl<'pipeline> RecordTable<'pipeline> for OtapResource<'pipeline, '_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'pipeline, '_>> {
        if key == consts::ATTRIBUTES || key == "Attributes" {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        None
    }
}

impl Display for OtapResource<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Resource(RecordCount={})", self.resource_struct.len())
    }
}

#[derive(Debug)]
pub struct OtapScope<'pipeline, 'record> {
    scope_struct: &'record StructArray,
    attributes: Option<OtapAttributes<'pipeline, 'record>>,
}

impl<'pipeline> RecordTable<'pipeline> for OtapScope<'pipeline, '_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'pipeline, '_>> {
        if key == consts::ATTRIBUTES || key == "Attributes" {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        let values = match key {
            consts::NAME | "Name"
                if let Some(name_column) = self.scope_struct.column_by_name(consts::NAME) =>
            {
                adaptive_dictionary_reader::<StringArray>(name_column)
            }
            consts::VERSION | "Version"
                if let Some(version_column) = self.scope_struct.column_by_name(consts::VERSION) =>
            {
                adaptive_dictionary_reader::<StringArray>(version_column)
            }
            _ => return None,
        };

        values.map(RecordTableValue::Dictionary)
    }
}

impl Display for OtapScope<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scope(RecordCount={})", self.scope_struct.len())
    }
}

#[derive(Debug)]
pub struct OtapIds<'record> {
    encoding: Option<&'record str>,
    encoded: &'record PrimitiveArray<UInt16Type>,
    decoded: OnceCell<PrimitiveArray<UInt16Type>>,
}

impl<'record> OtapIds<'record> {
    pub fn new(
        encoded_ids: &'record PrimitiveArray<UInt16Type>,
        encoding: Option<&'record str>,
    ) -> OtapIds<'record> {
        Self {
            encoding,
            encoded: encoded_ids,
            decoded: OnceCell::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.encoded.is_empty()
    }

    pub fn len(&self) -> usize {
        self.encoded.len()
    }

    pub fn get_ids(&self) -> &PrimitiveArray<UInt16Type> {
        self.decoded.get_or_init(|| self.init())
    }

    pub fn into_parts(mut self) -> Option<PrimitiveArray<UInt16Type>> {
        if self.encoding == Some(metadata::encodings::PLAIN) {
            None
        } else {
            Some(
                self.decoded
                    .take()
                    .unwrap_or_else(|| remove_delta_encoding_from_column(self.encoded)),
            )
        }
    }

    fn init(&self) -> PrimitiveArray<UInt16Type> {
        if self.encoding == Some(metadata::encodings::PLAIN) {
            self.encoded.clone()
        } else {
            remove_delta_encoding_from_column(self.encoded)
        }
    }
}

#[derive(Debug, Default)]
pub struct OtapDecodedIds {
    ids: Option<PrimitiveArray<UInt16Type>>,
    parent_ids: Option<PrimitiveArray<UInt16Type>>,
}

#[derive(Debug)]
pub struct OtapLogRecordState<'pipeline> {
    decoded_attribute_ids: OtapDecodedIds,
    decoded_scope_ids: OtapDecodedIds,
    decoded_resource_ids: OtapDecodedIds,
    fields: OtapLogRecordFields<'pipeline>,
    attributes: OnceCell<AHashMap<Box<str>, OtapValue<'pipeline>>>,
}

#[derive(VariantArray, EnumCount, Debug, Copy, Clone)]
pub enum OtapLogRecordField {
    TimeUnixNano,
    ObservedTimeUnixNano,
    SeverityNumber,
    SeverityText,
    TraceId,
    SpanId,
    Flags,
    EventName,
    Body,
}

impl OtapLogRecordField {
    pub fn supports_access_by_path(&self) -> bool {
        match self {
            OtapLogRecordField::TimeUnixNano => false,
            OtapLogRecordField::ObservedTimeUnixNano => false,
            OtapLogRecordField::SeverityNumber => false,
            OtapLogRecordField::SeverityText => false,
            OtapLogRecordField::TraceId => false,
            OtapLogRecordField::SpanId => false,
            OtapLogRecordField::Flags => false,
            OtapLogRecordField::EventName => false,
            OtapLogRecordField::Body => true,
        }
    }

    pub fn get_column_name_and_transform(
        &self,
    ) -> (
        &'static str,
        fn(DictionaryKeyArray, DictionaryValueArray) -> Option<Arc<dyn Array>>,
    ) {
        match self {
            OtapLogRecordField::TimeUnixNano => (consts::TIME_UNIX_NANO, |keys, values| {
                primitive_array_writer(
                    keys,
                    values,
                    DictionaryValueArray::transform_into_timestamp_nanoseconds_array,
                )
            }),
            OtapLogRecordField::ObservedTimeUnixNano => {
                (consts::OBSERVED_TIME_UNIX_NANO, |keys, values| {
                    primitive_array_writer(
                        keys,
                        values,
                        DictionaryValueArray::transform_into_timestamp_nanoseconds_array,
                    )
                })
            }
            OtapLogRecordField::SeverityNumber => (consts::SEVERITY_NUMBER, |keys, values| {
                adaptive_dictionary_writer(
                    keys,
                    values,
                    DictionaryValueArray::transform_into_int_array::<Int32Type>,
                )
            }),
            OtapLogRecordField::SeverityText => (consts::SEVERITY_TEXT, |keys, values| {
                adaptive_dictionary_writer(
                    keys,
                    values,
                    DictionaryValueArray::transform_into_string_array,
                )
            }),
            OtapLogRecordField::TraceId => (consts::TRACE_ID, |keys, values| {
                adaptive_dictionary_writer(
                    keys,
                    values,
                    DictionaryValueArray::transform_into_fixed_sized_binary_array::<16>,
                )
            }),
            OtapLogRecordField::SpanId => (consts::SPAN_ID, |keys, values| {
                adaptive_dictionary_writer(
                    keys,
                    values,
                    DictionaryValueArray::transform_into_fixed_sized_binary_array::<8>,
                )
            }),
            OtapLogRecordField::Flags => (consts::FLAGS, |keys, values| {
                primitive_array_writer(
                    keys,
                    values,
                    DictionaryValueArray::transform_into_int_array::<UInt32Type>,
                )
            }),
            OtapLogRecordField::EventName => (consts::EVENT_NAME, |keys, values| {
                adaptive_dictionary_writer(
                    keys,
                    values,
                    DictionaryValueArray::transform_into_string_array,
                )
            }),
            OtapLogRecordField::Body => (consts::BODY, body_writer),
        }
    }
}

pub enum OtapLogRecordModifiedField<'pipeline> {
    Removed,
    Set(Dictionary<'pipeline>),
}

#[derive(Debug)]
pub struct OtapLogRecordFields<'pipeline>(
    [OnceCell<OtapValue<'pipeline>>; OtapLogRecordField::COUNT],
);

impl<'pipeline> OtapLogRecordFields<'pipeline> {
    pub fn new() -> OtapLogRecordFields<'pipeline> {
        OtapLogRecordFields(std::array::from_fn(|_| OnceCell::new()))
    }

    pub fn get(&self, field: OtapLogRecordField, logs: &RecordBatch) -> &OtapValue<'pipeline> {
        self.0[field as usize].get_or_init(|| Self::init(field, logs))
    }

    pub fn get_mut(
        &mut self,
        field: OtapLogRecordField,
        logs: &RecordBatch,
    ) -> &mut OtapValue<'pipeline> {
        self.get(field, logs);
        self.0[field as usize].get_mut().unwrap()
    }

    pub fn set(&mut self, field: OtapLogRecordField, value: OtapValue<'pipeline>) {
        let v = OnceCell::new();
        v.set(value).expect("set");
        self.0[field as usize] = v;
    }

    pub fn take(&mut self, field: OtapLogRecordField, logs: &RecordBatch) -> OtapValue<'pipeline> {
        self.0[field as usize]
            .take()
            .unwrap_or_else(|| Self::init(field, logs))
    }

    pub fn take_if_modified(
        &mut self,
        field: OtapLogRecordField,
    ) -> Option<OtapLogRecordModifiedField<'pipeline>> {
        let field = &mut self.0[field as usize];
        match field.get() {
            Some(OtapValue::Removed) => {
                *field = OnceCell::new();
                Some(OtapLogRecordModifiedField::Removed)
            }
            Some(OtapValue::Set(v)) => {
                let r = v.clone();
                *field = OnceCell::new();
                Some(OtapLogRecordModifiedField::Set(r))
            }
            _ => None,
        }
    }

    fn init(field: OtapLogRecordField, logs: &RecordBatch) -> OtapValue<'pipeline> {
        match field {
            OtapLogRecordField::TimeUnixNano => {
                if let Some(time_unix_nano_column) =
                    logs.schema_ref().column_with_name(consts::TIME_UNIX_NANO)
                    && let Some(d) = primitive_array_reader::<TimestampNanosecondType>(
                        logs.column(time_unix_nano_column.0),
                    )
                {
                    return OtapValue::Read(d);
                }
            }
            OtapLogRecordField::ObservedTimeUnixNano => {
                if let Some(observed_time_unix_nano_column) = logs
                    .schema_ref()
                    .column_with_name(consts::OBSERVED_TIME_UNIX_NANO)
                    && let Some(d) = primitive_array_reader::<TimestampNanosecondType>(
                        logs.column(observed_time_unix_nano_column.0),
                    )
                {
                    return OtapValue::Read(d);
                }
            }
            OtapLogRecordField::SeverityNumber => {
                if let Some(severity_number_column) =
                    logs.schema_ref().column_with_name(consts::SEVERITY_NUMBER)
                    && let Some(d) = adaptive_dictionary_reader::<Int32Array>(
                        logs.column(severity_number_column.0),
                    )
                {
                    return OtapValue::Read(d);
                }
            }
            OtapLogRecordField::SeverityText => {
                if let Some(severity_text_column) =
                    logs.schema_ref().column_with_name(consts::SEVERITY_TEXT)
                    && let Some(d) = adaptive_dictionary_reader::<StringArray>(
                        logs.column(severity_text_column.0),
                    )
                {
                    return OtapValue::Read(d);
                }
            }
            OtapLogRecordField::TraceId => {
                if let Some(trace_id_column) = logs.schema_ref().column_with_name(consts::TRACE_ID)
                    && let Some(d) = adaptive_dictionary_reader::<FixedSizeBinaryArray>(
                        logs.column(trace_id_column.0),
                    )
                {
                    return OtapValue::Read(d);
                }
            }
            OtapLogRecordField::SpanId => {
                if let Some(span_id_column) = logs.schema_ref().column_with_name(consts::SPAN_ID)
                    && let Some(d) = adaptive_dictionary_reader::<FixedSizeBinaryArray>(
                        logs.column(span_id_column.0),
                    )
                {
                    return OtapValue::Read(d);
                }
            }
            OtapLogRecordField::Flags => {
                if let Some(flags_column) = logs.schema_ref().column_with_name(consts::FLAGS)
                    && let Some(d) =
                        primitive_array_reader::<UInt32Type>(logs.column(flags_column.0))
                {
                    return OtapValue::Read(d);
                }
            }
            OtapLogRecordField::EventName => {
                if let Some(event_name_column) =
                    logs.schema_ref().column_with_name(consts::EVENT_NAME)
                    && let Some(d) =
                        adaptive_dictionary_reader::<StringArray>(logs.column(event_name_column.0))
                {
                    return OtapValue::Read(d);
                }
            }
            OtapLogRecordField::Body => {
                if let Some(d) = build_logs_body_dictionary(logs, logs.schema_ref()) {
                    return OtapValue::Read(d);
                }
            }
        }

        OtapValue::NotFound
    }
}

impl<'pipeline> Default for OtapLogRecordFields<'pipeline> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub enum OtapValue<'pipeline> {
    NotFound,
    Removed,
    Read(Dictionary<'pipeline>),
    Set(Dictionary<'pipeline>),
}

#[derive(Debug)]
pub struct OtapAttributes<'pipeline, 'record> {
    ids: OtapIds<'record>,
    id_to_record_index_map: OnceCell<PrimitiveArray<UInt16Type>>,
    cache: RefCell<AHashMap<Box<str>, Dictionary<'pipeline>>>,
    attributes_batch: &'record RecordBatch,
    attribute_parent_ids: OnceCell<PrimitiveArray<UInt16Type>>,
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

        Self {
            ids,
            id_to_record_index_map: OnceCell::new(),
            cache: RefCell::new(AHashMap::new()),
            attributes_batch,
            attribute_parent_ids: OnceCell::new(),
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

    pub fn into_parts(mut self) -> OtapDecodedIds {
        let parent_id_column = self
            .attributes_batch
            .schema_ref()
            .column_with_name(consts::PARENT_ID)
            .expect("has parent ids");

        let parent_ids = if parent_id_column
            .1
            .metadata()
            .get(metadata::COLUMN_ENCODING)
            .map(|v| v.as_str())
            == Some(metadata::encodings::PLAIN)
        {
            None
        } else {
            Some(
                self.attribute_parent_ids
                    .take()
                    .unwrap_or_else(|| self.init_parent_ids(parent_id_column.0)),
            )
        };

        OtapDecodedIds {
            ids: self.ids.into_parts(),
            parent_ids,
        }
    }

    fn get_parent_ids(&self) -> &PrimitiveArray<UInt16Type> {
        self.attribute_parent_ids.get_or_init(|| {
            let parent_id_column = self
                .attributes_batch
                .schema_ref()
                .column_with_name(consts::PARENT_ID)
                .expect("has parent ids");

            if parent_id_column
                .1
                .metadata()
                .get(metadata::COLUMN_ENCODING)
                .map(|v| v.as_str())
                == Some(metadata::encodings::PLAIN)
            {
                self.attributes_batch
                    .column(parent_id_column.0)
                    .as_primitive::<UInt16Type>()
                    .clone()
            } else {
                self.init_parent_ids(parent_id_column.0)
            }
        })
    }

    fn init_parent_ids(&self, parent_id_column: usize) -> PrimitiveArray<UInt16Type> {
        materialize_parent_id_for_attributes::<u16>(self.attributes_batch)
            .expect("materialized batch")
            .column(parent_id_column)
            .as_primitive::<UInt16Type>()
            .clone()
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
            let attribute_parent_ids = self.get_parent_ids().values().as_ptr();
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

        cache.insert(key.into(), value);

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

fn build_logs_body_dictionary(
    logs: &RecordBatch,
    logs_schema: &Schema,
) -> Option<Dictionary<'static>> {
    if let Some(body_column) = logs_schema.column_with_name(consts::BODY) {
        let body_struct = logs.column(body_column.0).as_struct();

        build_logs_body_dictionary_from_struct(body_struct)
    } else {
        None
    }
}

fn build_logs_body_dictionary_from_struct(
    body_struct: &StructArray,
) -> Option<Dictionary<'static>> {
    if let Some(body_type) = body_struct.column_by_name(consts::ATTRIBUTE_TYPE) {
        let body_types = body_type.as_primitive::<UInt8Type>();

        let record_count = body_types.len();

        let mut key_builder = DictionaryKeyArrayBuilder::<UInt16Type>::new(record_count);
        let mut key_writer = key_builder.get_writer();

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
                        body_struct.column_by_name(consts::ATTRIBUTE_STR).map(|v| {
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
                                        unsafe { *offsets.get_unchecked(value_index + 1) } as usize;
                                    let start =
                                        unsafe { *offsets.get_unchecked(value_index) } as usize;
                                    strings.values().slice_with_length(start, end - start)
                                })));
                                vacant.insert(index as u16)
                            }
                        };
                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, *index) };
                        continue;
                    }
                }
                2 => {
                    if let Some(body_ints) = body_ints.get_or_init(|| {
                        body_struct.column_by_name(consts::ATTRIBUTE_INT).map(|v| {
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
                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, *index) };
                        continue;
                    }
                }
                3 => {
                    if let Some(body_doubles) = body_doubles.get_or_init(|| {
                        body_struct
                            .column_by_name(consts::ATTRIBUTE_DOUBLE)
                            .map(|v| v.as_primitive::<Float64Type>())
                    }) {
                        let index = values.len() as u16;
                        values.push(ValueOrRef::Double(body_doubles.value(key_index)));

                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, index) };
                        continue;
                    }
                }
                4 => {
                    if let Some(body_bools) = body_bools.get_or_init(|| {
                        body_struct
                            .column_by_name(consts::ATTRIBUTE_BOOL)
                            .map(|v| v.as_boolean())
                    }) {
                        let index = values.len() as u16;
                        values.push(ValueOrRef::Boolean(body_bools.value(key_index)));

                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, index) };
                        continue;
                    }
                }
                5 => {
                    if let Some(body_ser) = body_ser.get_or_init(|| {
                        body_struct.column_by_name(consts::ATTRIBUTE_SER).map(|v| {
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
                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, *index) };
                        continue;
                    }
                }
                6 => {
                    if let Some(body_ser) = body_ser.get_or_init(|| {
                        body_struct.column_by_name(consts::ATTRIBUTE_SER).map(|v| {
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
                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, *index) };
                        continue;
                    }
                }
                7 => {
                    if let Some(body_bytes) = body_bytes.get_or_init(|| {
                        body_struct
                            .column_by_name(consts::ATTRIBUTE_BYTES)
                            .map(|v| {
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
                                    let start = unsafe { *offsets.get_unchecked(index) } as usize;
                                    let end = unsafe { *offsets.get_unchecked(index + 1) } as usize;
                                    let buffer = bytes
                                        .values()
                                        .slice_with_length(start, end - start)
                                        .clone();
                                    BufferArray::new_u8(buffer)
                                })));
                                vacant.insert(index as u16)
                            }
                        };
                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, *index) };
                        continue;
                    }
                }
                d => todo!("Body type '{d}' is not supported"),
            }

            unsafe { key_writer.set_null_unchecked(key_index) };
        }

        return Some(Dictionary::new(
            key_builder.finish().into(),
            DictionaryValueArray::Vec(values.into()),
        ));
    }

    None
}
