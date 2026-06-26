// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::OnceCell,
    collections::hash_map::Entry,
    fmt::Display,
    ops::{Deref, DerefMut},
    rc::Rc,
    sync::Arc,
};

use ahash::AHashMap;
use arrow::{array::*, compute::kernels::filter, datatypes::*};
use data_engine_columnar::*;
use data_engine_expressions::*;
use otap_df_pdata::{
    encode::record::logs::LogsBodyBuilder,
    otap::raw_batch_store::POSITION_LOOKUP,
    proto::opentelemetry::arrow::v1::ArrowPayloadType,
    schema::{FieldExt, consts},
};
use roaring::RoaringBitmap;
use strum::*;

use crate::{arrow_helpers::*, filter::*, *};

static LOGS_BATCH_POSITION: usize = POSITION_LOOKUP[ArrowPayloadType::Logs as usize];
static LOG_ATTRIBUTES_BATCH_POSITION: usize = POSITION_LOOKUP[ArrowPayloadType::LogAttrs as usize];

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

            let attributes = batches[LOG_ATTRIBUTES_BATCH_POSITION]
                .as_ref()
                .map(|v| OtapAttributes::new(OtapIds::from_batch(logs), v));

            let resource = if let Some(resource_column) =
                logs_schema.column_with_name(consts::RESOURCE)
                && let Some(resource_struct) = logs.column(resource_column.0).as_struct_opt()
            {
                if let Some(resource_attributes_batch) =
                    batches[RESOURCE_ATTRIBUTES_BATCH_POSITION].as_ref()
                {
                    Some(OtapResource {
                        resource_struct,
                        attributes: Some(OtapAttributes::new(
                            OtapIds::from_struct(resource_struct),
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
                if let Some(scope_attributes_batch) =
                    batches[SCOPE_ATTRIBUTES_BATCH_POSITION].as_ref()
                {
                    Some(OtapScope {
                        scope_struct,
                        attributes: Some(OtapAttributes::new(
                            OtapIds::from_struct(scope_struct),
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

            let mut attributes_state = state.attributes.as_mut();

            let mut decoded_ids = [
                (consts::ID, None),
                (consts::SCOPE, None),
                (consts::RESOURCE, None),
            ];
            let mut decode = false;
            if let Some(ids) = attributes_state
                .as_mut()
                .and_then(|a| a.decoded_ids.as_mut())
                .and_then(|a| a.ids.take())
            {
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
                            attributes_state
                                .and_then(|a| a.decoded_ids.as_mut())
                                .and_then(|a| a.parent_ids.take()),
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

    fn apply<'pipeline, T: ColumnarEngineDiagnosticReceiver<'pipeline>>(
        &self,
        diagnostic_receiver: &T,
        expression: &'pipeline dyn Expression,
        state: &mut OtapLogRecordState<'pipeline>,
        batches: &mut [Option<RecordBatch>; 4],
    ) {
        let mut logs = batches[LOGS_BATCH_POSITION].take().expect("has logs");

        let record_count = logs.num_rows();

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

        if let Some(attributes) = state.attributes.take_if(|a| a.modified) {
            let attributes_batch = batches[LOG_ATTRIBUTES_BATCH_POSITION].as_ref().map(|b| {
                match attributes.decoded_ids {
                    None => OtapAttributesBatch::from_parts(
                        OtapIds::from_batch(&logs),
                        None,
                        attributes.id_to_record_index_map,
                        b,
                    ),
                    Some(decoded_ids) => {
                        let ids = if let Some(decoded_ids) = decoded_ids.ids {
                            OtapIds::from_decoded(decoded_ids)
                        } else {
                            OtapIds::from_batch(&logs)
                        };

                        OtapAttributesBatch::from_parts(
                            ids,
                            decoded_ids.parent_ids,
                            attributes.id_to_record_index_map,
                            b,
                        )
                    }
                }
            });

            if let Some((ids, attributes)) =
                attributes_writer(record_count, attributes.values, attributes_batch)
            {
                batches[LOG_ATTRIBUTES_BATCH_POSITION] = Some(attributes);

                let (schema, mut columns, _) = logs.into_parts();

                let mut schema_builder: SchemaBuilder = schema.as_ref().into();

                match schema.column_with_name(consts::ID) {
                    None => {
                        schema_builder.push(
                            Field::new(consts::ID, ids.data_type().clone(), false)
                                .with_plain_encoding(),
                        );
                        columns.push(Arc::new(ids));
                    }
                    Some((column_id, field)) => {
                        let field = field.clone().with_plain_encoding();

                        *schema_builder.field_mut(column_id) = Arc::new(field);

                        columns[column_id] = Arc::new(ids);
                    }
                }

                logs = RecordBatch::try_new(Arc::new(schema_builder.finish()), columns)
                    .expect("valid logs");
            } else {
                logs = remove_column(diagnostic_receiver, expression, logs, consts::ID);
                batches[LOG_ATTRIBUTES_BATCH_POSITION] = None;
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
            OtapValue::Read(v) | OtapValue::Set(v) => update_dictionary_values_for_path(
                diagnostic_receiver,
                v,
                key_filter,
                &path[0],
                &path[1..],
                value,
            ),
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

    if value.is_null() {
        diagnostic_receiver.add_diagnostic_if_enabled(
            ColumnarEngineDiagnosticLevel::Verbose,
            expression,
            || format!("Field '{field:?}' removed"),
        );

        fields.set(field, OtapValue::Removed);
    } else {
        diagnostic_receiver.add_diagnostic_if_enabled(
            ColumnarEngineDiagnosticLevel::Verbose,
            expression,
            || format!("Field '{field:?}' set to: {value}"),
        );

        fields.set(field, OtapValue::Set(value));
    }

    ColumnarRecordsWriteResult::Success
}

fn process_attributes_update<'a, T: ColumnarEngineDiagnosticReceiver<'a>>(
    diagnostic_receiver: &T,
    expression: &'a dyn Expression,
    path: &[ColumnarEngineSelectionPath<'a>],
    key_filter: Option<&RoaringBitmap>,
    values: &Dictionary<'a>,
    attributes: &mut OtapAttributes<'a, '_>,
) -> ColumnarRecordsWriteResult {
    if path.is_empty() {
        // support replace all attributes with a map
        todo!()
    }

    match unsafe { path.get_unchecked(0) } {
        ColumnarEngineSelectionPath::Key {
            expression: _,
            value: attribute_key,
        } => {
            let (mut attributes_modified, mut attributes_values_borrow) =
                attributes.get_values(attribute_key.get_value());

            *attributes_modified = true;

            let mut attributes_values =
                match std::mem::replace(attributes_values_borrow.deref_mut(), OtapValue::Removed) {
                    OtapValue::NotFound | OtapValue::Removed => {
                        Dictionary::new_null::<UInt16Type>(values.len())
                    }
                    OtapValue::Read(v) | OtapValue::Set(v) => v,
                };

            let attributes_values = if path.len() > 2 {
                update_dictionary_values_for_path(
                    diagnostic_receiver,
                    attributes_values,
                    key_filter,
                    &path[1],
                    &path[2..],
                    values,
                )
            } else if key_filter.is_none() && values.is_null() {
                attributes_values = Dictionary::new_null::<UInt16Type>(values.len());
                attributes_values
            } else {
                attributes_values.with_values(key_filter, values)
            };

            if attributes_values.is_null() {
                diagnostic_receiver.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Verbose,
                    expression,
                    || format!("Attribute '{}' removed", attribute_key.get_value()),
                );
                *attributes_values_borrow.deref_mut() = OtapValue::Removed;
            } else {
                diagnostic_receiver.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Verbose,
                    expression,
                    || {
                        format!(
                            "Attribute '{}' set to: {attributes_values}",
                            attribute_key.get_value()
                        )
                    },
                );

                *attributes_values_borrow.deref_mut() = OtapValue::Set(attributes_values);
            }
        }
        ColumnarEngineSelectionPath::Index {
            expression,
            value: _,
        } => {
            diagnostic_receiver.add_diagnostic_if_enabled(
                ColumnarEngineDiagnosticLevel::Warn,
                *expression,
                || "Attributes cannot be accessed by array index".into(),
            );
            return ColumnarRecordsWriteResult::NotFound;
        }
        ColumnarEngineSelectionPath::Dictionary {
            expression: keys_expression,
            value: root_keys,
        } => {
            if path.is_empty() {
                // support replace all attributes with a map
                todo!()
            }

            let key_length = root_keys.len();

            let mut plan: AHashMap<StringValueOrRef, RoaringBitmap> =
                AHashMap::with_capacity(key_length);

            match key_filter {
                Some(v) => build_plan(
                    diagnostic_receiver,
                    *keys_expression,
                    root_keys,
                    &mut plan,
                    v.iter().map(|v| v as usize),
                ),
                None => build_plan(
                    diagnostic_receiver,
                    *keys_expression,
                    root_keys,
                    &mut plan,
                    0..key_length,
                ),
            }

            for (attribute_key, key_filter) in plan.into_iter() {
                let (mut attributes_modified, mut attributes_values_borrow) =
                    attributes.get_values(attribute_key.get_value());

                *attributes_modified = true;

                let attributes_values = match std::mem::replace(
                    attributes_values_borrow.deref_mut(),
                    OtapValue::Removed,
                ) {
                    OtapValue::NotFound | OtapValue::Removed => {
                        Dictionary::new_null::<UInt16Type>(values.len())
                    }
                    OtapValue::Read(v) | OtapValue::Set(v) => v,
                };

                let attributes_values = if path.len() > 2 {
                    update_dictionary_values_for_path(
                        diagnostic_receiver,
                        attributes_values,
                        Some(&key_filter),
                        &path[1],
                        &path[2..],
                        values,
                    )
                } else {
                    attributes_values.with_values(Some(&key_filter), values)
                };

                if attributes_values.is_null() {
                    diagnostic_receiver.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Verbose,
                        expression,
                        || format!("Attribute '{}' removed", attribute_key.get_value()),
                    );
                    *attributes_values_borrow.deref_mut() = OtapValue::Removed;
                } else {
                    diagnostic_receiver.add_diagnostic_if_enabled(
                        ColumnarEngineDiagnosticLevel::Verbose,
                        expression,
                        || {
                            format!(
                                "Attribute '{}' set to: {attributes_values}",
                                attribute_key.get_value()
                            )
                        },
                    );

                    *attributes_values_borrow.deref_mut() = OtapValue::Set(attributes_values);
                }
            }
        }
    }

    ColumnarRecordsWriteResult::Success
}

fn build_plan<
    'pipeline,
    TDiagnostic: ColumnarEngineDiagnosticReceiver<'pipeline>,
    TIter: Iterator<Item = usize>,
>(
    diagnostic_receiver: &TDiagnostic,
    expression: &'pipeline dyn Expression,
    root_keys: &Dictionary<'pipeline>,
    plan: &mut AHashMap<StringValueOrRef<'pipeline>, RoaringBitmap>,
    key_iter: TIter,
) {
    let mut log_error = false;

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
            _ => log_error = true,
        }
    }

    if log_error {
        diagnostic_receiver.add_diagnostic_if_enabled(
            ColumnarEngineDiagnosticLevel::Warn,
            expression,
            || "Log record can only be accessed by string keys".into(),
        );
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

                let field = field.as_ref().clone().with_plain_encoding();

                let mut struct_fields = struct_fields.to_vec();

                struct_fields[struct_column_id] = Arc::new(field);

                struct_columns[struct_column_id] = Arc::new(decoded_ids);

                columns[column_id] = Arc::new(StructArray::new(
                    struct_fields.into(),
                    struct_columns,
                    struct_nulls,
                ));
            } else {
                let field = field.clone().with_plain_encoding();

                *schema_builder.field_mut(column_id) = Arc::new(field);

                columns[column_id] = Arc::new(decoded_ids);
            }
        }
    }

    RecordBatch::try_new(Arc::new(schema_builder.finish()), columns).expect("valid batch")
}

fn update_dictionary_values_for_path<'a, TDiagnostic: ColumnarEngineDiagnosticReceiver<'a>>(
    diagnostic_receiver: &TDiagnostic,
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

            let (expression, path_value) = match current_path {
                ColumnarEngineSelectionPath::Key { expression, value } => {
                    (expression, ValueOrRef::String(value.clone()))
                }
                ColumnarEngineSelectionPath::Index { expression, value } => {
                    (expression, ValueOrRef::Integer(*value))
                }
                ColumnarEngineSelectionPath::Dictionary { expression, value } => {
                    (expression, value.get_value(key_index))
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
                                        diagnostic_receiver,
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
                                    diagnostic_receiver.add_diagnostic_if_enabled(
                                        ColumnarEngineDiagnosticLevel::Warn,
                                        *expression,
                                        || format!("Could not search for map key '{}' specified in accessor expression because current node is a '{}' value", key.get_value(), source_value.get_value_type()));
                                    None
                                }
                            }
                            ValueOrRef::Integer(index) => {
                                if let ValueOrRef::Array(ArrayValueOrRef::Owned(array)) =
                                    source_value
                                {
                                    let mut array = array.deref().clone();
                                    update_array_value_for_path(
                                        diagnostic_receiver,
                                        *expression,
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
                                    diagnostic_receiver.add_diagnostic_if_enabled(
                                        ColumnarEngineDiagnosticLevel::Warn,
                                        *expression,
                                        || format!("Could not search for array index '{index}' specified in accessor expression because current node is a '{}' value", source_value.get_value_type()));
                                    None
                                }
                            }
                            v => {
                                diagnostic_receiver.add_diagnostic_if_enabled(
                                    ColumnarEngineDiagnosticLevel::Warn,
                                    *expression,
                                    || format!("Unexpected scalar expression with '{}' value type encountered in accessor expression", v.get_value_type()),);
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

fn update_map_value_for_path<'a, TDiagnostic: ColumnarEngineDiagnosticReceiver<'a>>(
    diagnostic_receiver: &TDiagnostic,
    key_index: usize,
    map: &mut AHashMap<Box<str>, ValueOrRef<'a>>,
    current_key: &str,
    remaining_path: &[ColumnarEngineSelectionPath<'a>],
    value: ValueOrRef<'a>,
) {
    if let Some(current_path) = remaining_path.first() {
        let (expression, path_value) = match current_path {
            ColumnarEngineSelectionPath::Key { expression, value } => {
                (expression, ValueOrRef::String(value.clone()))
            }
            ColumnarEngineSelectionPath::Index { expression, value } => {
                (expression, ValueOrRef::Integer(*value))
            }
            ColumnarEngineSelectionPath::Dictionary { expression, value } => {
                (expression, value.get_value(key_index))
            }
        };

        if let Entry::Occupied(mut o) = map.entry(current_key.into()) {
            let value_for_key = o.insert(ValueOrRef::Null);

            o.insert(update_any_value_for_path(
                diagnostic_receiver,
                *expression,
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

fn update_array_value_for_path<'a, TDiagnostic: ColumnarEngineDiagnosticReceiver<'a>>(
    diagnostic_receiver: &TDiagnostic,
    expression: &'a dyn Expression,
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
        diagnostic_receiver.add_diagnostic_if_enabled(
            ColumnarEngineDiagnosticLevel::Warn,
            expression,
            || format!("Array index '{current_index}' specified in accessor expression is invalid"),
        );
        return;
    }

    if let Some(current_path) = remaining_path.first() {
        let (expression, path_value) = match current_path {
            ColumnarEngineSelectionPath::Key { expression, value } => {
                (expression, ValueOrRef::String(value.clone()))
            }
            ColumnarEngineSelectionPath::Index { expression, value } => {
                (expression, ValueOrRef::Integer(*value))
            }
            ColumnarEngineSelectionPath::Dictionary { expression, value } => {
                (expression, value.get_value(key_index))
            }
        };

        let value_for_index =
            std::mem::replace(&mut array[current_index as usize], ValueOrRef::Null);

        array[current_index as usize] = update_any_value_for_path(
            diagnostic_receiver,
            *expression,
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

fn update_any_value_for_path<'a, TDiagnostic: ColumnarEngineDiagnosticReceiver<'a>>(
    diagnostic_receiver: &TDiagnostic,
    expression: &'a dyn Expression,
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
                    diagnostic_receiver,
                    key_index,
                    inner_map.get_values_mut(),
                    path_key.get_value(),
                    &remaining_path[1..],
                    value,
                );
                ValueOrRef::Map(MapValueOrRef::Owned(inner_map.into()))
            } else {
                diagnostic_receiver.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    expression,
                    || format!("Could not search for map key '{}' specified in accessor expression because current node is a '{}' value", path_key.get_value(), any_value.get_value_type()));
                any_value
            }
        }
        ValueOrRef::Integer(index) => {
            if let ValueOrRef::Array(ArrayValueOrRef::Owned(inner_array)) = any_value {
                let mut inner_array = Rc::unwrap_or_clone(inner_array);
                update_array_value_for_path(
                    diagnostic_receiver,
                    expression,
                    key_index,
                    inner_array.get_values_mut(),
                    index,
                    &remaining_path[1..],
                    value,
                );
                ValueOrRef::Array(ArrayValueOrRef::Owned(inner_array.into()))
            } else {
                diagnostic_receiver.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    expression,
                    || format!("Could not search for array index '{index}' specified in accessor expression because current node is a '{}' value", any_value.get_value_type()));
                any_value
            }
        }
        v => {
            diagnostic_receiver.add_diagnostic_if_enabled(
                ColumnarEngineDiagnosticLevel::Warn,
                expression,
                || format!("Unexpected scalar expression with '{}' value type encountered in accessor expression", v.get_value_type()),);
            any_value
        }
    }
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

    fn set_values<T: ColumnarEngineDiagnosticReceiver<'pipeline>>(
        &mut self,
        diagnostic_receiver: &T,
        expression: &'pipeline dyn Expression,
        root: &ColumnarEngineSelectionPath<'pipeline>,
        path: &[ColumnarEngineSelectionPath<'pipeline>],
        key_filter: Option<&RoaringBitmap>,
        values: Dictionary<'pipeline>,
    ) -> ColumnarRecordsWriteResult {
        let logs = self.logs.expect("has logs");

        if key_filter.is_some_and(|v| v.is_empty()) {
            return ColumnarRecordsWriteResult::Success;
        }

        match root {
            ColumnarEngineSelectionPath::Key {
                expression: key_expression,
                value: root_key,
            } => {
                let field = match get_log_record_schema().normalize_key(root_key.get_value()) {
                    consts::ATTRIBUTES => {
                        if path.is_empty() {
                            // support replace all attributes with a map
                            todo!()
                        }

                        let attributes = self
                            .attributes
                            .get_or_insert_with(OtapAttributes::new_empty);

                        return process_attributes_update(
                            diagnostic_receiver,
                            expression,
                            path,
                            key_filter,
                            &values,
                            attributes,
                        );
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
                    &mut self.fields,
                    logs,
                    *key_expression,
                    path,
                    key_filter,
                    &values,
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
                    Some(v) => build_plan(
                        diagnostic_receiver,
                        *keys_expression,
                        root_keys,
                        &mut plan,
                        v.iter().map(|v| v as usize),
                    ),
                    None => build_plan(
                        diagnostic_receiver,
                        *keys_expression,
                        root_keys,
                        &mut plan,
                        0..key_length,
                    ),
                }

                let mut written_data_count = 0;
                let plan_count = plan.len();

                for (key, key_filter) in plan.into_iter() {
                    let field = match get_log_record_schema().normalize_key(key.get_value()) {
                        consts::ATTRIBUTES => {
                            if path.is_empty() {
                                // support replace all attributes with a map
                                todo!()
                            }

                            let attributes = self
                                .attributes
                                .get_or_insert_with(OtapAttributes::new_empty);

                            match process_attributes_update(
                                diagnostic_receiver,
                                expression,
                                path,
                                Some(&key_filter),
                                &values,
                                attributes,
                            ) {
                                ColumnarRecordsWriteResult::Success
                                | ColumnarRecordsWriteResult::PartialSuccess => {
                                    written_data_count += 1;
                                }
                                ColumnarRecordsWriteResult::NotFound => {}
                            }

                            continue;
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
                                *keys_expression,
                                || format!("Field '{f}' does not exist on log record"),
                            );
                            continue;
                        }
                    };

                    if process_log_record_field_update(
                        diagnostic_receiver,
                        expression,
                        field,
                        &mut self.fields,
                        logs,
                        *keys_expression,
                        path,
                        Some(&key_filter),
                        &values,
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
}

impl<'pipeline> From<OtapLogRecordBatch<'pipeline, '_>> for OtapLogRecordState<'pipeline> {
    fn from(val: OtapLogRecordBatch<'pipeline, '_>) -> Self {
        OtapLogRecordState {
            fields: val.fields,
            attributes: val.attributes.map(|v| v.into_parts()),
            decoded_scope_ids: val
                .scope
                .and_then(|v| {
                    v.attributes
                        .map(|v| v.into_parts().decoded_ids.expect("has ids"))
                })
                .unwrap_or_default(),
            decoded_resource_ids: val
                .resource
                .and_then(|v| {
                    v.attributes
                        .map(|v| v.into_parts().decoded_ids.expect("has ids"))
                })
                .unwrap_or_default(),
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
pub struct OtapLogRecordState<'pipeline> {
    fields: OtapLogRecordFields<'pipeline>,
    attributes: Option<OtapAttributesState<'pipeline>>,
    decoded_scope_ids: OtapDecodedIds,
    decoded_resource_ids: OtapDecodedIds,
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
                EMPTY_ATTRIBUTE_VALUE_TYPE => {}
                STRING_ATTRIBUTE_VALUE_TYPE => {
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
                INT_ATTRIBUTE_VALUE_TYPE => {
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
                DOUBLE_ATTRIBUTE_VALUE_TYPE => {
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
                BOOL_ATTRIBUTE_VALUE_TYPE => {
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
                MAP_ATTRIBUTE_VALUE_TYPE => {
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
                SLICE_ATTRIBUTE_VALUE_TYPE => {
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
                BYTES_ATTRIBUTE_VALUE_TYPE => {
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
                d => panic!("Body type '{d}' is not supported"),
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
