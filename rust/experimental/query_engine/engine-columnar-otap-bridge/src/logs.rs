// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    cell::{OnceCell, RefCell},
    collections::hash_map::Entry,
    fmt::Display,
    hash::Hash,
    ops::{Deref, DerefMut},
    rc::Rc,
    sync::Arc,
};

use ahash::{AHashMap, AHashSet};
use arrow::{
    array::*,
    buffer::{MutableBuffer, NullBuffer},
    compute::kernels::filter,
    datatypes::*,
};
use data_engine_columnar::*;
use data_engine_expressions::{Expression, RegexValue, StringValue, Value};
use otap_df_pdata::{
    encode::record::logs::LogsBodyBuilder, otap::raw_batch_store::POSITION_LOOKUP,
    proto::opentelemetry::arrow::v1::ArrowPayloadType, schema::consts,
};
use roaring::RoaringBitmap;

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

    fn create<'a>(&self, batches: &'a [Option<RecordBatch>; 4]) -> OtapLogRecordBatch<'a> {
        if let Some(logs) = batches[POSITION_LOOKUP[ArrowPayloadType::Logs as usize]].as_ref() {
            let logs_schema = logs.schema_ref();

            let attributes = if let Some(id_column) = logs_schema.column_with_name("id")
                && let Some(attributes_batch) =
                    batches[POSITION_LOOKUP[ArrowPayloadType::LogAttrs as usize]].as_ref()
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
                    && let Some(resource_attributes_batch) =
                        batches[POSITION_LOOKUP[ArrowPayloadType::ResourceAttrs as usize]].as_ref()
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
                    && let Some(scope_attributes_batch) =
                        batches[POSITION_LOOKUP[ArrowPayloadType::ScopeAttrs as usize]].as_ref()
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
        batches: &[Option<RecordBatch>; 4],
        filter: &BooleanArray,
    ) -> [Option<RecordBatch>; 4] {
        let filter_true_count = filter.true_count();

        if let Some(logs) = batches[POSITION_LOOKUP[ArrowPayloadType::Logs as usize]].as_ref()
            && filter_true_count > 0
        {
            let number_of_logs_before_filter = logs.num_rows();
            if filter_true_count == number_of_logs_before_filter {
                return [
                    batches[POSITION_LOOKUP[ArrowPayloadType::ResourceAttrs as usize]].clone(),
                    batches[POSITION_LOOKUP[ArrowPayloadType::ScopeAttrs as usize]].clone(),
                    Some(logs.clone()),
                    batches[POSITION_LOOKUP[ArrowPayloadType::LogAttrs as usize]].clone(),
                ];
            }

            let filtered_logs_batch = filter::filter_record_batch(logs, filter).unwrap();

            let number_of_logs_after_filter = filtered_logs_batch.num_rows();
            if number_of_logs_after_filter > 0 {
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
                    batches[POSITION_LOOKUP[ArrowPayloadType::LogAttrs as usize]]
                        .as_ref()
                        .and_then(|v| filter_child_batch(&ids, v))
                };

                let resource_attributes_batch = if let Some(resource_attributes) =
                    batches[POSITION_LOOKUP[ArrowPayloadType::ResourceAttrs as usize]].as_ref()
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
                        filter_child_batch(&ids, resource_attributes)
                    }
                } else {
                    None
                };

                let scope_attributes_batch = if let Some(scope_attributes) =
                    batches[POSITION_LOOKUP[ArrowPayloadType::ScopeAttrs as usize]].as_ref()
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
                        filter_child_batch(&ids, scope_attributes)
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

    fn set<'a, T: ColumnarEngineDiagnosticReceiver<'a>>(
        &self,
        diagnostic_receiver: &T,
        mut state: OtapBody,
        batches: &mut [Option<RecordBatch>; 4],
        root: &ColumnarEngineSelectionPath<'a>,
        path: &[ColumnarEngineSelectionPath<'a>],
        value: Dictionary,
    ) -> ColumnarRecordsWriteResult {
        let path_length = path.len();

        match root {
            ColumnarEngineSelectionPath::Key {
                expression,
                value: root_key,
            } => {
                match get_log_record_schema().normalize_key(root_key.get_value()) {
                    consts::ATTRIBUTES => {
                        todo!()
                    }
                    consts::BODY => {
                        let value = if path_length > 0 {
                            let body = state.take().unwrap_or_else(|| {
                                if let Some(logs_batch) =
                                    &batches[POSITION_LOOKUP[ArrowPayloadType::Logs as usize]]
                                {
                                    return build_logs_body_dictionary(
                                        logs_batch,
                                        logs_batch.schema_ref(),
                                    );
                                }

                                None
                            });

                            match body {
                                None => {
                                    diagnostic_receiver.add_diagnostic_if_enabled(
                                        ColumnarEngineDiagnosticLevel::Warn,
                                        *expression,
                                        || "Cannot access into empty Body".into(),
                                    );
                                    return ColumnarRecordsWriteResult::NotFound;
                                }
                                Some(body) => update_dictionary_values_for_path(
                                    body,
                                    None,
                                    &path[0],
                                    &path[1..],
                                    value,
                                ),
                            }
                        } else {
                            value
                        };

                        set_column(
                            diagnostic_receiver,
                            *expression,
                            batches,
                            POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                            consts::BODY,
                            value,
                            body_writer,
                        );

                        ColumnarRecordsWriteResult::Success
                    }
                    consts::TIME_UNIX_NANO => {
                        if path_length > 0 {
                            return log_invalid_column_access(
                                diagnostic_receiver,
                                *expression,
                                consts::TIME_UNIX_NANO,
                            );
                        }

                        set_column(
                            diagnostic_receiver,
                            *expression,
                            batches,
                            POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                            consts::TIME_UNIX_NANO,
                            value,
                            |keys, values| {
                                primitive_array_writer(keys, values, DictionaryValueArray::transform_into_timestamp_nanoseconds_array)
                            },
                        );

                        ColumnarRecordsWriteResult::Success
                    }
                    consts::OBSERVED_TIME_UNIX_NANO => {
                        if path_length > 0 {
                            return log_invalid_column_access(
                                diagnostic_receiver,
                                *expression,
                                consts::OBSERVED_TIME_UNIX_NANO,
                            );
                        }

                        set_column(
                            diagnostic_receiver,
                            *expression,
                            batches,
                            POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                            consts::OBSERVED_TIME_UNIX_NANO,
                            value,
                            |keys, values| {
                                primitive_array_writer(keys, values, DictionaryValueArray::transform_into_timestamp_nanoseconds_array)
                            },
                        );

                        ColumnarRecordsWriteResult::Success
                    }
                    consts::SEVERITY_NUMBER => {
                        if path_length > 0 {
                            return log_invalid_column_access(
                                diagnostic_receiver,
                                *expression,
                                consts::SEVERITY_NUMBER,
                            );
                        }

                        set_column(
                            diagnostic_receiver,
                            *expression,
                            batches,
                            POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                            consts::SEVERITY_NUMBER,
                            value,
                            |keys, values| {
                                adaptive_dictionary_writer(
                                    keys,
                                    values,
                                    DictionaryValueArray::transform_into_int_array::<Int32Type>,
                                )
                            },
                        );

                        ColumnarRecordsWriteResult::Success
                    }
                    consts::SEVERITY_TEXT => {
                        if path_length > 0 {
                            return log_invalid_column_access(
                                diagnostic_receiver,
                                *expression,
                                consts::SEVERITY_TEXT,
                            );
                        }

                        set_column(
                            diagnostic_receiver,
                            *expression,
                            batches,
                            POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                            consts::SEVERITY_TEXT,
                            value,
                            |keys, values| {
                                adaptive_dictionary_writer(
                                    keys,
                                    values,
                                    DictionaryValueArray::transform_into_string_array,
                                )
                            },
                        );

                        ColumnarRecordsWriteResult::Success
                    }
                    consts::TRACE_ID => {
                        if path_length > 0 {
                            return log_invalid_column_access(
                                diagnostic_receiver,
                                *expression,
                                consts::TRACE_ID,
                            );
                        }

                        set_column(
                            diagnostic_receiver,
                            *expression,
                            batches,
                            POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                            consts::TRACE_ID,
                            value,
                            |keys, values| {
                                adaptive_dictionary_writer(
                                    keys,
                                    values,
                                    DictionaryValueArray::transform_into_fixed_sized_binary_array::<
                                        16,
                                    >,
                                )
                            },
                        );

                        ColumnarRecordsWriteResult::Success
                    }
                    consts::SPAN_ID => {
                        if path_length > 0 {
                            return log_invalid_column_access(
                                diagnostic_receiver,
                                *expression,
                                consts::SPAN_ID,
                            );
                        }

                        set_column(
                            diagnostic_receiver,
                            *expression,
                            batches,
                            POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                            consts::SPAN_ID,
                            value,
                            |keys, values| {
                                adaptive_dictionary_writer(
                                    keys,
                                    values,
                                    DictionaryValueArray::transform_into_fixed_sized_binary_array::<
                                        8,
                                    >,
                                )
                            },
                        );

                        ColumnarRecordsWriteResult::Success
                    }
                    consts::FLAGS => {
                        if path_length > 0 {
                            return log_invalid_column_access(
                                diagnostic_receiver,
                                *expression,
                                consts::FLAGS,
                            );
                        }

                        set_column(
                            diagnostic_receiver,
                            *expression,
                            batches,
                            POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                            consts::FLAGS,
                            value,
                            |keys, values| {
                                primitive_array_writer(
                                    keys,
                                    values,
                                    DictionaryValueArray::transform_into_int_array::<UInt32Type>,
                                )
                            },
                        );

                        ColumnarRecordsWriteResult::Success
                    }
                    consts::EVENT_NAME => {
                        if path_length > 0 {
                            return log_invalid_column_access(
                                diagnostic_receiver,
                                *expression,
                                consts::EVENT_NAME,
                            );
                        }

                        set_column(
                            diagnostic_receiver,
                            *expression,
                            batches,
                            POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                            consts::EVENT_NAME,
                            value,
                            |keys, values| {
                                adaptive_dictionary_writer(
                                    keys,
                                    values,
                                    DictionaryValueArray::transform_into_string_array,
                                )
                            },
                        );

                        ColumnarRecordsWriteResult::Success
                    }
                    f => {
                        diagnostic_receiver.add_diagnostic_if_enabled(
                            ColumnarEngineDiagnosticLevel::Warn,
                            *expression,
                            || format!("Field '{f}' does not exist on log record"),
                        );
                        ColumnarRecordsWriteResult::NotFound
                    }
                }
            }
            ColumnarEngineSelectionPath::Dictionary {
                expression,
                value: root_keys,
            } => {
                let key_length = root_keys.len();

                let mut plan: AHashMap<StringValueOrRef, RoaringBitmap> =
                    AHashMap::with_capacity(key_length);

                for key_index in 0..key_length {
                    if let ValueOrRef::String(key) = root_keys.get_value(key_index) {
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
                }

                let mut written_data_count = 0;
                let plan_count = plan.len();

                for (key, key_filter) in plan.into_iter() {
                    match get_log_record_schema().normalize_key(key.get_value()) {
                        consts::BODY => {
                            let body = state.take().unwrap_or_else(|| {
                                if let Some(logs_batch) =
                                    &batches[POSITION_LOOKUP[ArrowPayloadType::Logs as usize]]
                                {
                                    return build_logs_body_dictionary(
                                        logs_batch,
                                        logs_batch.schema_ref(),
                                    );
                                }

                                None
                            });

                            if path_length > 0 {
                                match body {
                                    None => {
                                        diagnostic_receiver.add_diagnostic_if_enabled(
                                            ColumnarEngineDiagnosticLevel::Warn,
                                            *expression,
                                            || "Cannot access into empty Body".into(),
                                        );
                                        continue;
                                    }
                                    Some(body) => {
                                        // todo: nested paths should be supported on body
                                        todo!()
                                    }
                                }
                            }

                            set_column_with_values(
                                diagnostic_receiver,
                                *expression,
                                batches,
                                POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                                consts::BODY,
                                |_| body,
                                key_filter,
                                &value,
                                body_writer,
                            );

                            written_data_count += 1;
                        }
                        consts::TIME_UNIX_NANO => {
                            if path_length > 0 {
                                log_invalid_column_access(
                                    diagnostic_receiver,
                                    *expression,
                                    consts::TIME_UNIX_NANO,
                                );
                                continue;
                            }

                            set_column_with_values(
                                diagnostic_receiver,
                                *expression,
                                batches,
                                POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                                consts::TIME_UNIX_NANO,
                                primitive_array_reader::<TimestampNanosecondType>,
                                key_filter,
                                &value,
                                |keys, values| {
                                    primitive_array_writer(keys, values, DictionaryValueArray::transform_into_timestamp_nanoseconds_array)
                                },
                            );

                            written_data_count += 1;
                        }
                        consts::OBSERVED_TIME_UNIX_NANO => {
                            if path_length > 0 {
                                log_invalid_column_access(
                                    diagnostic_receiver,
                                    *expression,
                                    consts::OBSERVED_TIME_UNIX_NANO,
                                );
                                continue;
                            }

                            set_column_with_values(
                                diagnostic_receiver,
                                *expression,
                                batches,
                                POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                                consts::OBSERVED_TIME_UNIX_NANO,
                                primitive_array_reader::<TimestampNanosecondType>,
                                key_filter,
                                &value,
                                |keys, values| {
                                    primitive_array_writer(keys, values, DictionaryValueArray::transform_into_timestamp_nanoseconds_array)
                                },
                            );

                            written_data_count += 1;
                        }
                        consts::SEVERITY_NUMBER => {
                            if path_length > 0 {
                                log_invalid_column_access(
                                    diagnostic_receiver,
                                    *expression,
                                    consts::SEVERITY_NUMBER,
                                );
                                continue;
                            }

                            set_column_with_values(
                                diagnostic_receiver,
                                *expression,
                                batches,
                                POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                                consts::SEVERITY_NUMBER,
                                adaptive_dictionary_reader::<Int32Array>,
                                key_filter,
                                &value,
                                |keys, values| {
                                    adaptive_dictionary_writer(
                                        keys,
                                        values,
                                        DictionaryValueArray::transform_into_int_array::<Int32Type>,
                                    )
                                },
                            );

                            written_data_count += 1;
                        }
                        consts::SEVERITY_TEXT => {
                            if path_length > 0 {
                                log_invalid_column_access(
                                    diagnostic_receiver,
                                    *expression,
                                    consts::SEVERITY_TEXT,
                                );
                                continue;
                            }

                            set_column_with_values(
                                diagnostic_receiver,
                                *expression,
                                batches,
                                POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                                consts::SEVERITY_TEXT,
                                adaptive_dictionary_reader::<StringArray>,
                                key_filter,
                                &value,
                                |keys, values| {
                                    adaptive_dictionary_writer(
                                        keys,
                                        values,
                                        DictionaryValueArray::transform_into_string_array,
                                    )
                                },
                            );

                            written_data_count += 1;
                        }
                        consts::TRACE_ID => {
                            if path_length > 0 {
                                log_invalid_column_access(
                                    diagnostic_receiver,
                                    *expression,
                                    consts::TRACE_ID,
                                );
                                continue;
                            }

                            set_column_with_values(
                                diagnostic_receiver,
                                *expression,
                                batches,
                                POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                                consts::TRACE_ID,
                                adaptive_dictionary_reader::<FixedSizeBinaryArray>,
                                key_filter,
                                &value,
                                |keys, values| {
                                    adaptive_dictionary_writer(
                                        keys,
                                        values,
                                        DictionaryValueArray::transform_into_fixed_sized_binary_array::<
                                            16,
                                        >,
                                    )
                                },
                            );

                            written_data_count += 1;
                        }
                        consts::SPAN_ID => {
                            if path_length > 0 {
                                log_invalid_column_access(
                                    diagnostic_receiver,
                                    *expression,
                                    consts::SPAN_ID,
                                );
                                continue;
                            }

                            set_column_with_values(
                                diagnostic_receiver,
                                *expression,
                                batches,
                                POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                                consts::SPAN_ID,
                                adaptive_dictionary_reader::<FixedSizeBinaryArray>,
                                key_filter,
                                &value,
                                |keys, values| {
                                    adaptive_dictionary_writer(
                                        keys,
                                        values,
                                        DictionaryValueArray::transform_into_fixed_sized_binary_array::<
                                            8,
                                        >,
                                    )
                                },
                            );

                            written_data_count += 1;
                        }
                        consts::FLAGS => {
                            if path_length > 0 {
                                log_invalid_column_access(
                                    diagnostic_receiver,
                                    *expression,
                                    consts::FLAGS,
                                );
                                continue;
                            }

                            set_column_with_values(
                                diagnostic_receiver,
                                *expression,
                                batches,
                                POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                                consts::FLAGS,
                                primitive_array_reader::<UInt32Type>,
                                key_filter,
                                &value,
                                |keys, values| {
                                    primitive_array_writer(
                                        keys,
                                        values,
                                        DictionaryValueArray::transform_into_int_array::<UInt32Type>,
                                    )
                                },
                            );

                            written_data_count += 1;
                        }
                        consts::EVENT_NAME => {
                            if path_length > 0 {
                                log_invalid_column_access(
                                    diagnostic_receiver,
                                    *expression,
                                    consts::EVENT_NAME,
                                );
                                continue;
                            }

                            set_column_with_values(
                                diagnostic_receiver,
                                *expression,
                                batches,
                                POSITION_LOOKUP[ArrowPayloadType::Logs as usize],
                                consts::EVENT_NAME,
                                adaptive_dictionary_reader::<StringArray>,
                                key_filter,
                                &value,
                                |keys, values| {
                                    adaptive_dictionary_writer(
                                        keys,
                                        values,
                                        DictionaryValueArray::transform_into_string_array,
                                    )
                                },
                            );

                            written_data_count += 1;
                        }
                        f => {
                            diagnostic_receiver.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                *expression,
                                || format!("Field '{f}' does not exist on log record"),
                            );
                        }
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

fn update_dictionary_values_for_path<'a>(
    source: RecordTableDictionary,
    key_filter: Option<RoaringBitmap>,
    current_path: &ColumnarEngineSelectionPath<'a>,
    remaining_path: &[ColumnarEngineSelectionPath<'a>],
    value: Dictionary<'a>,
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
            if let Some(key_filter) = &key_filter {
                if !key_filter.contains(key_index as u32) {
                    unsafe { key_writer.set_value_index_unchecked(key_index, value_index) };
                }
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
    if let Some(current_path) = remaining_path.get(0) {
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
    array: &mut Vec<ValueOrRef<'a>>,
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

    if let Some(current_path) = remaining_path.get(0) {
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

fn log_invalid_column_access<'a, T: ColumnarEngineDiagnosticReceiver<'a>>(
    diagnostic_receiver: &T,
    expression: &'a dyn Expression,
    column_name: &str,
) -> ColumnarRecordsWriteResult {
    diagnostic_receiver.add_diagnostic_if_enabled(
        ColumnarEngineDiagnosticLevel::Warn,
        expression,
        || format!("Cannot access into field '{column_name}' on log record"),
    );
    ColumnarRecordsWriteResult::NotFound
}

fn set_column<
    'a,
    TDiagnosticReceiver: ColumnarEngineDiagnosticReceiver<'a>,
    FTransform,
    const BATCH_SIZE: usize,
>(
    diagnostic_receiver: &TDiagnosticReceiver,
    expression: &'a dyn Expression,
    batches: &mut [Option<RecordBatch>; BATCH_SIZE],
    batch_position: usize,
    column_name: &str,
    value: Dictionary,
    array_transform: FTransform,
) where
    FTransform: Fn(DictionaryKeyArray, DictionaryValueArray) -> Option<Arc<dyn Array>>,
{
    if let Some(logs_batch) = batches[batch_position].take() {
        let (keys, values) = value.into_parts();

        let transformed_values = array_transform(keys, values);

        write_column_values_to_batch(
            diagnostic_receiver,
            expression,
            batches,
            batch_position,
            column_name,
            transformed_values,
            logs_batch,
        )
    }
}

fn set_column_with_values<
    'a,
    TDiagnosticReceiver: ColumnarEngineDiagnosticReceiver<'a>,
    FDictionaryTransform,
    FArrayTransform,
    const BATCH_SIZE: usize,
>(
    diagnostic_receiver: &TDiagnosticReceiver,
    expression: &'a dyn Expression,
    batches: &mut [Option<RecordBatch>; BATCH_SIZE],
    batch_position: usize,
    column_name: &str,
    dictionary_transform: FDictionaryTransform,
    key_filter: RoaringBitmap,
    values: &Dictionary,
    array_transform: FArrayTransform,
) where
    FDictionaryTransform: FnOnce(&Arc<dyn Array>) -> Option<RecordTableDictionary>,
    FArrayTransform: Fn(DictionaryKeyArray, DictionaryValueArray) -> Option<Arc<dyn Array>>,
{
    if let Some(logs_batch) = batches[batch_position].take() {
        let key_length = logs_batch.num_rows();

        let existing_values = if let Some(values) = logs_batch.column_by_name(column_name)
            && let Some(values) = dictionary_transform(values)
        {
            values.into()
        } else {
            Dictionary::new_null_with_data_type(key_length, DataType::UInt16)
        };

        let merged_values = existing_values.with_values(Some(key_filter), values);

        let (keys, values) = merged_values.into_parts();

        let transformed_values = array_transform(keys, values);

        write_column_values_to_batch(
            diagnostic_receiver,
            expression,
            batches,
            batch_position,
            column_name,
            transformed_values,
            logs_batch,
        );
    }
}

fn write_column_values_to_batch<
    'a,
    TDiagnosticReceiver: ColumnarEngineDiagnosticReceiver<'a>,
    const BATCH_SIZE: usize,
>(
    diagnostic_receiver: &TDiagnosticReceiver,
    expression: &'a dyn Expression,
    batches: &mut [Option<RecordBatch>; BATCH_SIZE],
    batch_position: usize,
    column_name: &str,
    transformed_values: Option<Arc<dyn Array>>,
    batch: RecordBatch,
) {
    let values = match transformed_values {
        None => {
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

                batches[batch_position] =
                    Some(unsafe { RecordBatch::new_unchecked(schema, columns, count) });
            }

            return;
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

    batches[batch_position] = Some(unsafe { RecordBatch::new_unchecked(schema, columns, count) });
}

fn adaptive_dictionary_reader<V: Array + 'static>(
    array: &Arc<dyn Array>,
) -> Option<RecordTableDictionary> {
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
) -> Option<RecordTableDictionary> {
    Some(RecordTableDictionary::from_array::<UInt16Type, _>(
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

type OtapBody = OnceCell<Option<RecordTableDictionary>>;

#[derive(Debug)]
pub struct OtapLogRecordBatch<'record> {
    diagnostic_level: Option<ColumnarEngineDiagnosticLevel>,
    logs: Option<&'record RecordBatch>,
    logs_schema: Option<&'record SchemaRef>,
    attributes: Option<OtapAttributes<'record>>,
    resource: Option<OtapResource<'record>>,
    scope: Option<OtapScope<'record>>,
    body: OtapBody,
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
    type RecordState = OtapBody;

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

    fn into_parts(self) -> OtapBody {
        self.body
    }
}

impl RecordTable for OtapLogRecordBatch<'_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>> {
        let key = get_log_record_schema().normalize_key(key);

        if key == consts::ATTRIBUTES {
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
                consts::TIME_UNIX_NANO
                    if let Some(time_unix_nano_column) =
                        logs_schema.column_with_name(consts::TIME_UNIX_NANO) =>
                {
                    primitive_array_reader::<TimestampNanosecondType>(
                        logs.column(time_unix_nano_column.0),
                    )
                }
                consts::OBSERVED_TIME_UNIX_NANO
                    if let Some(observed_time_unix_nano_column) =
                        logs_schema.column_with_name(consts::OBSERVED_TIME_UNIX_NANO) =>
                {
                    primitive_array_reader::<TimestampNanosecondType>(
                        logs.column(observed_time_unix_nano_column.0),
                    )
                }
                consts::SEVERITY_NUMBER
                    if let Some(severity_number_column) =
                        logs_schema.column_with_name(consts::SEVERITY_NUMBER) =>
                {
                    adaptive_dictionary_reader::<Int32Array>(logs.column(severity_number_column.0))
                }
                consts::SEVERITY_TEXT
                    if let Some(severity_text_column) =
                        logs_schema.column_with_name(consts::SEVERITY_TEXT) =>
                {
                    adaptive_dictionary_reader::<StringArray>(logs.column(severity_text_column.0))
                }
                consts::BODY => self
                    .body
                    .get_or_init(|| build_logs_body_dictionary(logs, logs_schema))
                    .clone(),
                consts::TRACE_ID
                    if let Some(trace_id_column) =
                        logs_schema.column_with_name(consts::TRACE_ID) =>
                {
                    adaptive_dictionary_reader::<FixedSizeBinaryArray>(
                        logs.column(trace_id_column.0),
                    )
                }
                consts::SPAN_ID
                    if let Some(span_id_column) = logs_schema.column_with_name(consts::SPAN_ID) =>
                {
                    adaptive_dictionary_reader::<FixedSizeBinaryArray>(
                        logs.column(span_id_column.0),
                    )
                }
                consts::FLAGS
                    if let Some(flags_column) = logs_schema.column_with_name(consts::FLAGS) =>
                {
                    primitive_array_reader::<UInt32Type>(logs.column(flags_column.0))
                }
                consts::EVENT_NAME
                    if let Some(event_name_column) =
                        logs_schema.column_with_name(consts::EVENT_NAME) =>
                {
                    adaptive_dictionary_reader::<StringArray>(logs.column(event_name_column.0))
                }
                _ => return None,
            };

            return values.map(RecordTableValue::Dictionary);
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
pub struct OtapResource<'record> {
    resource_struct: &'record StructArray,
    attributes: Option<OtapAttributes<'record>>,
}

impl RecordTable for OtapResource<'_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'_>> {
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
        if key == consts::ATTRIBUTES || key == "Attributes" {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        let values = match key {
            consts::NAME | "Name"
                if let Some(name_column) = self.scope_struct.column_by_name("name") =>
            {
                adaptive_dictionary_reader::<StringArray>(name_column)
            }
            consts::VERSION | "Version"
                if let Some(version_column) = self.scope_struct.column_by_name("version") =>
            {
                adaptive_dictionary_reader::<StringArray>(version_column)
            }
            _ => return None,
        };

        values.map(RecordTableValue::Dictionary)
    }
}

impl Display for OtapScope<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scope(RecordCount={})", self.scope_struct.len())
    }
}

#[derive(Debug)]
pub struct OtapAttributes<'record> {
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

fn build_logs_body_dictionary(
    logs: &RecordBatch,
    logs_schema: &Schema,
) -> Option<RecordTableDictionary> {
    if let Some(body_column) = logs_schema.column_with_name(consts::BODY) {
        let body_struct = logs.column(body_column.0).as_struct();

        build_logs_body_dictionary_from_struct(body_struct)
    } else {
        None
    }
}

fn build_logs_body_dictionary_from_struct(
    body_struct: &StructArray,
) -> Option<RecordTableDictionary> {
    if let Some(body_type) = body_struct.column_by_name("type") {
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
                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, *index) };
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

                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, index) };
                        continue;
                    }
                }
                4 => {
                    if let Some(body_bools) = body_bools
                        .get_or_init(|| body_struct.column_by_name("bool").map(|v| v.as_boolean()))
                    {
                        let index = values.len() as u16;
                        values.push(ValueOrRef::Boolean(body_bools.value(key_index)));

                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, index) };
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
                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, *index) };
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
                        unsafe { key_writer.set_value_index_typed_unchecked(key_index, *index) };
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

        return Some(RecordTableDictionary::new(
            key_builder.finish().into(),
            RecordTableDictionaryValueArray::Vec(values.into()),
        ));
    }

    None
}
