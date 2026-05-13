// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, sync::LazyLock};

use data_engine_columnar::*;
use data_engine_expressions::*;
use data_engine_kql_parser::*;
use otap_df_pdata::{
    otap::{Logs, OtapBatchStore, raw_batch_store::RawLogsStore},
    schema::consts,
};

use crate::{logs::*, *};

static LOG_RECORD_SCHEMA: LazyLock<ParserMapSchema> = LazyLock::new(|| {
    // Canonical schema definition comes from LogRecord proto definition
    // https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/pdata/src/views/otlp/proto/logs.rs
    ParserMapSchema::new()
        .set_default_map_key(consts::ATTRIBUTES)
        .with_key_definition(consts::TIME_UNIX_NANO, ParserMapKeySchema::DateTime)
        .with_key_definition(
            consts::OBSERVED_TIME_UNIX_NANO,
            ParserMapKeySchema::DateTime,
        )
        .with_key_definition(consts::SEVERITY_NUMBER, ParserMapKeySchema::Integer)
        .with_key_definition(consts::SEVERITY_TEXT, ParserMapKeySchema::String)
        .with_key_definition(consts::BODY, ParserMapKeySchema::Any)
        .with_key_definition(consts::TRACE_ID, ParserMapKeySchema::Array)
        .with_key_definition(consts::SPAN_ID, ParserMapKeySchema::Array)
        .with_key_definition(consts::FLAGS, ParserMapKeySchema::Integer)
        .with_key_definition(consts::EVENT_NAME, ParserMapKeySchema::String)
        .with_key_aliases([
            // Support aliases to the Log and Event definition naming
            // https://opentelemetry.io/docs/specs/otel/logs/data-model/
            ("Attributes", consts::ATTRIBUTES),
            ("Timestamp", consts::TIME_UNIX_NANO),
            ("ObservedTimestamp", consts::OBSERVED_TIME_UNIX_NANO),
            ("SeverityNumber", consts::SEVERITY_NUMBER),
            ("SeverityText", consts::SEVERITY_TEXT),
            ("Body", consts::BODY),
            ("TraceId", consts::TRACE_ID),
            ("SpanId", consts::SPAN_ID),
            ("TraceFlags", consts::FLAGS),
            ("EventName", consts::EVENT_NAME),
            // Support aliases from OTLP JSON encoding
            // https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
            ("timeUnixNano", consts::TIME_UNIX_NANO),
            ("observedTimeUnixNano", consts::OBSERVED_TIME_UNIX_NANO),
            ("severityNumber", consts::SEVERITY_NUMBER),
            ("severityText", consts::SEVERITY_TEXT),
            ("traceId", consts::TRACE_ID),
            ("spanId", consts::SPAN_ID),
            ("traceFlags", consts::FLAGS),
            ("eventName", consts::EVENT_NAME),
        ])
});

pub(crate) fn get_log_record_schema() -> &'static ParserMapSchema {
    &LOG_RECORD_SCHEMA
}

#[derive(Debug)]
pub struct BridgePipeline {
    attributes_schema: Option<ParserMapSchema>,
    engine: ColumnarEngine,
    options: BridgeOptions,
}

impl BridgePipeline {
    pub fn get_pipeline(&self) -> &PipelineExpression {
        self.engine.get_pipeline()
    }
}

impl Display for BridgePipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.engine.get_pipeline().fmt(f)
    }
}

#[derive(Debug)]
pub struct BridgeResponse<'a, T> {
    pub included_records: T,
    pub included_record_count: usize,
    pub dropped_record_count: usize,
    pub diagnostics: BridgeDiagnostics<'a>,
}

impl<T> Display for BridgeResponse<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.diagnostics.fmt(f)
    }
}

#[derive(Debug)]
pub struct BridgeDiagnostics<'a> {
    pipeline: &'a BridgePipeline,
    diagnostics: Vec<ColumnarEngineDiagnostic<'a>>,
}

impl<'a> BridgeDiagnostics<'a> {
    pub fn is_empty(&self) -> bool {
        self.diagnostics.is_empty()
    }

    pub fn get_pipeline(&self) -> &'a BridgePipeline {
        self.pipeline
    }

    pub fn get_diagnostics(&self) -> &[ColumnarEngineDiagnostic<'a>] {
        &self.diagnostics
    }
}

impl Display for BridgeDiagnostics<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        format_diagnostics(
            self.pipeline.get_pipeline().get_query(),
            &self.diagnostics,
            f,
        )
    }
}

pub fn parse_kql_logs_query_into_pipeline(
    query: &str,
    options: Option<BridgeOptions>,
) -> Result<BridgePipeline, Vec<ParserError>> {
    let mut options = options.unwrap_or_default();

    let parser_options = build_parser_options_for_logs_query(&mut options).map_err(|e| vec![e])?;
    let attributes_schema = match parser_options
        .get_source_map_schema()
        .and_then(|s| s.get_schema_for_key("Attributes"))
    {
        Some(ParserMapKeySchema::Map(Some(attributes_schema))) => Some(attributes_schema.clone()),
        _ => None,
    };
    let result = KqlParser::parse_with_options(query, parser_options)?;
    Ok(BridgePipeline {
        attributes_schema,
        engine: ColumnarEngine::new(result.pipeline),
        options,
    })
}

pub fn process_otap_logs_using_pipeline<'a>(
    pipeline: &'a BridgePipeline,
    factory: &OtapLogRecordBatchFactory,
    otap_logs: Logs,
) -> Result<BridgeResponse<'a, Logs>, BridgeError> {
    let mut batch = pipeline
        .engine
        .begin_batch()
        .map_err(|e| BridgeError::PipelineInitializationError(e.to_string()))?;

    let batches = otap_logs.into_batches();

    batch.push_records(factory, batches);

    let results = batch.flush();

    let mut logs = RawLogsStore::new();

    if !results.included_batches.is_empty() {
        let batches = logs.batches_mut();
        for (index, batch) in results
            .included_batches
            .into_iter()
            .next()
            .unwrap()
            .into_iter()
            .enumerate()
        {
            batches[index] = batch;
        }
    }

    Ok(BridgeResponse {
        included_records: logs.try_into().unwrap(),
        included_record_count: results.included_record_count,
        dropped_record_count: results.dropped_record_count,
        diagnostics: BridgeDiagnostics {
            pipeline,
            diagnostics: results.diagnostics,
        },
    })
}

fn build_parser_options_for_logs_query(
    options: &mut BridgeOptions,
) -> Result<ParserOptions, ParserError> {
    let mut parser_options = ParserOptions::new().with_attached_data_names(&[
        "resource",
        "instrumentation_scope",
        "scope",
    ]);

    let (log_record_schema, summary_schema) =
        build_log_record_schema(options.take_attributes_schema())?;

    if let Some(summary_schema) = summary_schema {
        parser_options = parser_options.with_summary_map_schema(summary_schema);
    }

    Ok(parser_options.with_source_map_schema(log_record_schema))
}

fn build_log_record_schema(
    attributes_schema: Option<ParserMapSchema>,
) -> Result<(ParserMapSchema, Option<ParserMapSchema>), ParserError> {
    let mut log_record_schema = LOG_RECORD_SCHEMA.clone();

    if let Some(mut attributes_schema) = attributes_schema {
        let schema = attributes_schema.get_schema_mut();
        for (top_level_key, top_level_key_schema) in log_record_schema.get_schema() {
            // Note: If any top-level fields are duplicated on Attributes Schema
            // they get removed automatically. This is done for two purposes.
            // The first is to make it easy for callers to pass in something
            // like a table schema. Many backends flatten log records into
            // columns. This feature is essentially a convenience thing so
            // callers with table schema don't need to map columns back to the
            // log record schema. The second reason is to prevent
            // accidental\confusing query results. If for example "Body" is
            // present in Attributes users might query with ambiguous naming.
            // For example: source | extend Body = 'something' will write to the
            // top-level field and not Attributes.

            // Check both the canonical key and all its aliases
            for key_name in log_record_schema.get_all_key_names_for_canonical_key(top_level_key) {
                if let Some(removed) = schema.remove(key_name.as_ref())
                    && &removed != top_level_key_schema
                {
                    return Err(ParserError::SchemaError(format!(
                        "'{key_name}' key cannot be declared as '{}' type",
                        &removed
                    )));
                }
            }
        }

        let allow_undefined_keys = attributes_schema.get_allow_undefined_keys();

        log_record_schema = log_record_schema.with_key_definition(
            "attributes",
            ParserMapKeySchema::Map(Some(attributes_schema)),
        );

        let mut summary_schema = ParserMapSchema::new();

        if allow_undefined_keys {
            summary_schema = summary_schema.set_allow_undefined_keys();
        }

        for (top_level_key, top_level_key_schema) in log_record_schema.get_schema() {
            if top_level_key.as_ref() == "attributes" {
                if let ParserMapKeySchema::Map(Some(attributes_schema)) = top_level_key_schema {
                    for (top_level_key, top_level_key_schema) in attributes_schema.get_schema() {
                        summary_schema = summary_schema
                            .with_key_definition(top_level_key, top_level_key_schema.clone());
                    }
                }
                continue;
            }
            summary_schema =
                summary_schema.with_key_definition(top_level_key, top_level_key_schema.clone());
        }

        return Ok((log_record_schema, Some(summary_schema)));
    }

    Ok((log_record_schema, None))
}

#[cfg(test)]
mod tests {
    use otap_df_pdata::proto::OtlpProtoMessage;
    use otap_df_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
    use otap_df_pdata::proto::opentelemetry::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value::Value,
    };
    use otap_df_pdata::proto::opentelemetry::logs::v1::{
        LogRecord, LogsData, ResourceLogs, ScopeLogs,
    };
    use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
    use otap_df_pdata::testing::round_trip::{otap_to_otlp, otlp_to_otap, to_otap_logs};
    use otap_df_pdata::{otap::OtapBatchStore, *};

    use super::*;

    #[test]
    fn test_engine_filter_all() {
        let log_records = vec![
            LogRecord::build().finish(),
            LogRecord::build().finish(),
            LogRecord::build().finish(),
            LogRecord::build().finish(),
        ];

        let otap_batch = to_otap_logs(log_records);

        let logs = match otap_batch {
            OtapArrowRecords::Logs(l) => l,
            _ => panic!(),
        };

        assert_eq!(
            4,
            logs.get(ArrowPayloadType::Logs).map_or(0, |v| v.num_rows())
        );

        let pipeline = parse_kql_logs_query_into_pipeline("source | where false", None).unwrap();

        println!("{pipeline}");

        let results = process_otap_logs_using_pipeline(
            &pipeline,
            &OtapLogRecordBatchFactory::new_with_options(Some(
                ColumnarEngineDiagnosticLevel::Verbose,
            )),
            logs,
        )
        .unwrap();

        println!("{results}");

        assert_eq!(4, results.dropped_record_count);
        assert_eq!(0, results.included_record_count);
    }

    #[test]
    fn test_engine_filter_severity_text_info() {
        let logs = LogsData {
            resource_logs: vec![
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: "resource1_attr1".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("value1".into())),
                            }),
                        }],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord::build().severity_text("Info").finish()],
                        scope: Some(InstrumentationScope {
                            attributes: vec![KeyValue {
                                key: "scope1_attr1".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("value1".into())),
                                }),
                            }],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: "resource2_attr1".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("value1".into())),
                            }),
                        }],
                        ..Default::default()
                    }),
                    scope_logs: vec![
                        ScopeLogs {
                            log_records: vec![
                                LogRecord::build().severity_text("Warn").finish(),
                                LogRecord::build().severity_text("Error").finish(),
                            ],
                            scope: Some(InstrumentationScope {
                                attributes: vec![KeyValue {
                                    key: "scope2_attr1".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::StringValue("value1".into())),
                                    }),
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        ScopeLogs {
                            log_records: vec![LogRecord::build().finish()],
                            scope: Some(InstrumentationScope {
                                attributes: vec![KeyValue {
                                    key: "scope3_attr1".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::StringValue("value2".into())),
                                    }),
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
        };

        let otap_batch = otlp_to_otap(&OtlpProtoMessage::Logs(logs));

        let logs = match otap_batch {
            OtapArrowRecords::Logs(l) => l,
            _ => panic!(),
        };

        assert_eq!(
            2,
            logs.get(ArrowPayloadType::ResourceAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            3,
            logs.get(ArrowPayloadType::ScopeAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            4,
            logs.get(ArrowPayloadType::Logs).map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            0,
            logs.get(ArrowPayloadType::LogAttrs)
                .map_or(0, |v| v.num_rows())
        );

        let pipeline =
            parse_kql_logs_query_into_pipeline("source | where severity_text == 'Info'", None)
                .unwrap();

        println!("{pipeline}");

        let results = process_otap_logs_using_pipeline(
            &pipeline,
            &OtapLogRecordBatchFactory::new_with_options(Some(
                ColumnarEngineDiagnosticLevel::Verbose,
            )),
            logs,
        )
        .unwrap();

        println!("{results}");

        assert_eq!(3, results.dropped_record_count);
        assert_eq!(1, results.included_record_count);

        let final_batch = &results.included_records;

        assert_eq!(
            1,
            final_batch
                .get(ArrowPayloadType::ResourceAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            1,
            final_batch
                .get(ArrowPayloadType::ScopeAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            1,
            final_batch
                .get(ArrowPayloadType::Logs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            0,
            final_batch
                .get(ArrowPayloadType::LogAttrs)
                .map_or(0, |v| v.num_rows())
        );
    }

    #[test]
    fn test_engine_filter_attribute() {
        let log_records = vec![
            LogRecord::build()
                .attributes(vec![KeyValue {
                    key: "attr1".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("value1".into())),
                    }),
                }])
                .finish(),
            LogRecord::build()
                .attributes(vec![KeyValue {
                    key: "attr1".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("value1".into())),
                    }),
                }])
                .finish(),
            LogRecord::build()
                .attributes(vec![KeyValue {
                    key: "attr1".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("value2".into())),
                    }),
                }])
                .finish(),
            LogRecord::build()
                .attributes(vec![KeyValue {
                    key: "attr1".into(),
                    value: Some(AnyValue {
                        value: Some(Value::IntValue(18)),
                    }),
                }])
                .finish(),
            LogRecord::build().finish(),
        ];

        let otap_batch = to_otap_logs(log_records);

        let logs = match otap_batch {
            OtapArrowRecords::Logs(l) => l,
            _ => panic!(),
        };

        assert_eq!(
            5,
            logs.get(ArrowPayloadType::Logs).map_or(0, |v| v.num_rows())
        );

        let pipeline = parse_kql_logs_query_into_pipeline(
            "source | where Attributes['attr1'] >= 18 or Attributes['attr1'] == 'value1'",
            None,
        )
        .unwrap();

        println!("{pipeline}");

        let results = process_otap_logs_using_pipeline(
            &pipeline,
            &OtapLogRecordBatchFactory::new_with_options(Some(
                ColumnarEngineDiagnosticLevel::Verbose,
            )),
            logs,
        )
        .unwrap();

        println!("{results}");

        assert_eq!(2, results.dropped_record_count);
        assert_eq!(3, results.included_record_count);

        let final_logs = &results.included_records;

        assert_eq!(
            3,
            final_logs
                .get(ArrowPayloadType::Logs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            3,
            final_logs
                .get(ArrowPayloadType::LogAttrs)
                .map_or(0, |v| v.num_rows())
        );
    }

    #[test]
    fn test_engine_filter_resource() {
        let logs = LogsData {
            resource_logs: vec![
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: "resource_attr1".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("value1".into())),
                            }),
                        }],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord::build().severity_text("Info").finish()],
                        scope: Some(InstrumentationScope {
                            attributes: vec![KeyValue {
                                key: "scope_attr1".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("value1".into())),
                                }),
                            }],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: "resource_attr1".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("value2".into())),
                            }),
                        }],
                        ..Default::default()
                    }),
                    scope_logs: vec![
                        ScopeLogs {
                            log_records: vec![
                                LogRecord::build().severity_text("Warn").finish(),
                                LogRecord::build().severity_text("Error").finish(),
                            ],
                            scope: Some(InstrumentationScope {
                                attributes: vec![KeyValue {
                                    key: "scope_attr1".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::StringValue("value1".into())),
                                    }),
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        ScopeLogs {
                            log_records: vec![LogRecord::build().finish()],
                            scope: Some(InstrumentationScope {
                                attributes: vec![KeyValue {
                                    key: "scope_attr1".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::StringValue("value2".into())),
                                    }),
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
        };

        let otap_batch = otlp_to_otap(&OtlpProtoMessage::Logs(logs));

        let logs = match otap_batch {
            OtapArrowRecords::Logs(l) => l,
            _ => panic!(),
        };

        assert_eq!(
            2,
            logs.get(ArrowPayloadType::ResourceAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            3,
            logs.get(ArrowPayloadType::ScopeAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            4,
            logs.get(ArrowPayloadType::Logs).map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            0,
            logs.get(ArrowPayloadType::LogAttrs)
                .map_or(0, |v| v.num_rows())
        );

        let pipeline = parse_kql_logs_query_into_pipeline(
            "source | where resource.attributes['resource_attr1'] == 'value1'",
            None,
        )
        .unwrap();

        println!("{pipeline}");

        let results = process_otap_logs_using_pipeline(
            &pipeline,
            &OtapLogRecordBatchFactory::new_with_options(Some(
                ColumnarEngineDiagnosticLevel::Verbose,
            )),
            logs,
        )
        .unwrap();

        println!("{results}");

        assert_eq!(3, results.dropped_record_count);
        assert_eq!(1, results.included_record_count);

        let final_logs = &results.included_records;

        assert_eq!(
            1,
            final_logs
                .get(ArrowPayloadType::ResourceAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            1,
            final_logs
                .get(ArrowPayloadType::ScopeAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            1,
            final_logs
                .get(ArrowPayloadType::Logs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            0,
            final_logs
                .get(ArrowPayloadType::LogAttrs)
                .map_or(0, |v| v.num_rows())
        );
    }

    #[test]
    fn test_engine_filter_scope() {
        let logs = LogsData {
            resource_logs: vec![
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: "resource_attr1".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("value1".into())),
                            }),
                        }],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        log_records: vec![LogRecord::build().severity_text("Info").finish()],
                        scope: Some(InstrumentationScope {
                            attributes: vec![KeyValue {
                                key: "scope_attr1".into(),
                                value: Some(AnyValue {
                                    value: Some(Value::StringValue("value1".into())),
                                }),
                            }],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![KeyValue {
                            key: "resource_attr1".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("value2".into())),
                            }),
                        }],
                        ..Default::default()
                    }),
                    scope_logs: vec![
                        ScopeLogs {
                            log_records: vec![
                                LogRecord::build().severity_text("Warn").finish(),
                                LogRecord::build().severity_text("Error").finish(),
                            ],
                            scope: Some(InstrumentationScope {
                                version: "version2".into(),
                                attributes: vec![KeyValue {
                                    key: "scope_attr1".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::StringValue("value1".into())),
                                    }),
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        ScopeLogs {
                            log_records: vec![LogRecord::build().finish()],
                            scope: Some(InstrumentationScope {
                                name: "scope3".into(),
                                attributes: vec![KeyValue {
                                    key: "scope_attr1".into(),
                                    value: Some(AnyValue {
                                        value: Some(Value::StringValue("value2".into())),
                                    }),
                                }],
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
        };

        let otap_batch = otlp_to_otap(&OtlpProtoMessage::Logs(logs));

        let logs = match otap_batch {
            OtapArrowRecords::Logs(l) => l,
            _ => panic!(),
        };

        assert_eq!(
            2,
            logs.get(ArrowPayloadType::ResourceAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            3,
            logs.get(ArrowPayloadType::ScopeAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            4,
            logs.get(ArrowPayloadType::Logs).map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            0,
            logs.get(ArrowPayloadType::LogAttrs)
                .map_or(0, |v| v.num_rows())
        );

        let pipeline = parse_kql_logs_query_into_pipeline("source | where scope.name == 'invalid' or scope.version == 'invalid' or scope.attributes['scope_attr1'] == 'value1'", None)
            .unwrap();

        println!("{pipeline}");

        let results = process_otap_logs_using_pipeline(
            &pipeline,
            &OtapLogRecordBatchFactory::new_with_options(Some(
                ColumnarEngineDiagnosticLevel::Verbose,
            )),
            logs,
        )
        .unwrap();

        println!("{results}");

        assert_eq!(2, results.dropped_record_count);
        assert_eq!(2, results.included_record_count);

        let final_logs = &results.included_records;

        assert_eq!(
            2,
            final_logs
                .get(ArrowPayloadType::ResourceAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            2,
            final_logs
                .get(ArrowPayloadType::ScopeAttrs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            2,
            final_logs
                .get(ArrowPayloadType::Logs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            0,
            final_logs
                .get(ArrowPayloadType::LogAttrs)
                .map_or(0, |v| v.num_rows())
        );
    }

    macro_rules! test_engine_set_field_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[test]
                fn $name() {
                    let (log_records, query, validation) = $value;
                    let logs = LogsData {
                        resource_logs: vec![ResourceLogs {
                            scope_logs: vec![ScopeLogs {
                                log_records,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                    };

                    let otap_batch = otlp_to_otap(&OtlpProtoMessage::Logs(logs));

                    let logs = match otap_batch {
                        OtapArrowRecords::Logs(l) => l,
                        _ => panic!(),
                    };

                    assert_eq!(
                        3,
                        logs.get(ArrowPayloadType::Logs).map_or(0, |v| v.num_rows())
                    );

                    let pipeline = parse_kql_logs_query_into_pipeline(
                        query,
                        None,
                    )
                    .unwrap();

                    println!("{pipeline}");

                    let results = process_otap_logs_using_pipeline(
                        &pipeline,
                        &OtapLogRecordBatchFactory::new_with_options(Some(
                            ColumnarEngineDiagnosticLevel::Verbose,
                        )),
                        logs,
                    )
                    .unwrap();

                    println!("{results}");

                    assert_eq!(0, results.dropped_record_count);
                    assert_eq!(3, results.included_record_count);

                    let final_batch = results.included_records;

                    assert_eq!(
                        3,
                        final_batch
                            .get(ArrowPayloadType::Logs)
                            .map_or(0, |v| v.num_rows())
                    );

                    if let OtlpProtoMessage::Logs(logs) = otap_to_otlp(&OtapArrowRecords::Logs(final_batch)) {
                        validation(&logs.resource_logs[0].scope_logs[0].log_records);
                    } else {
                        panic!()
                    }
                }
            )*
        }
    }

    test_engine_set_field_tests! {
        test_engine_set_severity_text_column_exists: (
            vec![
                LogRecord::build().severity_text("hello world").finish(),
                LogRecord::build().finish(),
                LogRecord::build().severity_text("goodbye world").finish(),
            ],
            "source | extend severity_text = 'hello world'",
            |logs: &Vec<LogRecord>| {
                assert_eq!(
                    "hello world",
                    logs[0].severity_text);
                assert_eq!(
                    "hello world",
                    logs[1].severity_text);
                assert_eq!(
                    "hello world",
                    logs[2].severity_text);
            }),
        test_engine_set_severity_text_column_doesnt_exist: (
            vec![
                LogRecord::build().finish(),
                LogRecord::build().finish(),
                LogRecord::build().finish(),
            ],
            "source | extend severity_text = 'hello world'",
            |logs: &Vec<LogRecord>| {
                assert_eq!(
                    "hello world",
                    logs[0].severity_text);
                assert_eq!(
                    "hello world",
                    logs[1].severity_text);
                assert_eq!(
                    "hello world",
                    logs[2].severity_text);
            }),
        test_engine_set_event_name_column_exists: (
            vec![
                LogRecord::build().event_name("event1").finish(),
                LogRecord::build().finish(),
                LogRecord::build().event_name("event2").finish(),
            ],
            "source | extend event_name = 'my_event'",
            |logs: &Vec<LogRecord>| {
                assert_eq!(
                    "my_event",
                    logs[0].event_name);
                assert_eq!(
                    "my_event",
                    logs[1].event_name);
                assert_eq!(
                    "my_event",
                    logs[2].event_name);
            }),
        test_engine_set_event_name_column_doesnt_exist: (
            vec![
                LogRecord::build().finish(),
                LogRecord::build().finish(),
                LogRecord::build().finish(),
            ],
            "source | extend event_name = 'my_event'",
            |logs: &Vec<LogRecord>| {
                assert_eq!(
                    "my_event",
                    logs[0].event_name);
                assert_eq!(
                    "my_event",
                    logs[1].event_name);
                assert_eq!(
                    "my_event",
                    logs[2].event_name);
            }),
        test_engine_set_severity_number_column_exists: (
            vec![
                LogRecord::build().severity_number(0).finish(),
                LogRecord::build().finish(),
                LogRecord::build().severity_number(1).finish(),
            ],
            "source | extend severity_number = 18",
            |logs: &Vec<LogRecord>| {
                assert_eq!(
                    18,
                    logs[0].severity_number);
                assert_eq!(
                    18,
                    logs[1].severity_number);
                assert_eq!(
                    18,
                    logs[2].severity_number);
            }),
        test_engine_set_severity_number_column_doesnt_exist: (
            vec![
                LogRecord::build().finish(),
                LogRecord::build().finish(),
                LogRecord::build().finish(),
            ],
            "source | extend severity_number = 18",
            |logs: &Vec<LogRecord>| {
                assert_eq!(
                    18,
                    logs[0].severity_number);
                assert_eq!(
                    18,
                    logs[1].severity_number);
                assert_eq!(
                    18,
                    logs[2].severity_number);
            }),
        test_engine_set_dynamic_column: (
            vec![
                LogRecord::build().attributes(vec![KeyValue { key: "some_attr".into(), value: Some(AnyValue { value: Some(Value::StringValue("severity_text".into())) }) }]).finish(),
                LogRecord::build().finish(),
                LogRecord::build().attributes(vec![KeyValue { key: "some_attr".into(), value: Some(AnyValue { value: Some(Value::StringValue("event_name".into())) }) }]).finish(),
            ],
            "source | extend source[some_attr] = 'hello world'",
            |logs: &Vec<LogRecord>| {
                assert_eq!(
                    "hello world",
                    logs[0].severity_text);
                assert_eq!(
                    "hello world",
                    logs[2].event_name);
            }),
    }

    #[test]
    fn test_engine_set_attribute() {
        let logs = LogsData {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![
                        LogRecord::build().finish(),
                        LogRecord::build().finish(),
                        LogRecord::build().finish(),
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let otap_batch = otlp_to_otap(&OtlpProtoMessage::Logs(logs));

        let logs = match otap_batch {
            OtapArrowRecords::Logs(l) => l,
            _ => panic!(),
        };

        assert_eq!(
            3,
            logs.get(ArrowPayloadType::Logs).map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            0,
            logs.get(ArrowPayloadType::LogAttrs)
                .map_or(0, |v| v.num_rows())
        );

        let pipeline =
            parse_kql_logs_query_into_pipeline("source | extend new_attr = 'hello world'", None)
                .unwrap();

        println!("{pipeline}");

        let results = process_otap_logs_using_pipeline(
            &pipeline,
            &OtapLogRecordBatchFactory::new_with_options(Some(
                ColumnarEngineDiagnosticLevel::Verbose,
            )),
            logs,
        )
        .unwrap();

        println!("{results}");

        assert_eq!(0, results.dropped_record_count);
        assert_eq!(3, results.included_record_count);

        let final_batch = &results.included_records;

        assert_eq!(
            3,
            final_batch
                .get(ArrowPayloadType::Logs)
                .map_or(0, |v| v.num_rows())
        );
        assert_eq!(
            3,
            final_batch
                .get(ArrowPayloadType::LogAttrs)
                .map_or(0, |v| v.num_rows())
        );
    }
}
