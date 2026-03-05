// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::LazyLock;

use arrow::array::RecordBatch;
use data_engine_expressions::*;
use data_engine_kql_parser::*;
use data_engine_columnar::*;
use otap_df_pdata::otap::Logs;

use crate::*;

static LOG_RECORD_SCHEMA: LazyLock<ParserMapSchema> = LazyLock::new(|| {
    // Canonical schema definition comes from LogRecord proto definition
    // https://github.com/open-telemetry/otel-arrow/blob/main/rust/otap-dataflow/crates/pdata/src/views/otlp/proto/logs.rs
    ParserMapSchema::new()
        .set_default_map_key("attributes")
        .with_key_definition("time_unix_nano", ParserMapKeySchema::DateTime)
        .with_key_definition("observed_time_unix_nano", ParserMapKeySchema::DateTime)
        .with_key_definition("severity_number", ParserMapKeySchema::Integer)
        .with_key_definition("severity_text", ParserMapKeySchema::String)
        .with_key_definition("body", ParserMapKeySchema::Any)
        .with_key_definition("trace_id", ParserMapKeySchema::Array)
        .with_key_definition("span_id", ParserMapKeySchema::Array)
        .with_key_definition("flags", ParserMapKeySchema::Integer)
        .with_key_definition("event_name", ParserMapKeySchema::String)
        .with_key_aliases([
            // Support aliases to the Log and Event definition naming
            // https://opentelemetry.io/docs/specs/otel/logs/data-model/
            ("Attributes", "attributes"),
            ("Timestamp", "time_unix_nano"),
            ("ObservedTimestamp", "observed_time_unix_nano"),
            ("SeverityNumber", "severity_number"),
            ("SeverityText", "severity_text"),
            ("Body", "body"),
            ("TraceId", "trace_id"),
            ("SpanId", "span_id"),
            ("TraceFlags", "flags"),
            ("EventName", "event_name"),
            // Support aliases from OTLP JSON encoding
            // https://opentelemetry.io/docs/specs/otlp/#json-protobuf-encoding
            ("timeUnixNano", "time_unix_nano"),
            ("observedTimeUnixNano", "observed_time_unix_nano"),
            ("severityNumber", "severity_number"),
            ("severityText", "severity_text"),
            ("traceId", "trace_id"),
            ("spanId", "span_id"),
            ("eventName", "event_name"),
        ])
});

#[derive(Debug)]
pub struct BridgePipeline {
    attributes_schema: Option<ParserMapSchema>,
    pipeline: PipelineExpression,
    options: BridgeOptions,
}

impl BridgePipeline {
    pub fn get_pipeline(&self) -> &PipelineExpression {
        &self.pipeline
    }
}

#[derive(Debug)]
pub struct BridgeResponse {
    pub included_records: Option<[RecordBatch]>,
    pub included_record_count: usize,
    pub dropped_record_count: usize,
}

pub fn parse_kql_logs_query_into_pipeline(
    query: &str,
    options: Option<BridgeOptions>,
) -> Result<BridgePipeline, Vec<ParserError>> {
    let mut options = options.unwrap_or_default();

    let parser_options = build_parser_options(&mut options).map_err(|e| vec![e])?;
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
        pipeline: result.pipeline,
        options,
    })
}

pub fn process_otap_logs_using_pipeline(
    pipeline: &BridgePipeline,
    diagnostic_level: ColumnarEngineDiagnosticLevel,
    otap_logs: Logs,
) -> Result<BridgeResponse, BridgeError> {
    let request =
        ExportLogsServiceRequest::from_protobuf(export_logs_service_request_protobuf_data);

    if let Err(e) = request {
        return Err(BridgeError::OtlpProtobufReadError(e));
    }

    process_export_logs_service_request_using_pipeline(pipeline, log_level, request.unwrap())
}

fn build_parser_options(options: &mut BridgeOptions) -> Result<ParserOptions, ParserError> {
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
    use bytes::Bytes;
    use data_engine_kql_parser::{KqlParser, Parser};
    use otap_df_pdata::{otap::OtapBatchStore, *};

    use super::*;

    #[test]
    fn test_engine_filter_all() {
        let pdata: OtapPayload = OtlpProtoBytes::ExportLogsRequest(Bytes::from_static(&[
            10, 100, 10, 0, 18, 96, 18, 25, 26, 4, 73, 110, 102, 111, 50, 17, 10, 5, 97, 116, 116,
            114, 49, 18, 8, 10, 6, 118, 97, 108, 117, 101, 49, 18, 19, 50, 17, 10, 5, 97, 116, 116,
            114, 49, 18, 8, 10, 6, 118, 97, 108, 117, 101, 50, 18, 6, 26, 4, 87, 97, 114, 110, 18,
            38, 26, 4, 87, 97, 114, 110, 50, 17, 10, 5, 97, 116, 116, 114, 49, 18, 8, 10, 6, 118,
            97, 108, 117, 101, 49, 50, 11, 10, 5, 97, 116, 116, 114, 50, 18, 2, 24, 18,
        ]))
        .into();

        let otap_batch: OtapArrowRecords = pdata.try_into().unwrap();

        let logs = match otap_batch {
            OtapArrowRecords::Logs(l) => l,
            _ => panic!(),
        };

        let batches = logs.into_batches();

        let pipeline = KqlParser::parse(
            "source | where false",
        )
        .unwrap()
        .pipeline;

        let engine = ColumnarEngine::new_with_options(pipeline, ColumnarEngineOptions::new().with_diagnostic_level(ColumnarEngineDiagnosticLevel::Verbose));

        let mut batch = engine.begin_batch().unwrap();

        batch.push_records::<OtapLogRecordBatchFactory>(batches);

        let results = batch.flush();

        assert_eq!(4, results.dropped_record_count);
        assert!(results.included_batches.is_empty());

        println!("{results}");
    }

    #[test]
    fn test_engine_filter_severity_text_info() {
        let pdata: OtapPayload = OtlpProtoBytes::ExportLogsRequest(Bytes::from_static(&[
            10, 100, 10, 0, 18, 96, 18, 25, 26, 4, 73, 110, 102, 111, 50, 17, 10, 5, 97, 116, 116,
            114, 49, 18, 8, 10, 6, 118, 97, 108, 117, 101, 49, 18, 19, 50, 17, 10, 5, 97, 116, 116,
            114, 49, 18, 8, 10, 6, 118, 97, 108, 117, 101, 50, 18, 6, 26, 4, 87, 97, 114, 110, 18,
            38, 26, 4, 87, 97, 114, 110, 50, 17, 10, 5, 97, 116, 116, 114, 49, 18, 8, 10, 6, 118,
            97, 108, 117, 101, 49, 50, 11, 10, 5, 97, 116, 116, 114, 50, 18, 2, 24, 18,
        ]))
        .into();

        let otap_batch: OtapArrowRecords = pdata.try_into().unwrap();

        let logs = match otap_batch {
            OtapArrowRecords::Logs(l) => l,
            _ => panic!(),
        };

        let batches = logs.into_batches();

        assert_eq!(4, batches[2].as_ref().map_or(0, |v| v.num_rows()));

        let pipeline = KqlParser::parse(
            "source | where severity_text == 'Info'",
        )
        .unwrap()
        .pipeline;

        println!("pipeline raw: {pipeline}");

        let engine = ColumnarEngine::new_with_options(pipeline, ColumnarEngineOptions::new().with_diagnostic_level(ColumnarEngineDiagnosticLevel::Verbose));

        println!("pipeline modified: {}", engine.get_pipeline());

        let mut batch = engine.begin_batch().unwrap();

        batch.push_records::<OtapLogRecordBatchFactory>(batches);

        let results = batch.flush();

        assert_eq!(3, results.dropped_record_count);
        assert_eq!(1, results.included_batches.len());
        assert_eq!(1, results.included_batches[0][2].as_ref().map_or(0, |v| v.num_rows()));

        println!("{results}");
    }

    #[test]
    fn test_engine_filter_attribute() {
        let pdata: OtapPayload = OtlpProtoBytes::ExportLogsRequest(Bytes::from_static(&[
            10, 100, 10, 0, 18, 96, 18, 25, 26, 4, 73, 110, 102, 111, 50, 17, 10, 5, 97, 116, 116,
            114, 49, 18, 8, 10, 6, 118, 97, 108, 117, 101, 49, 18, 19, 50, 17, 10, 5, 97, 116, 116,
            114, 49, 18, 8, 10, 6, 118, 97, 108, 117, 101, 50, 18, 6, 26, 4, 87, 97, 114, 110, 18,
            38, 26, 4, 87, 97, 114, 110, 50, 17, 10, 5, 97, 116, 116, 114, 49, 18, 8, 10, 6, 118,
            97, 108, 117, 101, 49, 50, 11, 10, 5, 97, 116, 116, 114, 50, 18, 2, 24, 18,
        ]))
        .into();

        let otap_batch: OtapArrowRecords = pdata.try_into().unwrap();

        let logs = match otap_batch {
            OtapArrowRecords::Logs(l) => l,
            _ => panic!(),
        };

        let batches = logs.into_batches();

        assert_eq!(4, batches[2].as_ref().map_or(0, |v| v.num_rows()));

        let pipeline = KqlParser::parse(
            "source | where Attributes['attr1'] == 'value1'",
        )
        .unwrap()
        .pipeline;

        let engine = ColumnarEngine::new_with_options(pipeline, ColumnarEngineOptions::new().with_diagnostic_level(ColumnarEngineDiagnosticLevel::Verbose));

        let mut batch = engine.begin_batch().unwrap();

        batch.push_records::<OtapLogRecordBatchFactory>(batches);

        let results = batch.flush();

        assert_eq!(2, results.dropped_record_count);
        assert_eq!(1, results.included_batches.len());
        assert_eq!(2, results.included_batches[0][2].as_ref().map_or(0, |v| v.num_rows()));

        println!("{results}");
    }
}
