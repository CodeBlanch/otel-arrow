// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use data_engine_columnar::ColumnarEngineDiagnosticLevel;
use data_engine_columnar_otap_bridge::*;
use otap_df_pdata::OtapArrowRecords;
use otap_df_pdata::otap::Logs;
use otap_df_pdata::proto::OtlpProtoMessage;
use otap_df_pdata::testing::fixtures::logs_with_varying_attributes_and_properties;
use otap_df_pdata::testing::round_trip::otlp_to_otap;

fn generate_logs_batch(batch_size: usize) -> Logs {
    let logs_data = logs_with_varying_attributes_and_properties(batch_size);
    let pdata = otlp_to_otap(&OtlpProtoMessage::Logs(logs_data));
    match pdata {
        OtapArrowRecords::Logs(logs) => logs,
        _ => panic!(),
    }
}

fn bench_log_pipeline(
    c: &mut Criterion,
    batch_sizes: &[usize],
    bench_group_name: &str,
    bench_pipeline_kql: &str,
) {
    let mut group = c.benchmark_group(bench_group_name);
    for batch_size in batch_sizes {
        let benchmark_id = BenchmarkId::new("batch_size", batch_size);
        let _ = group.bench_with_input(benchmark_id, batch_size, |b, batch_size| {
            let batch = generate_logs_batch(*batch_size);
            let pipeline = parse_kql_logs_query_into_pipeline(bench_pipeline_kql, None)
                .expect("can parse pipeline");
            let mut factory = OtapLogRecordBatchFactory::new_with_options(Some(
                ColumnarEngineDiagnosticLevel::Warn,
            ));
            b.iter_with_setup(
                || batch.clone(),
                |batch| {
                    process_otap_logs_using_pipeline(&pipeline, &mut factory, batch)
                        .expect("doesn't fail")
                },
            );
        });
    }
    group.finish();
}

fn bench_filter_pipelines(c: &mut Criterion) {
    let batch_sizes = [/*32, 1024,*/ 8192];

    /*bench_log_pipeline(
        c,
        &batch_sizes,
        "simple_field_filter",
        "source | where severity_text == \"WARN\"",
    );
    bench_log_pipeline(
        c,
        &batch_sizes,
        "simple_attr_filter",
        "source | where attributes[\"code.namespace\"] == \"main\"",
    );
    bench_log_pipeline(
        c,
        &batch_sizes,
        "attr_or_attr_filter",
        "source | where attributes[\"code.namespace\"] == \"main\" or attributes[\"code.line.number\"] == 2",
    );
    bench_log_pipeline(
        c,
        &batch_sizes,
        "attr_and_prop_filter",
        "source | where attributes[\"code.namespace\"] == \"main\" and severity_text == \"WARN\"",
    );
    bench_log_pipeline(
        c,
        &batch_sizes,
        "attr_and_attr_filter",
        "source | where attributes[\"code.namespace\"] == \"main\" and attributes[\"code.line\"] == 2",
    );
    bench_log_pipeline(
        c,
        &batch_sizes,
        "attr_and_or_together_filter",
        "source | where
            (attributes[\"code.namespace\"] == \"main\" and attributes[\"code.line\"] == 2)
            or
            (attributes[\"code.namespace\"] == \"otap_dataflow_engine\" and attributes[\"code.line.number\"] == 3)",
    );*/
    /*bench_log_pipeline(
        c,
        &batch_sizes,
        "and_attrs_short_circuit",
        // left expr of "and" should always return false for all rows
        "source | where attributes[\"code.line.number\"] > 1000 and attributes[\"code.line.number\"] == 2",
        //"source | where attributes[\"code.line.number\"] < 1000 and attributes[\"code.line.number\"] == 2",
    );*/
    /*bench_log_pipeline(
        c,
        &batch_sizes,
        "and_short_circuit",
        // left expr of "and" should be false for all rows
        //
        // this is different from the case above in that the "and" here is currently something that
        // won't get optimized into a Composite<AttributeFilterExec> so we can test the fast path
        // in Composite<FilterExec>
        "source | where severity_text == \"invalid value\" and attributes[\"code.line.number\"] == 2",
    );
    bench_log_pipeline(
        c,
        &batch_sizes,
        "or_short_circuit",
        // left expr of "or" should be true for all rows
        //
        // this is different from the case above in that the "and" here is currently something that
        // won't get optimized into a Composite<AttributeFilterExec> so we can test the fast path
        // in Composite<FilterExec>
        "source | where attributes[\"code.line.number\"] >= 0 or not(attributes[\"some.attr\"] >= 0 and severity_text == \"WARN\")",
    );*/
    bench_log_pipeline(
        c,
        &batch_sizes,
        "len()",
        // left expr of "and" should always return false for all rows
        "source | where strlen(severity_text) >= 4",
        //"source | where attributes[\"code.line.number\"] < 1000 and attributes[\"code.line.number\"] == 2",
    );
}

mod benches {
    use super::*;

    criterion_group!(
        name = benches;
        config = Criterion::default();
        targets = bench_filter_pipelines
    );
}

criterion_main!(benches::benches);
