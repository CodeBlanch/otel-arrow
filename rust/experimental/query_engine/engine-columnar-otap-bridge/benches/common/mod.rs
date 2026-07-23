// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use criterion::{BenchmarkId, Criterion};
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

pub fn bench_log_pipeline(
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
            let factory = OtapLogRecordBatchFactory::new_with_options(Some(
                ColumnarEngineDiagnosticLevel::Warn,
            ));
            b.iter_with_setup(
                || batch.clone(),
                |batch| process_otap_logs_using_pipeline(&pipeline, &factory, batch),
            );
        });
    }
    group.finish();
}
