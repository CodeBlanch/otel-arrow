// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use criterion::{Criterion, criterion_group, criterion_main};

mod common;

use crate::common::bench_log_pipeline;

fn bench_filter_pipelines(c: &mut Criterion) {
    let batch_sizes = [/*32, 1024, */ 8192];

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
    );
    bench_log_pipeline(
        c,
        &batch_sizes,
        "and_attrs_short_circuit",
        // left expr of "and" should always return false for all rows
        "source | where attributes[\"code.line.number\"] > 1000 and attributes[\"code.line.number\"] == 2",
    );
    bench_log_pipeline(
        c,
        &batch_sizes,
        "and_short_circuit",
        // left expr of "and" should be false for all rows
        //
        // this is different from the case above in that the "and" here is currently something that
        // won't get optimized into a Composite<AttributeFilterExec> so we can test the fast path
        // in Composite<FilterExec>
        "source | where severity_text == \"invalid value\" and attributes[\"code.line.number\"] == 2",
    );*/
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
