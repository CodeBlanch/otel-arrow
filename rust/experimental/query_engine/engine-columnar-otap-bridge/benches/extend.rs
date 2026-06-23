// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use criterion::{Criterion, criterion_group, criterion_main};

mod common;

use crate::common::bench_log_pipeline;

fn bench_extend_pipelines(c: &mut Criterion) {
    let batch_sizes = [128, 1536, 8192];

    // Upsert a string key that does NOT exist on any log (pure insert path).
    // Every parent gets a new attribute row appended.
    bench_log_pipeline(
        c,
        &batch_sizes,
        "upsert_new_str_key",
        r#"logs | extend attributes["new_key"] = "new_val""#,
    );

    // Upsert a string key that DOES exist on every log (pure update path).
    // "code.namespace" is present on every log record in the fixture.
    bench_log_pipeline(
        c,
        &batch_sizes,
        "upsert_existing_str_key",
        r#"logs | extend attributes["code.namespace"] = "updated""#,
    );

    // Execute two attribute assignments at once. The planner should fuse these operations into
    // a single assignment pipeline stage, which should run faster than two sequential stages.
    // the time of these benchmark cases should be less than twice the previous two
    bench_log_pipeline(
        c,
        &batch_sizes,
        "upsert_two_new_str_keys",
        r#"logs | extend attributes["new_key1"] = "val1", attributes["new_key2"] = "val2""#,
    );

    bench_log_pipeline(
        c,
        &batch_sizes,
        "upsert_two_existing_str_keys",
        r#"logs | extend attributes["code.namespace"] = "hello", attributes["code.function.name"] = "world""#,
    );

    // mix of insert and upsert
    bench_log_pipeline(
        c,
        &batch_sizes,
        "upsert_two_existing_one_new_str_keys",
        r#"logs |
            extend attributes["code.namespace"] = "hello",
            attributes["code.function.name"] = "world",
            attributes["new_key2"] = "val2"
        "#,
    );
}

mod benches {
    use super::*;

    criterion_group!(
        name = benches;
        config = Criterion::default();
        targets = bench_extend_pipelines
    );
}

criterion_main!(benches::benches);
