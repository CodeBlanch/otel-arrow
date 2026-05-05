// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::{
    execution_context::ExecutionContext, resolved_value::*, selection::select_from_record_table, *,
};

pub fn execute_source_scalar_expression<'a, 'pipeline, TRecords: ColumnarRecords>(
    execution_context: &'a ExecutionContext<'a, 'pipeline, TRecords>,
    source_scalar_expression: &'pipeline SourceScalarExpression,
) -> ResolvedScalarValue<'pipeline, 'a> {
    let record = match execution_context.get_records() {
        Some(r) => r,
        None => {
            execution_context.add_diagnostic_if_enabled(
                ColumnarEngineDiagnosticLevel::Warn,
                source_scalar_expression,
                || "Source could not be found".into(),
            );
            return ResolvedScalarValue::new_null();
        }
    };

    let key_data_type = record.get_key_data_type();

    select_from_record_table(
        execution_context,
        key_data_type,
        record,
        source_scalar_expression
            .get_value_accessor()
            .get_selectors(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::test_helpers::*;

    use super::*;

    #[test]
    fn test_select_from_source_table_using_single_string() {
        let values_dictionary = build_dictionary(
            vec![Some(0), Some(0), None, Some(1), Some(2), Some(3)],
            vec![
                ValueOrRef::String(StringValueOrRef::new_owned("hello world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodbye world".into())),
                ValueOrRef::Map(MapValueOrRef::from([(
                    "key1".into(),
                    ValueOrRef::Integer(18),
                )])),
                ValueOrRef::Integer(0),
            ],
        );

        let select_valid_key = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::String(StringScalarExpression::new(
                    QueryLocation::new_fake(),
                    "values",
                )),
            )]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Source(select_valid_key),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(values_dictionary.as_dictionary(), actual)
                }
                _ => panic!("test failure"),
            },
        );

        let select_invalid_key = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                StaticScalarExpression::String(StringScalarExpression::new(
                    QueryLocation::new_fake(),
                    "unknown",
                )),
            )]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::new()),
            ScalarExpression::Source(select_invalid_key),
            |r| {
                matches!(r, ResolvedScalarValue::Single(ValueOrRef::Null));
            },
        );

        let select_root =
            SourceScalarExpression::new(QueryLocation::new_fake(), ValueAccessor::new());

        run_scalar_expression_test(
            TestRecords::new(HashMap::new()),
            ScalarExpression::Source(select_root),
            |r| {
                matches!(r, ResolvedScalarValue::Table(_));
            },
        );

        let select_sub_key = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "values"),
                )),
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "key1"),
                )),
            ]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Source(select_sub_key),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![None, None, None, None, Some(0), None],
                            vec![ValueOrRef::Integer(18)]
                        )
                        .as_dictionary(),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let select_sub_key_invalid = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "values"),
                )),
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "invalid"),
                )),
            ]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Source(select_sub_key_invalid),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(vec![None, None, None, None, None, None], vec![])
                            .as_dictionary(),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );
    }

    #[test]
    fn test_select_from_source_table_using_single_integer() {
        let values_dictionary = build_dictionary(
            vec![Some(0), Some(0), None, Some(1)],
            vec![
                ValueOrRef::Array(ArrayValueOrRef::from([
                    ValueOrRef::Integer(0),
                    ValueOrRef::Integer(1),
                    ValueOrRef::Integer(2),
                ])),
                ValueOrRef::Integer(0),
            ],
        );

        let select_sub_index = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "values"),
                )),
                ScalarExpression::Static(StaticScalarExpression::Integer(
                    IntegerScalarExpression::new(QueryLocation::new_fake(), 0),
                )),
            ]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Source(select_sub_index),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![Some(0), Some(0), None, None],
                            vec![ValueOrRef::Integer(0)]
                        )
                        .as_dictionary(),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let select_sub_index_negative = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "values"),
                )),
                ScalarExpression::Static(StaticScalarExpression::Integer(
                    IntegerScalarExpression::new(QueryLocation::new_fake(), -1),
                )),
            ]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Source(select_sub_index_negative),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![Some(0), Some(0), None, None],
                            vec![ValueOrRef::Integer(2)]
                        )
                        .as_dictionary(),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let select_sub_index_invalid = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "values"),
                )),
                ScalarExpression::Static(StaticScalarExpression::Integer(
                    IntegerScalarExpression::new(QueryLocation::new_fake(), 100),
                )),
            ]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([(
                "values".into(),
                values_dictionary.clone(),
            )])),
            ScalarExpression::Source(select_sub_index_invalid),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(vec![None, None, None, None], vec![]).as_dictionary(),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );
    }

    #[test]
    fn test_select_from_source_table_using_dictionary() {
        let values_dictionary = build_dictionary(
            vec![
                Some(0), // string value: hello world
                Some(0), // string value: hello world
                None,
                Some(1), // string value: goodbye world
                Some(2), // map value
                Some(2), // map value
                Some(3), // array value
                Some(3), // array value
                Some(4), // integer value
            ],
            vec![
                ValueOrRef::String(StringValueOrRef::new_owned("hello world".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("goodebye world".into())),
                ValueOrRef::Map(MapValueOrRef::from([
                    (
                        "key1".into(),
                        ValueOrRef::String(StringValueOrRef::new_owned("value1".into())),
                    ),
                    (
                        "key2".into(),
                        ValueOrRef::String(StringValueOrRef::new_owned("value2".into())),
                    ),
                ])),
                ValueOrRef::Array(ArrayValueOrRef::from([
                    ValueOrRef::Integer(0),
                    ValueOrRef::Integer(1),
                    ValueOrRef::Integer(2),
                ])),
                ValueOrRef::Integer(0),
            ],
        );

        let keys_dictionary = build_dictionary(
            vec![
                None,
                None,
                None,
                None,
                Some(0), // Should eval as map['key1']
                Some(1), // Should eval as map['key2']
                None,
                None,
                None,
            ],
            vec![
                ValueOrRef::String(StringValueOrRef::new_owned("key1".into())),
                ValueOrRef::String(StringValueOrRef::new_owned("key2".into())),
            ],
        );

        let indicies_dictionary = build_dictionary(
            vec![
                None,
                None,
                None,
                None,
                None,
                None,
                Some(0), // Should eval as array[0]
                Some(1), // Should eval as array[-1]
                None,
            ],
            vec![ValueOrRef::Integer(0), ValueOrRef::Integer(-1)],
        );

        let select_sub_key = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "values"),
                )),
                ScalarExpression::Source(SourceScalarExpression::new(
                    QueryLocation::new_fake(),
                    ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                        StaticScalarExpression::String(StringScalarExpression::new(
                            QueryLocation::new_fake(),
                            "keys",
                        )),
                    )]),
                )),
            ]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([
                ("values".into(), values_dictionary.clone()),
                ("keys".into(), keys_dictionary.clone()),
            ])),
            ScalarExpression::Source(select_sub_key),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![
                                None,
                                None,
                                None,
                                None,
                                Some(0), // Should eval as map['key1']
                                Some(1), // Should eval as map['key2']
                                None,
                                None,
                                None,
                            ],
                            vec![
                                ValueOrRef::String(StringValueOrRef::new_owned("value1".into())),
                                ValueOrRef::String(StringValueOrRef::new_owned("value2".into())),
                            ],
                        )
                        .as_dictionary(),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );

        let select_sub_element = SourceScalarExpression::new(
            QueryLocation::new_fake(),
            ValueAccessor::new_with_selectors(vec![
                ScalarExpression::Static(StaticScalarExpression::String(
                    StringScalarExpression::new(QueryLocation::new_fake(), "values"),
                )),
                ScalarExpression::Source(SourceScalarExpression::new(
                    QueryLocation::new_fake(),
                    ValueAccessor::new_with_selectors(vec![ScalarExpression::Static(
                        StaticScalarExpression::String(StringScalarExpression::new(
                            QueryLocation::new_fake(),
                            "indicies",
                        )),
                    )]),
                )),
            ]),
        );

        run_scalar_expression_test(
            TestRecords::new(HashMap::from([
                ("values".into(), values_dictionary.clone()),
                ("indicies".into(), indicies_dictionary.clone()),
            ])),
            ScalarExpression::Source(select_sub_element),
            |r| match r {
                ResolvedScalarValue::Dictionary(actual) => {
                    assert_eq!(
                        build_dictionary(
                            vec![
                                None,
                                None,
                                None,
                                None,
                                None,
                                None,
                                Some(0), // Should eval as array[0]
                                Some(1), // Should eval as array[-1]
                                None,
                            ],
                            vec![ValueOrRef::Integer(0), ValueOrRef::Integer(2),],
                        )
                        .as_dictionary(),
                        actual
                    );
                }
                _ => panic!("test failure"),
            },
        );
    }
}
