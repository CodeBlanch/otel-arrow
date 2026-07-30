// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{cell::RefCell, collections::HashMap, fmt::Display, sync::Arc};

use ahash::AHashMap;
use arrow::{array::*, compute::kernels::filter, datatypes::*};
use data_engine_expressions::*;
use roaring::RoaringBitmap;

use crate::{execution_context::*, resolved_value::*, scalars::execute_scalar_expression, *};

#[derive(Default)]
pub(crate) struct ExecutionContextState<'pipeline> {
    pipeline: Option<&'pipeline PipelineExpression>,
    records: Option<TestRecords<'pipeline>>,
    global_variables: AHashMap<Box<str>, ResolvedSingleOrDictionaryValue<'pipeline>>,
}

impl<'pipeline> ExecutionContextState<'pipeline> {
    pub fn new() -> ExecutionContextState<'pipeline> {
        Default::default()
    }

    pub fn with_pipeline(mut self, pipeline: &'pipeline PipelineExpression) -> Self {
        self.pipeline = Some(pipeline);
        self
    }

    pub fn with_records(mut self, records: TestRecords<'pipeline>) -> Self {
        self.records = Some(records);
        self
    }

    pub fn with_global_variables(
        mut self,
        global_variables: AHashMap<Box<str>, ResolvedSingleOrDictionaryValue<'pipeline>>,
    ) -> Self {
        self.global_variables = global_variables;
        self
    }
}

pub(crate) fn run_scalar_expression_test<FValidate>(
    records: TestRecords,
    expression: ScalarExpression,
    validate: FValidate,
) where
    for<'a, 'b> FValidate: FnOnce(ResolvedScalarValue<'a, 'b>),
{
    run_scalar_expression_test_with_state(
        ExecutionContextState::new().with_records(records),
        expression,
        validate,
    )
}

pub(crate) fn run_scalar_expression_test_with_state<FValidate>(
    state: ExecutionContextState<'_>,
    expression: ScalarExpression,
    validate: FValidate,
) where
    for<'a, 'b> FValidate: FnOnce(ResolvedScalarValue<'a, 'b>),
{
    let d = RefCell::new(vec![]);

    let mut local_pipeline = None;

    let p = state.pipeline.unwrap_or_else(|| {
        local_pipeline = Some(Default::default());
        local_pipeline.as_ref().expect("has pipeline")
    });

    let global_variables = RefCell::new(state.global_variables);

    let ec = ExecutionContext::new(
        ColumnarEngineDiagnosticLevel::Verbose,
        &d,
        p,
        &global_variables,
        state.records,
    );

    let result = execute_scalar_expression(&ec, &expression);

    println!("{ec}");

    validate(result)
}

pub(crate) fn build_dictionary(
    keys: Vec<Option<u16>>,
    values: Vec<ValueOrRef<'static>>,
) -> Dictionary<'static> {
    let mut key_builder = PrimitiveBuilder::<UInt16Type>::new();

    for key in keys {
        match key {
            None => key_builder.append_null(),
            Some(k) => key_builder.append_value(k),
        }
    }

    let keys = key_builder.finish();

    Dictionary::new(keys.into(), DictionaryValueArray::Vec(values.into()))
}

pub(crate) struct TestRecordsFactory {}

impl ColumnarRecordsFactory<2> for TestRecordsFactory {
    type Records<'pipeline, 'record> = TestRecords<'pipeline>;
    type State<'pipeline> = TestRecords<'pipeline>;

    fn create<'pipeline, 'record>(
        &self,
        _state: Option<Self::State<'pipeline>>,
        batches: &'record [Option<RecordBatch>; 2],
    ) -> Self::Records<'pipeline, 'record> {
        TestRecords::from_batches(batches)
    }

    fn filter<'pipeline>(
        &self,
        _state: &mut Self::State<'pipeline>,
        batches: &mut [Option<RecordBatch>; 2],
        filter: &BooleanArray,
    ) {
        if let Some(records) = &batches[0] {
            batches[0] = Some(filter::filter_record_batch(records, filter).unwrap());

            if let Some(attached_records) = &batches[1] {
                batches[1] = Some(filter::filter_record_batch(attached_records, filter).unwrap());
            }

            return;
        }

        batches[1] = None;
    }

    fn apply<'pipeline, T: ColumnarEngineDiagnosticReceiver<'pipeline>>(
        &self,
        _diagnostic_receiver: &T,
        _expression: &'pipeline dyn Expression,
        state: &mut Self::State<'pipeline>,
        batches: &mut [Option<RecordBatch>; 2],
    ) {
        *batches = state.into_batches();
    }
}

#[derive(Debug)]
pub(crate) struct TestRecords<'pipeline> {
    ids: Option<PrimitiveArray<Int64Type>>,
    values: Option<HashMap<Box<str>, Dictionary<'pipeline>>>,
    attached_records: Option<HashMap<Box<str>, TestRecords<'pipeline>>>,
}

impl<'pipeline> TestRecords<'pipeline> {
    pub fn new() -> TestRecords<'pipeline> {
        Self {
            ids: None,
            values: None,
            attached_records: None,
        }
    }

    pub fn with_ids(mut self, ids: PrimitiveArray<Int64Type>) -> Self {
        self.ids = Some(ids);
        self
    }

    pub fn with_values(mut self, values: HashMap<Box<str>, Dictionary<'pipeline>>) -> Self {
        self.values = Some(values);
        self
    }

    pub fn with_attached_records(
        mut self,
        attached_records: HashMap<Box<str>, TestRecords<'pipeline>>,
    ) -> Self {
        self.attached_records = Some(attached_records);
        self
    }

    pub fn from_batches(batches: &[Option<RecordBatch>; 2]) -> Self {
        let mut state = TestRecords::new();

        if let Some(records) = &batches[0] {
            let mut values: HashMap<Box<str>, Dictionary> = HashMap::new();

            for (id, field) in records.schema_ref().fields().iter().enumerate() {
                if field.name() == "ids" {
                    state = state.with_ids(records.column(id).as_primitive::<Int64Type>().clone())
                } else {
                    let d = records
                        .column(id)
                        .as_dictionary::<UInt16Type>()
                        .downcast_dict::<StringArray>()
                        .expect("string dict");

                    values.insert(field.name().as_str().into(), d.into());
                }
            }

            if !values.is_empty() {
                state = state.with_values(values);
            }
        }

        if let Some(attached_records) = &batches[1] {
            let mut values: HashMap<Box<str>, TestRecords> = HashMap::new();

            for (id, field) in attached_records.schema_ref().fields().iter().enumerate() {
                let name = field.name();

                let s = attached_records.column(id).as_struct();

                let mut field_values: HashMap<Box<str>, Dictionary> = HashMap::new();

                for (id, field) in s.fields().iter().enumerate() {
                    let v = s
                        .column(id)
                        .as_dictionary::<UInt16Type>()
                        .downcast_dict::<StringArray>()
                        .expect("has strings");

                    field_values.insert(field.name().as_str().into(), v.into());
                }

                if !field_values.is_empty() {
                    values.insert(
                        name.as_str().into(),
                        TestRecords::new().with_values(field_values),
                    );
                }
            }

            if !values.is_empty() {
                state = state.with_attached_records(values);
            }
        }

        state
    }

    pub fn into_batches(&mut self) -> [Option<RecordBatch>; 2] {
        let mut schema = SchemaBuilder::new();
        let mut columns: Vec<ArrayRef> = vec![];

        if let Some(ids) = self.ids.take() {
            schema.push(Field::new("ids", DataType::Int64, false));

            columns.push(Arc::new(ids));
        }

        if let Some(values) = self.values.take() {
            for (key, value) in values {
                let (keys, values) = value.into_parts();

                let (transformed_values, lookup) = values.into_string_array();

                let array = Arc::new(DictionaryArray::<UInt16Type>::new(
                    keys.into_key_array(lookup),
                    Arc::new(transformed_values),
                ));

                schema.push(Field::new(key, array.data_type().clone(), true));

                columns.push(array);
            }
        }

        if columns.is_empty() {
            return [None, None];
        }

        let records = RecordBatch::try_new(schema.finish().into(), columns).expect("valid batch");

        if let Some(attached_records) = self.attached_records.take() {
            let mut schema = SchemaBuilder::new();
            let mut columns: Vec<ArrayRef> = vec![];

            for (key, mut value) in attached_records {
                let mut struct_fields = vec![];
                let mut struct_columns = vec![];

                if let Some(values) = value.values.take() {
                    for (key, value) in values {
                        let (keys, values) = value.into_parts();

                        let (transformed_values, lookup) = values.into_string_array();

                        let array: Arc<dyn Array> = Arc::new(DictionaryArray::<UInt16Type>::new(
                            keys.into_key_array(lookup),
                            Arc::new(transformed_values),
                        ));

                        struct_fields.push(Field::new(key, array.data_type().clone(), true));
                        struct_columns.push(array);
                    }
                }

                let struct_fields: Fields = struct_fields.into();

                let array = Arc::new(
                    StructArray::try_new(struct_fields.clone(), struct_columns, None).unwrap(),
                );

                schema.push(Field::new(key, DataType::Struct(struct_fields), true));
                columns.push(array);
            }

            [
                Some(records),
                Some(RecordBatch::try_new(schema.finish().into(), columns).expect("valid batch")),
            ]
        } else {
            [Some(records), None]
        }
    }
}

impl From<TestRecords<'_>> for [Option<RecordBatch>; 2] {
    fn from(mut value: TestRecords<'_>) -> Self {
        value.into_batches()
    }
}

impl<'pipeline> ColumnarRecords<'pipeline> for TestRecords<'pipeline> {
    fn get_diagnostic_level(&self) -> Option<ColumnarEngineDiagnosticLevel> {
        None
    }

    fn get_key_data_type(&self) -> DataType {
        DataType::UInt16
    }

    fn len(&self) -> usize {
        if let Some(ids) = self.ids.as_ref() {
            ids.len()
        } else if let Some(values) = self.values.as_ref()
            && !values.is_empty()
        {
            values.iter().next().expect("has value").1.len()
        } else {
            0
        }
    }

    fn get_attached_records(&self, name: &str) -> Option<&dyn RecordTable<'pipeline>> {
        self.attached_records
            .as_ref()
            .and_then(|a| a.get(name).map(|v| v as &dyn RecordTable))
    }

    fn set_values<T: ColumnarEngineDiagnosticReceiver<'pipeline>>(
        &mut self,
        diagnostic_receiver: &T,
        _expression: &'pipeline dyn Expression,
        root: &ColumnarEngineSelectionPath<'pipeline>,
        path: &[ColumnarEngineSelectionPath<'pipeline>],
        key_filter: Option<&RoaringBitmap>,
        values: Dictionary<'pipeline>,
    ) -> ColumnarRecordsWriteResult {
        if key_filter.is_some_and(|v| v.is_empty()) {
            return ColumnarRecordsWriteResult::Success;
        }

        let path_length = path.len();

        match root {
            ColumnarEngineSelectionPath::Key {
                expression: key_expression,
                value: root_key,
            } => {
                match root_key.get_value() {
                    "ids" => {
                        if path_length > 0 {
                            diagnostic_receiver.add_diagnostic_if_enabled(
                                ColumnarEngineDiagnosticLevel::Warn,
                                *key_expression,
                                || format!("Cannot access into field 'ids'"),
                            );
                            return ColumnarRecordsWriteResult::NotFound;
                        }

                        let value = if let Some(key_filter) = key_filter {
                            let existing_values = match self.ids.as_ref() {
                                Some(v) => Dictionary::from_array::<UInt16Type, _>(v),
                                None => {
                                    let key_length = self.len();
                                    Dictionary::new_null_with_data_type(
                                        key_length,
                                        DataType::UInt16,
                                    )
                                }
                            };

                            existing_values.with_values(Some(key_filter), &values)
                        } else {
                            values.clone()
                        };

                        if value.is_null() {
                            self.ids = None;
                        } else {
                            self.ids = Some(values.transform_into_primitive(
                                DictionaryValueArray::into_int_array::<Int64Type>,
                            ));
                        }
                    }
                    key => {
                        let key_length = self.len();

                        let attribute_values = self.values.get_or_insert_with(|| HashMap::new());

                        let value = if path_length > 0 {
                            match attribute_values.remove(key) {
                                None => {
                                    diagnostic_receiver.add_diagnostic_if_enabled(
                                        ColumnarEngineDiagnosticLevel::Warn,
                                        *key_expression,
                                        || format!("Cannot access into empty '{key}'"),
                                    );
                                    return ColumnarRecordsWriteResult::NotFound;
                                }
                                Some(v) => v.with_values_and_path_typed(
                                    diagnostic_receiver,
                                    key_filter,
                                    path,
                                    &values,
                                ),
                            }
                        } else if let Some(key_filter) = key_filter {
                            let existing_values = match attribute_values.remove(key) {
                                Some(v) => v,
                                None => Dictionary::new_null_with_data_type(
                                    key_length,
                                    DataType::UInt16,
                                ),
                            };

                            existing_values.with_values(Some(key_filter), &values)
                        } else {
                            values.clone()
                        };

                        if !value.is_null() {
                            attribute_values.insert(key.into(), value);
                        }
                    }
                }

                ColumnarRecordsWriteResult::Success
            }
            ColumnarEngineSelectionPath::Dictionary {
                expression: _,
                value: _,
            } => {
                todo!()
            }
            ColumnarEngineSelectionPath::Index {
                expression,
                value: _,
            } => {
                diagnostic_receiver.add_diagnostic_if_enabled(
                    ColumnarEngineDiagnosticLevel::Warn,
                    *expression,
                    || "Test record cannot be accessed by array index".into(),
                );
                ColumnarRecordsWriteResult::NotFound
            }
        }
    }
}

impl<'pipeline> RecordTable<'pipeline> for TestRecords<'pipeline> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'pipeline, '_>> {
        self.values
            .as_ref()
            .and_then(|v| v.get(key).map(|v| RecordTableValue::Dictionary(v.clone())))
    }
}

impl Display for TestRecords<'_> {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
