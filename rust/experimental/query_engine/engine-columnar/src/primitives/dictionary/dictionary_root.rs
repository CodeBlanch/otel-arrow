// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::hash_map::Entry,
    fmt::{Display, Write},
    ops::Deref,
    rc::Rc,
};

use ahash::AHashMap;
use arrow::{array::*, buffer::*, datatypes::*};
use data_engine_expressions::*;
use roaring::RoaringBitmap;

use crate::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Dictionary<'a> {
    keys: DictionaryKeyArray,
    values: DictionaryValueArray<'a>,
}

impl Dictionary<'_> {
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn is_null(&self) -> bool {
        self.keys.is_null() || self.values.is_null()
    }

    pub fn nulls(&self) -> Option<NullBuffer> {
        let key_nulls = self.keys.nulls();

        if let Some(v) = key_nulls.as_ref()
            && v.null_count() == self.keys.len()
        {
            return key_nulls;
        }

        if let Some(value_nulls) = self.values.nulls()
            && value_nulls.null_count() > 0
        {
            let key_length = self.keys.len();

            let mut builder: BooleanBufferBuilder = key_nulls.map_or_else(
                || {
                    let mut buffer = MutableBuffer::new_null(key_length);
                    buffer.fill(0xFF);
                    BooleanBufferBuilder::new_from_buffer(buffer, key_length)
                },
                |v| {
                    let mut b = MutableBuffer::with_capacity(v.len());
                    b.extend_from_slice(v.validity());
                    BooleanBufferBuilder::new_from_buffer(b, v.len())
                },
            );

            for key_index in 0..key_length {
                if let Some(value_index) = self.keys.get_value_index_for_key_index(key_index)
                    && value_nulls.is_null(value_index)
                {
                    builder.set_bit(key_index, false);
                }
            }

            return Some(NullBuffer::new(builder.into()));
        }

        key_nulls
    }

    pub fn keys(&self) -> &DictionaryKeyArray {
        &self.keys
    }

    pub fn get_value_index(&self, key_index: usize) -> Option<usize> {
        self.keys.get_value_index_for_key_index(key_index)
    }
}

impl<'a> Dictionary<'a> {
    pub fn new(keys: DictionaryKeyArray, values: DictionaryValueArray<'a>) -> Dictionary<'a> {
        Self { keys, values }
    }

    pub fn from_array<K: ArrowDictionaryKeyType, V: ArrowPrimitiveType>(
        values: &PrimitiveArray<V>,
    ) -> Dictionary<'a> {
        Self {
            keys: DictionaryKeyArray::UniqueValues {
                data_type: K::DATA_TYPE,
                length: values.len(),
            },
            values: (values as &dyn Array).into(),
        }
    }

    pub fn new_scalar_with_data_type(
        key_data_type: DataType,
        key_count: usize,
        value: ValueOrRef<'a>,
    ) -> Dictionary<'a> {
        match key_data_type {
            DataType::Int8 => Self::new_scalar::<Int8Type>(key_count, value),
            DataType::Int16 => Self::new_scalar::<Int16Type>(key_count, value),
            DataType::Int32 => Self::new_scalar::<Int32Type>(key_count, value),
            DataType::Int64 => Self::new_scalar::<Int64Type>(key_count, value),

            DataType::UInt8 => Self::new_scalar::<UInt8Type>(key_count, value),
            DataType::UInt16 => Self::new_scalar::<UInt16Type>(key_count, value),
            DataType::UInt32 => Self::new_scalar::<UInt32Type>(key_count, value),
            DataType::UInt64 => Self::new_scalar::<UInt64Type>(key_count, value),

            d => panic!("Unexpected dictionary key type '{d}' encountered"),
        }
    }

    pub fn new_scalar<K: ArrowDictionaryKeyType>(
        key_count: usize,
        value: ValueOrRef<'a>,
    ) -> Dictionary<'a> {
        Dictionary::new(
            DictionaryKeyArray::SingleValue {
                data_type: K::DATA_TYPE,
                length: key_count,
                value_index: Some(0),
            },
            vec![value].into(),
        )
    }

    pub fn new_null_with_data_type(count: usize, data_type: DataType) -> Dictionary<'a> {
        match data_type {
            DataType::Int8 => Self::new_null::<Int8Type>(count),
            DataType::Int16 => Self::new_null::<Int16Type>(count),
            DataType::Int32 => Self::new_null::<Int32Type>(count),
            DataType::Int64 => Self::new_null::<Int64Type>(count),

            DataType::UInt8 => Self::new_null::<UInt8Type>(count),
            DataType::UInt16 => Self::new_null::<UInt16Type>(count),
            DataType::UInt32 => Self::new_null::<UInt32Type>(count),
            DataType::UInt64 => Self::new_null::<UInt64Type>(count),

            d => panic!("Unexpected dictionary key type '{d}' encountered"),
        }
    }

    pub fn new_null<K: ArrowDictionaryKeyType>(count: usize) -> Dictionary<'a> {
        Dictionary::new(
            DictionaryKeyArray::SingleValue {
                data_type: K::DATA_TYPE,
                length: count,
                value_index: None,
            },
            vec![].into(),
        )
    }

    pub fn values(&self) -> &DictionaryValueArray<'a> {
        &self.values
    }

    pub fn into_parts(self) -> (DictionaryKeyArray, DictionaryValueArray<'a>) {
        (self.keys, self.values)
    }

    pub fn get_value(&self, key_index: usize) -> ValueOrRef<'a> {
        if let Some(value_index) = self.get_value_index(key_index) {
            return self.values.get_value_at(value_index);
        }

        ValueOrRef::Null
    }

    pub fn with_values(
        self,
        key_filter: Option<&RoaringBitmap>,
        values: &Dictionary<'a>,
    ) -> Dictionary<'a> {
        let (source_keys, source_values) = self.into_parts();

        match source_keys.data_type() {
            DataType::UInt8 => {
                with_values_typed::<UInt8Type>(source_keys, source_values, key_filter, values)
            }
            DataType::UInt16 => {
                with_values_typed::<UInt16Type>(source_keys, source_values, key_filter, values)
            }
            DataType::UInt32 => {
                with_values_typed::<UInt32Type>(source_keys, source_values, key_filter, values)
            }
            DataType::UInt64 => {
                with_values_typed::<UInt64Type>(source_keys, source_values, key_filter, values)
            }

            DataType::Int8 => {
                with_values_typed::<Int8Type>(source_keys, source_values, key_filter, values)
            }
            DataType::Int16 => {
                with_values_typed::<Int16Type>(source_keys, source_values, key_filter, values)
            }
            DataType::Int32 => {
                with_values_typed::<Int32Type>(source_keys, source_values, key_filter, values)
            }
            DataType::Int64 => {
                with_values_typed::<Int64Type>(source_keys, source_values, key_filter, values)
            }

            d => panic!("Key type '{d}' is not supported"),
        }
    }

    pub fn with_values_and_path_typed<TDiagnostic: ColumnarEngineDiagnosticReceiver<'a>>(
        self,
        diagnostic_receiver: &TDiagnostic,
        key_filter: Option<&RoaringBitmap>,
        path: &[ColumnarEngineSelectionPath<'a>],
        values: &Dictionary<'a>,
    ) -> Dictionary<'a> {
        let (source_keys, source_values) = self.into_parts();

        match source_keys.data_type() {
            DataType::UInt8 => with_values_and_path_typed::<UInt8Type, _>(
                diagnostic_receiver,
                source_keys,
                source_values,
                key_filter,
                path,
                values,
            ),
            DataType::UInt16 => with_values_and_path_typed::<UInt16Type, _>(
                diagnostic_receiver,
                source_keys,
                source_values,
                key_filter,
                path,
                values,
            ),
            DataType::UInt32 => with_values_and_path_typed::<UInt32Type, _>(
                diagnostic_receiver,
                source_keys,
                source_values,
                key_filter,
                path,
                values,
            ),
            DataType::UInt64 => with_values_and_path_typed::<UInt64Type, _>(
                diagnostic_receiver,
                source_keys,
                source_values,
                key_filter,
                path,
                values,
            ),

            DataType::Int8 => with_values_and_path_typed::<Int8Type, _>(
                diagnostic_receiver,
                source_keys,
                source_values,
                key_filter,
                path,
                values,
            ),
            DataType::Int16 => with_values_and_path_typed::<Int16Type, _>(
                diagnostic_receiver,
                source_keys,
                source_values,
                key_filter,
                path,
                values,
            ),
            DataType::Int32 => with_values_and_path_typed::<Int32Type, _>(
                diagnostic_receiver,
                source_keys,
                source_values,
                key_filter,
                path,
                values,
            ),
            DataType::Int64 => with_values_and_path_typed::<Int64Type, _>(
                diagnostic_receiver,
                source_keys,
                source_values,
                key_filter,
                path,
                values,
            ),

            d => panic!("Key type '{d}' is not supported"),
        }
    }
}

impl Display for Dictionary<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('{')?;
        for key in 0..self.keys.len() {
            if key > 0 {
                f.write_char(',')?;
            }
            write!(f, "{key}:")?;
            fmt_value(self.get_value(key).to_value(), f)?;
        }
        f.write_char('}')
    }
}

impl<'a, T: ArrowDictionaryKeyType> From<&DictionaryArray<T>> for Dictionary<'a> {
    fn from(value: &DictionaryArray<T>) -> Self {
        Dictionary {
            keys: value.keys().into(),
            values: (value.values() as &dyn Array).into(),
        }
    }
}

impl<'a, 'b, K: ArrowDictionaryKeyType, V> From<TypedDictionaryArray<'b, K, V>> for Dictionary<'a>
where
    DictionaryValueArray<'a>: From<&'b V>,
{
    fn from(value: TypedDictionaryArray<'b, K, V>) -> Self {
        Dictionary {
            keys: value.keys().into(),
            values: value.values().into(),
        }
    }
}

fn with_values_typed<'a, K: ArrowDictionaryKeyType>(
    source_keys: DictionaryKeyArray,
    source_values: DictionaryValueArray<'a>,
    key_filter: Option<&RoaringBitmap>,
    values: &Dictionary<'a>,
) -> Dictionary<'a> {
    if key_filter.is_none()
        && let DictionaryKeyArray::SingleValue {
            data_type,
            length,
            value_index,
        } = values.keys()
    {
        return match value_index {
            Some(value_index) => {
                let v = values.values().get_value_at(*value_index);
                if matches!(v, ValueOrRef::Null) {
                    Dictionary::new_null_with_data_type(*length, data_type.clone())
                } else {
                    Dictionary::new_scalar_with_data_type(data_type.clone(), *length, v)
                }
            }
            None => Dictionary::new_null_with_data_type(*length, data_type.clone()),
        };
    }

    let (mut source_values, lookup) = source_values.into_set();

    let mut source_key_builder = source_keys.into_key_builder::<K>(lookup);

    let key_length = source_key_builder.get_key_length();
    let mut source_key_writer = source_key_builder.get_writer();

    let mut visited_values = AHashMap::with_capacity(source_values.len());

    match key_filter {
        None => {
            for key_index in 0..key_length {
                write_value(
                    values,
                    &mut source_values,
                    &mut source_key_writer,
                    &mut visited_values,
                    key_index,
                );
            }
        }
        Some(key_filter) => {
            for key_index in key_filter {
                write_value(
                    values,
                    &mut source_values,
                    &mut source_key_writer,
                    &mut visited_values,
                    key_index as usize,
                );
            }
        }
    }

    if source_values.is_empty() {
        return Dictionary::new_null::<K>(key_length);
    } else if source_values.len() == 1 && !source_key_builder.has_nulls() {
        return Dictionary::new_scalar::<K>(
            key_length,
            source_values.into_iter().next().expect("has value"),
        );
    }

    Dictionary::new(source_key_builder.finish().into(), source_values.into())
}

fn write_value<'a, K: ArrowDictionaryKeyType>(
    values: &Dictionary<'a>,
    source_values: &mut ValueOrRefSet<'a>,
    source_key_writer: &mut DictionaryKeyArrayWriter<'_, K>,
    visited_values: &mut AHashMap<usize, Option<usize>>,
    key_index: usize,
) {
    let value_index =
        match visited_values.entry(values.get_value_index(key_index).unwrap_or(usize::MAX)) {
            Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
            Entry::Vacant(vacant_entry) => {
                let value = values.values().get_value_at(*vacant_entry.key());

                let value_index = if matches!(value, ValueOrRef::Null) {
                    None
                } else {
                    let (value_index, _) = source_values.insert_full(value);
                    Some(value_index)
                };

                vacant_entry.insert(value_index)
            }
        };

    match value_index {
        None => unsafe { source_key_writer.set_null_unchecked(key_index) },
        Some(value_index) => unsafe {
            source_key_writer.set_value_index_unchecked(key_index, *value_index);
            source_key_writer.set_nonnull_unchecked(key_index);
        },
    }
}

fn with_values_and_path_typed<
    'a,
    K: ArrowDictionaryKeyType,
    TDiagnostic: ColumnarEngineDiagnosticReceiver<'a>,
>(
    diagnostic_receiver: &TDiagnostic,
    source_keys: DictionaryKeyArray,
    source_values: DictionaryValueArray<'a>,
    key_filter: Option<&RoaringBitmap>,
    path: &[ColumnarEngineSelectionPath<'a>],
    value: &Dictionary<'a>,
) -> Dictionary<'a> {
    assert!(!path.is_empty());

    let (mut source_values, source_value_lookup) = source_values.into_set();

    let key_length = source_keys.len();

    let mut visited_values = AHashMap::with_capacity(key_length);

    let mut key_builder = DictionaryKeyArrayBuilder::<K>::new(key_length);
    let mut key_writer = key_builder.get_writer();

    let current_path = unsafe { path.get_unchecked(0) };

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
                                        &path[1..],
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
                                        &path[1..],
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

    if source_values.is_empty() {
        return Dictionary::new_null::<K>(key_length);
    } else if source_values.len() == 1 && !key_builder.has_nulls() {
        return Dictionary::new_scalar::<K>(
            key_length,
            source_values.into_iter().next().expect("has value"),
        );
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
