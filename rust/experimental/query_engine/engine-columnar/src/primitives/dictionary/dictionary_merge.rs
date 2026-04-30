// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::collections::hash_map::Entry;

use ahash::{AHashMap, RandomState};
use arrow::{array::*, buffer::MutableBuffer, datatypes::*};
use indexmap::IndexSet;

use crate::{dictionary_transform::push_null, *};

pub(crate) fn merge<'a, FMerge, const COUNT: usize>(
    values: [Dictionary<'a>; COUNT],
    merge: FMerge,
) -> Dictionary<'a>
where
    FMerge: FnMut([ValueOrRef<'a>; COUNT]) -> ValueOrRef<'a>,
{
    assert!(COUNT > 0);

    match values[0].keys().data_type() {
        DataType::UInt8 => merge_typed::<UInt8Type, FMerge, COUNT>(values, merge),
        DataType::UInt16 => merge_typed::<UInt16Type, FMerge, COUNT>(values, merge),
        DataType::UInt32 => merge_typed::<UInt32Type, FMerge, COUNT>(values, merge),
        DataType::UInt64 => merge_typed::<UInt64Type, FMerge, COUNT>(values, merge),

        DataType::Int8 => merge_typed::<Int8Type, FMerge, COUNT>(values, merge),
        DataType::Int16 => merge_typed::<Int16Type, FMerge, COUNT>(values, merge),
        DataType::Int32 => merge_typed::<Int32Type, FMerge, COUNT>(values, merge),
        DataType::Int64 => merge_typed::<Int64Type, FMerge, COUNT>(values, merge),

        _ => panic!("Key type is not supported"),
    }
}

fn merge_typed<'a, K: ArrowDictionaryKeyType, FMerge, const COUNT: usize>(
    values: [Dictionary<'a>; COUNT],
    mut merge: FMerge,
) -> Dictionary<'a>
where
    FMerge: FnMut([ValueOrRef<'a>; COUNT]) -> ValueOrRef<'a>,
{
    debug_assert!(COUNT > 0);

    let key_count = values[0].keys().len();

    let mut visited_values: AHashMap<
        [Option<usize>; COUNT],
        Option<<K as ArrowPrimitiveType>::Native>,
    > = AHashMap::new();
    let mut merged_values = IndexSet::with_hasher(RandomState::new());

    let mut key_buffer = MutableBuffer::from_len_zeroed(size_of::<K::Native>() * key_count);
    let key_builder = key_buffer.typed_data_mut::<K::Native>().as_mut_ptr();

    let key_bit_length = arrow::util::bit_util::ceil(key_count, 8);
    let mut null_buffer = None;

    for key_index in 0..key_count {
        let mut value_indices = [None; COUNT];
        for i in 0..COUNT {
            value_indices[i] = values[i].get_value_index(key_index);
        }

        match visited_values.entry(value_indices) {
            Entry::Occupied(occupied) => match occupied.get() {
                Some(v) => {
                    unsafe { *key_builder.add(key_index) = *v };
                }
                None => {
                    push_null(&mut null_buffer, key_index, key_bit_length);
                }
            },
            Entry::Vacant(vacant) => {
                let mut values_to_merge: [ValueOrRef<'_>; COUNT] =
                    std::array::from_fn(|_| ValueOrRef::Null);
                for (i, value_index) in vacant.key().iter().enumerate() {
                    values_to_merge[i] =
                        value_index.map_or(ValueOrRef::Null, |v| values[i].values().get_value_at(v))
                }
                match merge(values_to_merge) {
                    ValueOrRef::Null => {
                        vacant.insert(None);
                        push_null(&mut null_buffer, key_index, key_bit_length);
                    }
                    v => {
                        let (merged_value_index, _) = merged_values.insert_full(v);
                        let native_merged_value_index =
                            <K as ArrowPrimitiveType>::Native::from_usize(merged_value_index)
                                .unwrap();
                        vacant.insert(Some(native_merged_value_index));
                        unsafe { *key_builder.add(key_index) = native_merged_value_index };
                    }
                }
            }
        }
    }

    Dictionary::new(
        PrimitiveArray::<K>::new(
            key_buffer.into(),
            null_buffer.and_then(|v| NullBufferBuilder::new_from_buffer(v, key_count).finish()),
        )
        .into(),
        merged_values.into(),
    )
}

#[cfg(test)]
mod tests {
    use data_engine_expressions::*;

    use super::*;

    #[test]
    fn merge_three() {
        let mut a = PrimitiveDictionaryBuilder::<UInt16Type, Int64Type>::new();
        a.append_null();
        a.append_value(0);
        a.append_value(5);
        let a_array = a.finish();

        let a_dict: Dictionary = a_array.downcast_dict::<Int64Array>().unwrap().into();

        let mut b = PrimitiveDictionaryBuilder::<UInt16Type, Int64Type>::new();
        b.append_value(3);
        b.append_value(3);
        b.append_null();
        let b_array = b.finish();

        let b_dict: Dictionary = b_array.downcast_dict::<Int64Array>().unwrap().into();

        let mut c = StringDictionaryBuilder::<UInt16Type>::new();
        c.append_value("hello world");
        c.append_value("hello world");
        c.append_value("hello world");
        let c_array = c.finish();

        let c_dict: Dictionary = c_array.downcast_dict::<StringArray>().unwrap().into();

        let merged = merge([a_dict, b_dict, c_dict], |values| {
            debug_assert!(values.len() == 3);

            let start = match values[0].to_value() {
                Value::Integer(i) => i.get_value() as usize,
                _ => 0,
            };

            let end = match values[1].to_value() {
                Value::Integer(i) => Some(i.get_value() as usize),
                _ => None,
            };

            match &values[2] {
                ValueOrRef::String(string_value) => {
                    let v = string_value.get_value();
                    let end = end.unwrap_or(v.len());
                    ValueOrRef::String(StringValueOrRef::new_slice(
                        string_value.clone(),
                        start,
                        end,
                    ))
                }
                _ => ValueOrRef::Null,
            }
        });

        let mut expected_keys = PrimitiveBuilder::<UInt16Type>::new();
        expected_keys.append_value(0);
        expected_keys.append_value(0);
        expected_keys.append_value(1);
        let expected_key_array = expected_keys.finish();

        let mut expected_values = IndexSet::with_hasher(RandomState::new());
        expected_values.insert(ValueOrRef::String(StringValueOrRef::new_ref("hel")));
        expected_values.insert(ValueOrRef::String(StringValueOrRef::new_ref(" world")));

        assert_eq!(
            Dictionary::new(expected_key_array.into(), expected_values.into()),
            merged
        );
    }
}
