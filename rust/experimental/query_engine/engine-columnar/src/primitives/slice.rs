// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use data_engine_expressions::*;

use crate::*;

#[derive(Debug)]
pub(crate) enum Slice<'a> {
    String(StringSlice<'a>),
}

impl AsValue for Slice<'_> {
    fn get_value_type(&self) -> ValueType {
        match self {
            Slice::String(_) => ValueType::String,
        }
    }

    fn to_value(&self) -> Value<'_> {
        match self {
            Slice::String(s) => Value::String(s),
        }
    }
}

#[derive(Debug)]
pub(crate) struct StringSlice<'a> {
    inner_value: StringValueOrRef<'a>,
    range_start_inclusive: usize,
    range_end_exclusive: usize,
}

impl<'a> StringSlice<'a> {
    pub(crate) fn from_char_range(
        inner_value: StringValueOrRef<'a>,
        range_start_inclusive: usize,
        range_end_exclusive: usize,
    ) -> StringSlice<'a> {
        // Note: Slice of a str returns raw utf8 bytes. Chars can take 1 to 4
        // bytes. In order to correctly slice the str as chars we have to find
        // the correct byte indices to do the slicing
        let count = range_end_exclusive - range_start_inclusive;
        let mut chars = inner_value
            .get_value()
            .char_indices()
            .skip(range_start_inclusive)
            .take(count);

        if let Some(first) = chars.next() {
            if let Some(last) = chars.last() {
                let mut buf = [0; 4];
                let encoded = last.1.encode_utf8(&mut buf);

                Self {
                    inner_value,
                    range_start_inclusive: first.0,
                    range_end_exclusive: last.0 + encoded.len(),
                }
            } else {
                let mut buf = [0; 4];
                let encoded = first.1.encode_utf8(&mut buf);

                Self {
                    inner_value,
                    range_start_inclusive: first.0,
                    range_end_exclusive: first.0 + encoded.len(),
                }
            }
        } else {
            Self {
                inner_value,
                range_start_inclusive: 0,
                range_end_exclusive: 0,
            }
        }
    }

    pub(crate) fn from_byte_range(
        inner_value: StringValueOrRef<'a>,
        range_start_inclusive: usize,
        range_end_exclusive: usize,
    ) -> StringSlice<'a> {
        Self {
            inner_value,
            range_start_inclusive,
            range_end_exclusive,
        }
    }
}

impl StringValue for StringSlice<'_> {
    fn get_value(&self) -> &str {
        let value = self.inner_value.get_value();

        &value[self.range_start_inclusive..self.range_end_exclusive]
    }
}
