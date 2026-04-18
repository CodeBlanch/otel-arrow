// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::rc::Rc;

use data_engine_expressions::*;

use crate::{resolved_value::*, *};

#[derive(Debug, Clone)]
pub enum StringValueOrRef<'a> {
    Ref(&'a str),
    Owned(Rc<String>),
    Slice(StringValueOrRefSlice<'a>),
}

impl StringValueOrRef<'_> {
    pub fn new_owned(value: String) -> StringValueOrRef<'static> {
        StringValueOrRef::Owned(value.into())
    }
}

impl<'a> StringValueOrRef<'a> {
    pub fn new_ref(value: &'a str) -> StringValueOrRef<'a> {
        StringValueOrRef::Ref(value)
    }

    pub(crate) fn new_slice(
        inner_value: StringValueOrRef<'a>,
        range_start_inclusive: usize,
        range_end_exclusive: usize,
    ) -> StringValueOrRef<'a> {
        let value = inner_value.get_value();

        // Note: Slice of a str returns raw utf8 bytes. Chars can take 1 to 4
        // bytes. In order to correctly slice the str as chars we have to find
        // the correct byte indices to do the slicing
        let count = range_end_exclusive - range_start_inclusive;
        if count == 0 {
            return StringValueOrRef::Ref("");
        }

        let mut chars = value.char_indices().skip(range_start_inclusive).take(count);

        if let Some(first) = chars.next() {
            let mut buf = [0; 4];
            let (start, end) = if let Some(last) = chars.last() {
                let encoded = last.1.encode_utf8(&mut buf);

                (first.0, last.0 + encoded.len())
            } else {
                let encoded = first.1.encode_utf8(&mut buf);

                (first.0, first.0 + encoded.len())
            };

            if end - start == value.len() {
                inner_value
            } else {
                match inner_value {
                    StringValueOrRef::Ref(r) => StringValueOrRef::Ref(&r[start..end]),
                    StringValueOrRef::Owned(o) => StringValueOrRef::Slice(StringValueOrRefSlice {
                        value: StringValueOrRef::Owned(o).into(),
                        range_start_inclusive: start,
                        range_end_exclusive: end,
                    }),
                    StringValueOrRef::Slice(s) => {
                        let start = start + s.range_start_inclusive;
                        let end = end + s.range_start_inclusive;

                        StringValueOrRef::Slice(StringValueOrRefSlice {
                            value: s.value,
                            range_start_inclusive: start,
                            range_end_exclusive: end,
                        })
                    }
                }
            }
        } else {
            StringValueOrRef::Ref("")
        }
    }
}

impl StringValue for StringValueOrRef<'_> {
    fn get_value(&self) -> &str {
        match self {
            StringValueOrRef::Ref(s) => s,
            StringValueOrRef::Owned(s) => s,
            StringValueOrRef::Slice(s) => s.get_value(),
        }
    }
}

impl<'a> From<StringValueOrRef<'a>> for ResolvedScalarValue<'a> {
    fn from(value: StringValueOrRef<'a>) -> Self {
        ResolvedScalarValue::Single(ValueOrRef::String(value))
    }
}

#[derive(Debug, Clone)]
pub struct StringValueOrRefSlice<'a> {
    value: Box<StringValueOrRef<'a>>,
    range_start_inclusive: usize,
    range_end_exclusive: usize,
}

impl StringValue for StringValueOrRefSlice<'_> {
    fn get_value(&self) -> &str {
        let value = self.value.get_value();

        &value[self.range_start_inclusive..self.range_end_exclusive]
    }
}
