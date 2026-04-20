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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            StringValueOrRef::Ref(s) => s.len(),
            StringValueOrRef::Owned(s) => s.len(),
            StringValueOrRef::Slice(s) => s.len(),
        }
    }

    pub fn char_len(&self) -> usize {
        match self {
            StringValueOrRef::Ref(s) => s.chars().count(),
            StringValueOrRef::Owned(s) => s.chars().count(),
            StringValueOrRef::Slice(s) => s.char_len(),
        }
    }

    pub fn char_indices(&self) -> CharIndices<'_> {
        match self {
            StringValueOrRef::Ref(s) => CharIndices::String(s.char_indices()),
            StringValueOrRef::Owned(s) => CharIndices::String(s.char_indices()),
            StringValueOrRef::Slice(s) => s.char_indices(),
        }
    }

    pub fn append_to(self, value: &mut String) {
        match self {
            StringValueOrRef::Ref(s) => value.push_str(s),
            StringValueOrRef::Owned(s) => value.push_str(&s),
            StringValueOrRef::Slice(s) => s.append_to(value),
        }
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
        // Note: Slice of a str returns raw utf8 bytes. Chars can take 1 to 4
        // bytes. In order to correctly slice the str as chars we have to find
        // the correct byte indices to do the slicing
        let count = range_end_exclusive - range_start_inclusive;
        if count == 0 {
            return StringValueOrRef::Ref("");
        }

        let str_byte_len = inner_value.len();

        let mut chars = inner_value
            .char_indices()
            .skip(range_start_inclusive)
            .take(count);

        if let Some(first) = chars.next() {
            let mut char_len = 1;
            let mut last = None;
            loop {
                if let Some(c) = chars.next() {
                    char_len += 1;
                    last = Some(c);
                    continue;
                }
                break;
            }
            let mut buf = [0; 4];
            let (start, end) = if let Some(last) = last {
                let encoded = last.1.encode_utf8(&mut buf);

                (first.0, last.0 + encoded.len())
            } else {
                let encoded = first.1.encode_utf8(&mut buf);

                (first.0, first.0 + encoded.len())
            };

            if end - start == str_byte_len {
                inner_value
            } else {
                match inner_value {
                    StringValueOrRef::Ref(r) => StringValueOrRef::Ref(&r[start..end]),
                    StringValueOrRef::Owned(o) => StringValueOrRef::Slice(StringValueOrRefSlice {
                        value: StringValueOrRef::Owned(o).into(),
                        byte_start_inclusive: start,
                        byte_end_exclusive: end,
                        char_len,
                    }),
                    StringValueOrRef::Slice(s) => {
                        let start = start + s.byte_start_inclusive;
                        let end = end + s.byte_start_inclusive;

                        StringValueOrRef::Slice(StringValueOrRefSlice {
                            value: s.value,
                            byte_start_inclusive: start,
                            byte_end_exclusive: end,
                            char_len,
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

impl From<StringValueOrRef<'_>> for String {
    fn from(value: StringValueOrRef<'_>) -> Self {
        match value {
            StringValueOrRef::Ref(s) => s.into(),
            StringValueOrRef::Owned(s) => match Rc::try_unwrap(s) {
                Ok(s) => s,
                Err(o) => (*o).clone(),
            },
            StringValueOrRef::Slice(s) => {
                let mut v = String::new();
                s.append_to(&mut v);
                v
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct StringValueOrRefSlice<'a> {
    value: Box<StringValueOrRef<'a>>,
    byte_start_inclusive: usize,
    byte_end_exclusive: usize,
    char_len: usize,
}

impl StringValueOrRefSlice<'_> {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.byte_end_exclusive - self.byte_start_inclusive
    }

    pub fn char_len(&self) -> usize {
        self.char_len
    }

    pub fn char_indices(&self) -> CharIndices<'_> {
        CharIndices::Slice(StringValueOrRefSliceCharIndices {
            source: self.value.as_ref().char_indices().into(),
            position: 0,
            start_byte_index: self.byte_start_inclusive,
            end_char_index_exclusive: self.char_len,
        })
    }

    pub fn append_to(self, value: &mut String) {
        value.reserve(self.len());
        for (_, c) in self.char_indices() {
            value.push(c);
        }
    }
}

impl StringValue for StringValueOrRefSlice<'_> {
    fn get_value(&self) -> &str {
        let value = self.value.get_value();

        &value[self.byte_start_inclusive..self.byte_end_exclusive]
    }
}

pub enum CharIndices<'a> {
    String(std::str::CharIndices<'a>),
    Slice(StringValueOrRefSliceCharIndices<'a>),
}

impl Iterator for CharIndices<'_> {
    type Item = (usize, char);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            CharIndices::String(c) => c.next(),
            CharIndices::Slice(c) => loop {
                if let Some(v) = c.source.next() {
                    c.position += 1;
                    if v.0 < c.start_byte_index {
                        continue;
                    }
                    if c.position > c.end_char_index_exclusive {
                        return None;
                    }
                    return Some((v.0 - c.start_byte_index, v.1));
                } else {
                    return None;
                }
            },
        }
    }
}

pub struct StringValueOrRefSliceCharIndices<'a> {
    source: Box<CharIndices<'a>>,
    position: usize,
    start_byte_index: usize,
    end_char_index_exclusive: usize,
}
