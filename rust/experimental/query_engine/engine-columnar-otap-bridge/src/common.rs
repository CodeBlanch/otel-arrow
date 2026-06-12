// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::cell::OnceCell;

use arrow::{array::*, datatypes::*};
use data_engine_columnar::*;
use otap_df_pdata::{
    otap::transform::{materialize_parent_id_for_attributes, remove_delta_encoding_from_column},
    schema::consts::{self, metadata},
};

#[derive(Debug)]
pub enum OtapValue<'pipeline> {
    NotFound,
    Removed,
    Read(Dictionary<'pipeline>),
    Set(Dictionary<'pipeline>),
}

#[derive(Debug)]
pub struct OtapIds<'record> {
    encoding: Option<&'record str>,
    encoded: Option<&'record PrimitiveArray<UInt16Type>>,
    decoded: OnceCell<PrimitiveArray<UInt16Type>>,
}

impl<'record> OtapIds<'record> {
    pub fn new(
        encoded_ids: &'record PrimitiveArray<UInt16Type>,
        encoding: Option<&'record str>,
    ) -> OtapIds<'record> {
        Self {
            encoding,
            encoded: Some(encoded_ids),
            decoded: OnceCell::new(),
        }
    }

    pub fn from_batch(value: &'record RecordBatch) -> OtapIds<'record> {
        let id_column = value
            .schema_ref()
            .column_with_name(consts::ID)
            .expect("has ids");

        OtapIds::new(
            value.column(id_column.0).as_primitive::<UInt16Type>(),
            id_column
                .1
                .metadata()
                .get(metadata::COLUMN_ENCODING)
                .map(|v| v.as_str()),
        )
    }

    pub fn from_struct(value: &'record StructArray) -> OtapIds<'record> {
        let id_column = value.fields().find(consts::ID).expect("has ids");

        OtapIds::new(
            value.column(id_column.0).as_primitive::<UInt16Type>(),
            id_column
                .1
                .metadata()
                .get(metadata::COLUMN_ENCODING)
                .map(|v| v.as_str()),
        )
    }

    pub fn from_decoded(decoded_ids: PrimitiveArray<UInt16Type>) -> OtapIds<'record> {
        let val = OnceCell::new();
        val.set(decoded_ids).expect("set");

        Self {
            encoding: None,
            encoded: None,
            decoded: val,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.decoded.get().map_or_else(
            || self.encoded.is_none_or(|v| v.is_empty()),
            |v| v.is_empty(),
        )
    }

    pub fn len(&self) -> usize {
        self.decoded
            .get()
            .map_or_else(|| self.encoded.map_or(0, |v| v.len()), |v| v.len())
    }

    pub fn get_ids(&self) -> &PrimitiveArray<UInt16Type> {
        if self.encoding == Some(metadata::encodings::PLAIN)
            && let Some(encoded) = self.encoded
        {
            encoded
        } else {
            self.decoded.get_or_init(|| self.init())
        }
    }

    pub fn into_parts(mut self) -> Option<PrimitiveArray<UInt16Type>> {
        if self.encoding == Some(metadata::encodings::PLAIN) {
            None
        } else {
            Some(self.decoded.take().unwrap_or_else(|| self.init()))
        }
    }

    fn init(&self) -> PrimitiveArray<UInt16Type> {
        remove_delta_encoding_from_column(self.encoded.expect("has encoded ids"))
    }
}

#[derive(Debug)]
pub struct OtapParentIds<'record> {
    attributes_batch: Option<&'record RecordBatch>,
    parent_id_column: Option<usize>,
    encoding: Option<&'record str>,
    encoded: Option<&'record PrimitiveArray<UInt16Type>>,
    decoded: OnceCell<PrimitiveArray<UInt16Type>>,
}

impl<'record> OtapParentIds<'record> {
    pub fn new(attributes_batch: &'record RecordBatch) -> OtapParentIds<'record> {
        let parent_id_column = attributes_batch
            .schema_ref()
            .column_with_name(consts::PARENT_ID)
            .expect("has parent ids");

        let encoded_ids = attributes_batch
            .column(parent_id_column.0)
            .as_primitive::<UInt16Type>();

        let encoding = parent_id_column
            .1
            .metadata()
            .get(metadata::COLUMN_ENCODING)
            .map(|v| v.as_str());

        Self {
            attributes_batch: Some(attributes_batch),
            parent_id_column: Some(parent_id_column.0),
            encoding,
            encoded: Some(encoded_ids),
            decoded: OnceCell::new(),
        }
    }

    pub fn from_decoded(decoded_ids: PrimitiveArray<UInt16Type>) -> OtapParentIds<'record> {
        let val = OnceCell::new();
        val.set(decoded_ids).expect("set");

        Self {
            attributes_batch: None,
            parent_id_column: None,
            encoding: None,
            encoded: None,
            decoded: val,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.decoded.get().map_or_else(
            || self.encoded.is_none_or(|v| v.is_empty()),
            |v| v.is_empty(),
        )
    }

    pub fn len(&self) -> usize {
        self.decoded
            .get()
            .map_or_else(|| self.encoded.map_or(0, |v| v.len()), |v| v.len())
    }

    pub fn get_ids(&self) -> &PrimitiveArray<UInt16Type> {
        if self.encoding == Some(metadata::encodings::PLAIN)
            && let Some(encoded) = self.encoded
        {
            encoded
        } else {
            self.decoded.get_or_init(|| self.init())
        }
    }

    pub fn into_parts(mut self) -> Option<PrimitiveArray<UInt16Type>> {
        if self.encoding == Some(metadata::encodings::PLAIN) {
            None
        } else {
            Some(self.decoded.take().unwrap_or_else(|| self.init()))
        }
    }

    fn init(&self) -> PrimitiveArray<UInt16Type> {
        materialize_parent_id_for_attributes::<u16>(self.attributes_batch.expect("has attributes"))
            .expect("materialized batch")
            .column(self.parent_id_column.expect("has parent ids"))
            .as_primitive::<UInt16Type>()
            .clone()
    }
}

#[derive(Debug, Default)]
pub struct OtapDecodedIds {
    pub ids: Option<PrimitiveArray<UInt16Type>>,
    pub parent_ids: Option<PrimitiveArray<UInt16Type>>,
}
