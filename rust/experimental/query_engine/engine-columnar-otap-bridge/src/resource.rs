// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::fmt::Display;

use arrow::array::*;
use data_engine_columnar::*;
use otap_df_pdata::{
    otap::raw_batch_store::POSITION_LOOKUP,
    proto::opentelemetry::arrow::v1::ArrowPayloadType,
    schema::consts::{self},
};

use crate::*;

pub(crate) static RESOURCE_ATTRIBUTES_BATCH_POSITION: usize =
    POSITION_LOOKUP[ArrowPayloadType::ResourceAttrs as usize];

#[derive(Debug)]
pub struct OtapResource<'pipeline, 'record> {
    pub resource_struct: &'record StructArray,
    pub attributes: Option<OtapAttributes<'pipeline, 'record>>,
}

impl<'pipeline> RecordTable<'pipeline> for OtapResource<'pipeline, '_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'pipeline, '_>> {
        if key == consts::ATTRIBUTES || key == "Attributes" {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        None
    }
}

impl Display for OtapResource<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Resource(RecordCount={})", self.resource_struct.len())
    }
}
