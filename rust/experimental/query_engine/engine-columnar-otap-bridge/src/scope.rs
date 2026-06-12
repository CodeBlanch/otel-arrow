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

use crate::{arrow_helpers::*, *};

pub(crate) static SCOPE_ATTRIBUTES_BATCH_POSITION: usize =
    POSITION_LOOKUP[ArrowPayloadType::ScopeAttrs as usize];

#[derive(Debug)]
pub struct OtapScope<'pipeline, 'record> {
    pub scope_struct: &'record StructArray,
    pub attributes: Option<OtapAttributes<'pipeline, 'record>>,
}

impl<'pipeline> RecordTable<'pipeline> for OtapScope<'pipeline, '_> {
    fn get_values(&self, key: &str) -> Option<RecordTableValue<'pipeline, '_>> {
        if key == consts::ATTRIBUTES || key == "Attributes" {
            if let Some(attributes) = &self.attributes {
                return Some(RecordTableValue::Table(attributes));
            } else {
                return None;
            }
        }

        let values = match key {
            consts::NAME | "Name"
                if let Some(name_column) = self.scope_struct.column_by_name(consts::NAME) =>
            {
                adaptive_dictionary_reader::<StringArray>(name_column)
            }
            consts::VERSION | "Version"
                if let Some(version_column) = self.scope_struct.column_by_name(consts::VERSION) =>
            {
                adaptive_dictionary_reader::<StringArray>(version_column)
            }
            _ => return None,
        };

        values.map(RecordTableValue::Dictionary)
    }
}

impl Display for OtapScope<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Scope(RecordCount={})", self.scope_struct.len())
    }
}
