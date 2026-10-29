//! FlatGeobuf SQL functions.
//!
//! The implementation intentionally uses the format crates directly. The
//! workspace's `geodatafusion-flatgeobuf` integration crate depends on this
//! crate, so depending on it here would create a cycle.
//!
//! # API adaptations
//!
//! DataFusion has no PostgreSQL-compatible composite `anyelement` value, so
//! the public SQL shapes differ from PostGIS in three explicit ways:
//!
//! - `ST_AsFlatGeobuf(geometry, attributes...)` is the flattened row form.
//!   Attribute argument names become FlatGeobuf property names. A single
//!   metadata-bearing struct argument, with optional `index` and `geom_name`
//!   arguments, is accepted as the equivalent row form. DataFusion's built-in
//!   `struct` and `named_struct` functions drop extension-type metadata, so
//!   the flattened form is the reliable public path. When the first argument
//!   is a geometry column, a trailing constant boolean is the PostGIS `index`
//!   option rather than a property. The flattened form rejects the `geom_name`
//!   option because its geometry column is positional; only the struct row
//!   form can select the geometry column by name. Serialized WKB/WKT inputs
//!   are decoded to native GeoArrow geometry while retaining their CRS
//!   metadata.
//! - `ST_FromFlatGeobuf(bytes)` derives its output schema from the file:
//!   properties in file order, then a geometry column named `geom`. PostgreSQL
//!   selects the output record type with a template row argument, which
//!   DataFusion table functions cannot express; the two-argument form is
//!   rejected with an explicit error instead of ignoring the template. The
//!   synthetic feature-id column emitted by PostGIS is not reproduced.
//! - PostGIS implements `ST_FromFlatGeobufToTable` as a `void` scalar with a
//!   catalog side effect. DataFusion exposes it as a table function that
//!   registers the requested table on the caller's catalog and returns that
//!   table's rows. Registration happens on the first stream poll, so planning,
//!   plain `EXPLAIN`, an uncollected `DataFrame`, and `LIMIT 0` do not mutate
//!   the catalog.

mod decode;
mod encode;
mod from_flatgeobuf;
mod schema;
mod to_table;

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{Expr, SessionContext};
use datafusion::scalar::ScalarValue;

use self::encode::AsFlatGeobuf;
use self::from_flatgeobuf::FromFlatGeobuf;
use self::to_table::FromFlatGeobufToTable;

/// Register the FlatGeobuf SQL functions.
pub fn register(session_context: &SessionContext) {
    session_context.register_udaf(AsFlatGeobuf::new().into());
    session_context.register_udtf("st_fromflatgeobuf", std::sync::Arc::new(FromFlatGeobuf));
    session_context.register_udtf(
        "st_fromflatgeobuftotable",
        std::sync::Arc::new(FromFlatGeobufToTable),
    );
}

pub(super) fn constant_bytes(expr: &Expr, function: &str) -> Result<Vec<u8>> {
    match expr {
        Expr::Literal(ScalarValue::Binary(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeBinary(Some(value)), _)
        | Expr::Literal(ScalarValue::BinaryView(Some(value)), _) => Ok(value.clone()),
        _ => Err(DataFusionError::Execution(format!(
            "{function}: FlatGeobuf input must be a constant binary value"
        ))),
    }
}

pub(super) fn constant_string(expr: &Expr, function: &str) -> Result<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Ok(value.clone()),
        _ => Err(DataFusionError::Execution(format!(
            "{function}: argument must be a constant text value"
        ))),
    }
}
