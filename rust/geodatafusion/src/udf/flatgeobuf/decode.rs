use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use flatgeobuf::FgbReader;
use geoarrow_flatgeobuf::reader::schema::FlatGeobufSchemaScanner;
use geoarrow_flatgeobuf::reader::{
    FlatGeobufHeaderExt, FlatGeobufReaderOptions, FlatGeobufRecordBatchIterator,
};
use geoarrow_schema::{CoordType, Crs, GeoArrowType, Metadata};

/// A decoded FlatGeobuf payload.
pub(super) struct DecodedFlatGeobuf {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Decode FlatGeobuf bytes into Arrow record batches.
///
/// FlatGeobuf does not store a name for the geometry column. The returned
/// schema uses `geometry_name` so callers can align it with their SQL-facing
/// schema.
pub(super) fn decode_bytes(
    bytes: &[u8],
    use_view_types: bool,
    geometry_name: &str,
) -> Result<DecodedFlatGeobuf> {
    let reader = FgbReader::open(Cursor::new(bytes))
        .map_err(|error| DataFusionError::Execution(format!("invalid FlatGeobuf data: {error}")))?;
    let header = reader.header();
    let geometry_type = with_header_crs(
        header
            .geoarrow_type(CoordType::default())
            .map_err(|error| {
                DataFusionError::Execution(format!("unsupported FlatGeobuf geometry: {error}"))
            })?,
        &header,
    );

    let properties_schema = match header.properties_schema(use_view_types) {
        Some(schema) => schema,
        None => {
            drop(reader);
            scan_properties_schema(bytes, use_view_types)?
        }
    };

    let mut fields = properties_schema.fields().to_vec();
    if fields.iter().any(|field| field.name() == geometry_name) {
        return Err(DataFusionError::Execution(format!(
            "FlatGeobuf property column `{geometry_name}` conflicts with the geometry column"
        )));
    }
    fields.push(Arc::new(geometry_type.to_field(geometry_name, true)));
    let schema = Arc::new(Schema::new(fields));

    let reader = FgbReader::open(Cursor::new(bytes))
        .map_err(|error| DataFusionError::Execution(format!("invalid FlatGeobuf data: {error}")))?;
    let selection = reader
        .select_all_seq()
        .map_err(|error| DataFusionError::Execution(format!("invalid FlatGeobuf data: {error}")))?;
    let options = FlatGeobufReaderOptions::new(properties_schema, geometry_type);
    let iterator = FlatGeobufRecordBatchIterator::try_new(selection, options)
        .map_err(|error| DataFusionError::Execution(format!("invalid FlatGeobuf data: {error}")))?;
    let raw_batches = iterator
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| DataFusionError::Execution(format!("invalid FlatGeobuf data: {error}")))?;

    let batches = raw_batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).map_err(|error| {
                DataFusionError::Execution(format!("invalid FlatGeobuf data: {error}"))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(DecodedFlatGeobuf { schema, batches })
}

/// FlatGeobuf defines a missing CRS organization as EPSG, but the GeoArrow
/// reader currently drops an authority code when `org` is absent.
fn with_header_crs(geometry_type: GeoArrowType, header: &flatgeobuf::Header<'_>) -> GeoArrowType {
    if geometry_type.metadata().crs().crs_value().is_some() {
        return geometry_type;
    }
    let Some(crs) = header.crs() else {
        return geometry_type;
    };
    let authority = if let Some(code) = crs.code_string() {
        format!("{}:{code}", crs.org().unwrap_or("EPSG"))
    } else if crs.code() != 0 {
        format!("{}:{}", crs.org().unwrap_or("EPSG"), crs.code())
    } else {
        return geometry_type;
    };
    let metadata = Metadata::new(Crs::from_authority_code(authority), None);
    geometry_type.with_metadata(Arc::new(metadata))
}

fn scan_properties_schema(bytes: &[u8], use_view_types: bool) -> Result<SchemaRef> {
    let reader = FgbReader::open(Cursor::new(bytes))
        .map_err(|error| DataFusionError::Execution(format!("invalid FlatGeobuf data: {error}")))?;
    let selection = reader
        .select_all_seq()
        .map_err(|error| DataFusionError::Execution(format!("invalid FlatGeobuf data: {error}")))?;
    let mut scanner = FlatGeobufSchemaScanner::new(use_view_types);
    scanner
        .process_seq(selection, Some(1024))
        .map_err(|error| DataFusionError::Execution(format!("invalid FlatGeobuf data: {error}")))?;
    Ok(scanner.finish())
}
