use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use geoarrow_schema::{GeoArrowType, GeometryType};

/// The normalized row schema used by the FlatGeobuf aggregate.
pub(super) struct FlatGeobufRowSchema {
    pub schema: SchemaRef,
}

/// Select the geometry field and normalize properties to types FlatGeobuf can
/// represent without silently dropping data.
pub(super) fn row_schema(
    input_fields: &[FieldRef],
    geom_name: Option<&str>,
) -> Result<FlatGeobufRowSchema> {
    let geometry_indices = input_fields
        .iter()
        .enumerate()
        .filter_map(|(index, field)| match geometry_type(field) {
            Ok(Some(_)) => Some(Ok(index)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>>>()?;

    if geometry_indices.is_empty() {
        return Err(DataFusionError::Execution(
            "ST_AsFlatGeobuf: row contains no geometry column".to_string(),
        ));
    }

    let geometry_index = match geom_name {
        Some(name) => input_fields
            .iter()
            .position(|field| field.name() == name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "ST_AsFlatGeobuf: geometry column `{name}` was not found"
                ))
            })?,
        None => geometry_indices[0],
    };
    if geometry_type(&input_fields[geometry_index])?.is_none() {
        return Err(DataFusionError::Execution(format!(
            "ST_AsFlatGeobuf: column `{}` is not a geometry column",
            input_fields[geometry_index].name()
        )));
    }
    if geometry_indices.len() != 1 {
        return Err(DataFusionError::Execution(
            "ST_AsFlatGeobuf: row contains multiple geometry columns".to_string(),
        ));
    }

    let geometry_field = normalize_geometry_field(&input_fields[geometry_index])?;
    let fields = input_fields
        .iter()
        .enumerate()
        .map(|(index, field)| {
            if index == geometry_index {
                Ok(Arc::clone(&geometry_field))
            } else {
                normalize_property_field(field).map(Arc::new)
            }
        })
        .collect::<Result<Vec<_>>>()?;

    ensure_unique_property_names(&fields, geometry_index)?;
    Ok(FlatGeobufRowSchema {
        schema: Arc::new(Schema::new(fields)),
    })
}

/// Build a schema for the flattened `ST_AsFlatGeobuf(geometry, attributes...)`
/// adaptation used by DataFusion, where composite row types are not available.
pub(super) fn flattened_schema(arg_fields: &[FieldRef]) -> Result<FlatGeobufRowSchema> {
    let (geometry, properties) = arg_fields.split_first().ok_or_else(|| {
        DataFusionError::Execution("ST_AsFlatGeobuf expects at least one argument".to_string())
    })?;
    let mut fields = vec![normalize_geometry_field(geometry)?];
    fields.extend(
        properties
            .iter()
            .map(|field| normalize_property_field(field).map(Arc::new))
            .collect::<Result<Vec<_>>>()?,
    );
    ensure_unique_property_names(&fields, 0)?;
    Ok(FlatGeobufRowSchema {
        schema: Arc::new(Schema::new(fields)),
    })
}

pub(super) fn geometry_type(field: &Field) -> Result<Option<GeoArrowType>> {
    GeoArrowType::from_extension_field(field)
        .map_err(|error| DataFusionError::Execution(format!("invalid GeoArrow field: {error}")))
}

pub(super) fn normalize_geometry_field(field: &Field) -> Result<FieldRef> {
    let Some(geometry) = geometry_type(field)? else {
        return Err(DataFusionError::Execution(
            "ST_AsFlatGeobuf: selected column is not a geometry column".to_string(),
        ));
    };

    // FlatGeobuf stores native coordinates, so serialized WKB/WKT inputs are
    // decoded into a generic GeoArrow geometry array while retaining the CRS
    // metadata from the input field.
    let normalized = match geometry {
        GeoArrowType::Wkb(_)
        | GeoArrowType::LargeWkb(_)
        | GeoArrowType::WkbView(_)
        | GeoArrowType::Wkt(_)
        | GeoArrowType::LargeWkt(_)
        | GeoArrowType::WktView(_) => {
            GeoArrowType::Geometry(GeometryType::new(Arc::clone(geometry.metadata())))
        }
        other => other,
    };
    Ok(Arc::new(
        normalized.to_field("geometry", field.is_nullable()),
    ))
}

fn ensure_unique_property_names(fields: &[FieldRef], geometry_index: usize) -> Result<()> {
    let mut names = std::collections::HashSet::new();
    for (index, field) in fields.iter().enumerate() {
        if index == geometry_index {
            continue;
        }
        if !names.insert(field.name().as_str()) {
            return Err(DataFusionError::Execution(format!(
                "ST_AsFlatGeobuf: duplicate property column `{}`",
                field.name()
            )));
        }
    }
    if names.contains("geometry") {
        return Err(DataFusionError::Execution(
            "ST_AsFlatGeobuf: property column `geometry` conflicts with the geometry column"
                .to_string(),
        ));
    }
    Ok(())
}

fn normalize_property_field(field: &Field) -> Result<Field> {
    let data_type = match field.data_type() {
        DataType::Boolean
        | DataType::Int8
        | DataType::UInt8
        | DataType::Int16
        | DataType::UInt16
        | DataType::Int32
        | DataType::UInt32
        | DataType::Int64
        | DataType::UInt64
        | DataType::Float64 => field.data_type().clone(),
        DataType::Float16 | DataType::Float32 => DataType::Float32,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        unsupported => {
            return Err(DataFusionError::Execution(format!(
                "ST_AsFlatGeobuf: property `{}` has type {unsupported}, which FlatGeobuf cannot represent",
                field.name()
            )));
        }
    };

    Ok(Field::new(field.name(), data_type, field.is_nullable()))
}
