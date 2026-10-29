use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion::scalar::ScalarValue;
use geo_traits::{GeometryTrait, GeometryType as NativeGeometryType};
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CoordType, Dimension, GeometryType, Metadata, PointType};

use crate::error::GeoDataFusionResult;
pub(crate) use crate::udf::native::geometry::srid_from_metadata;
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata, metadata_for_srid, normalize_srid,
    validate_geometry_for_strict_builder,
};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) enum GeometryKind {
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
    GeometryCollection,
}

impl GeometryKind {
    fn matches(self, geometry: &impl GeometryTrait<T = f64>) -> bool {
        let actual = match geometry.as_type() {
            NativeGeometryType::Point(_) => Self::Point,
            NativeGeometryType::LineString(_) => Self::LineString,
            NativeGeometryType::Polygon(_) => Self::Polygon,
            NativeGeometryType::MultiPoint(_) => Self::MultiPoint,
            NativeGeometryType::MultiLineString(_) => Self::MultiLineString,
            NativeGeometryType::MultiPolygon(_) => Self::MultiPolygon,
            NativeGeometryType::GeometryCollection(_) => Self::GeometryCollection,
            _ => return false,
        };

        self == actual
    }
}

pub(crate) fn typed_geometry_signature(one_arg_types: Vec<DataType>) -> Signature {
    let mut signatures = vec![TypeSignature::Uniform(1, one_arg_types.clone())];

    for input_type in one_arg_types {
        signatures.push(TypeSignature::Exact(vec![
            input_type.clone(),
            DataType::Int32,
        ]));
        signatures.push(TypeSignature::Exact(vec![input_type, DataType::Int64]));
    }

    Signature::one_of(signatures, Volatility::Immutable)
}

/// Recover GeoArrow extension metadata after row constructors strip it.
///
/// `named_struct` keeps the physical GeoArrow layout but drops field metadata.
/// Generic geometry unions still identify their point child's coordinate
/// layout, and unambiguous typed point layouts can be recovered as points.
pub(crate) fn geometry_field_from_physical(field: &Field) -> Option<FieldRef> {
    if field.extension_type_name().is_some() {
        return Some(Arc::new(field.clone()));
    }
    match field.data_type() {
        DataType::Union(union_fields, _) => {
            let point = union_fields
                .iter()
                .find(|(_, child)| child.name() == "Point")
                .map(|(_, child)| child)?;
            let coord_type = match point.data_type() {
                DataType::Struct(_) => CoordType::Separated,
                DataType::FixedSizeList(_, _) => CoordType::Interleaved,
                _ => return None,
            };
            Some(Arc::new(
                GeometryType::new(Arc::new(Metadata::default()))
                    .with_coord_type(coord_type)
                    .to_field(field.name(), field.is_nullable()),
            ))
        }
        DataType::Struct(fields) => {
            let names = fields
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>();
            let dimension = match names.as_slice() {
                ["x", "y"] => Dimension::XY,
                ["x", "y", "z"] => Dimension::XYZ,
                ["x", "y", "m"] => Dimension::XYM,
                ["x", "y", "z", "m"] => Dimension::XYZM,
                _ => return None,
            };
            if fields
                .iter()
                .any(|field| field.data_type() != &DataType::Float64)
            {
                return None;
            }
            Some(Arc::new(
                PointType::new(dimension, Arc::new(Metadata::default()))
                    .with_coord_type(CoordType::Separated)
                    .to_field(field.name(), field.is_nullable()),
            ))
        }
        DataType::FixedSizeList(item, size) if item.data_type() == &DataType::Float64 => {
            let dimension = match *size {
                2 => Dimension::XY,
                3 => Dimension::XYZ,
                4 => Dimension::XYZM,
                _ => return None,
            };
            Some(Arc::new(
                PointType::new(dimension, Arc::new(Metadata::default()))
                    .with_coord_type(CoordType::Interleaved)
                    .to_field(field.name(), field.is_nullable()),
            ))
        }
        _ => None,
    }
}

pub(crate) fn decode_geometry_array<'a, A: GeoArrowArrayAccessor<'a>>(
    array: &'a A,
    typ: GeometryType,
    target: Option<GeometryKind>,
) -> GeoDataFusionResult<ArrayRef> {
    decode_geometry_array_inner(array, typ, target, None)
}

pub(crate) fn decode_geometry_array_with_srid<'a, A: GeoArrowArrayAccessor<'a>>(
    array: &'a A,
    typ: GeometryType,
    target: Option<GeometryKind>,
    function_name: &str,
    expected_srid: i32,
    embedded_srids: &[Option<i32>],
) -> GeoDataFusionResult<ArrayRef> {
    decode_geometry_array_inner(
        array,
        typ,
        target,
        Some((function_name, expected_srid, embedded_srids)),
    )
}

fn decode_geometry_array_inner<'a, A: GeoArrowArrayAccessor<'a>>(
    array: &'a A,
    typ: GeometryType,
    target: Option<GeometryKind>,
    srids: Option<(&str, i32, &[Option<i32>])>,
) -> GeoDataFusionResult<ArrayRef> {
    let mut builder = StrictGeometryBuilder::new(typ);
    for (row, value) in array.iter().enumerate() {
        match value {
            None => builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?,
            Some(Err(err)) => return Err(err.into()),
            Some(Ok(geometry)) => {
                push_decoded_geometry(&mut builder, &geometry, target, srids, row)?
            }
        }
    }

    Ok(Arc::new(builder.finish()).to_array_ref())
}

/// Pushes one decoded geometry with the shared constructor semantics.
///
/// Type-specific constructors return NULL for valid geometries of another type,
/// while embedded SRIDs must agree with the declared CRS.
pub(crate) fn push_decoded_geometry(
    builder: &mut StrictGeometryBuilder,
    geometry: &impl GeometryTrait<T = f64>,
    target: Option<GeometryKind>,
    srids: Option<(&str, i32, &[Option<i32>])>,
    row: usize,
) -> GeoDataFusionResult<()> {
    if target.is_none_or(|target| target.matches(geometry)) {
        if let Some((function_name, expected_srid, embedded_srids)) = srids {
            let embedded_srid = embedded_srids.get(row).copied().flatten().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{function_name}: missing embedded SRID for row {row}"
                ))
            })?;
            if embedded_srid != expected_srid {
                return Err(DataFusionError::Execution(format!(
                    "{function_name}: embedded SRID {embedded_srid} does not match declared CRS SRID {expected_srid}"
                ))
                .into());
            }
        }
        builder.push_geometry(Some(geometry))?;
    } else {
        validate_geometry_for_strict_builder(geometry)?;
        builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
    }
    Ok(())
}

pub(crate) fn read_ewkb_srid(bytes: &[u8]) -> Result<Option<i32>> {
    let byte_order = *bytes
        .first()
        .ok_or_else(|| DataFusionError::Execution("empty EWKB buffer".to_string()))?;
    let little_endian = match byte_order {
        0 => false,
        1 => true,
        value => {
            return Err(DataFusionError::Execution(format!(
                "invalid EWKB byte order {value}"
            )));
        }
    };
    let raw_type = bytes
        .get(1..5)
        .ok_or_else(|| DataFusionError::Execution("truncated EWKB header".to_string()))?;
    let raw_type = if little_endian {
        u32::from_le_bytes(raw_type.try_into().unwrap())
    } else {
        u32::from_be_bytes(raw_type.try_into().unwrap())
    };
    if raw_type & 0x2000_0000 == 0 {
        return Ok(None);
    }

    let srid = bytes.get(5..9).ok_or_else(|| {
        DataFusionError::Execution("EWKB SRID flag without SRID bytes".to_string())
    })?;
    let srid = if little_endian {
        i32::from_le_bytes(srid.try_into().unwrap())
    } else {
        i32::from_be_bytes(srid.try_into().unwrap())
    };
    Ok(Some(normalize_srid(i64::from(srid))?))
}

/// Read embedded SRIDs when the input field is a WKB storage type.
///
/// `None` at a row means the buffer is null or has no embedded SRID.
pub(crate) fn present_ewkb_srids(
    array: &dyn Array,
    field: &Field,
) -> Result<Option<Vec<Option<i32>>>> {
    let result = match field.data_type() {
        DataType::Binary => array
            .as_binary::<i32>()
            .iter()
            .map(|value| value.map(read_ewkb_srid).transpose())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|value| value.flatten())
            .collect(),
        DataType::LargeBinary => array
            .as_binary::<i64>()
            .iter()
            .map(|value| value.map(read_ewkb_srid).transpose())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|value| value.flatten())
            .collect(),
        DataType::BinaryView => array
            .as_binary_view()
            .iter()
            .map(|value| value.map(read_ewkb_srid).transpose())
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|value| value.flatten())
            .collect(),
        _ => return Ok(None),
    };
    Ok(Some(result))
}

pub(crate) fn validate_embedded_srids(
    function_name: &str,
    expected_srid: i32,
    embedded_srids: &[Option<i32>],
) -> Result<()> {
    for (row, embedded_srid) in embedded_srids.iter().enumerate() {
        if let Some(embedded_srid) = embedded_srid
            && *embedded_srid != expected_srid
        {
            return Err(DataFusionError::Execution(format!(
                "{function_name}: embedded SRID {embedded_srid} on row {row} does not match declared CRS SRID {expected_srid}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn empty_geometry_array(typ: GeometryType, len: usize) -> ArrayRef {
    let mut builder = GeometryBuilder::new(typ);
    for _ in 0..len {
        builder.push_null();
    }
    Arc::new(builder.finish()).to_array_ref()
}

pub(crate) fn typed_metadata_from_args(
    args: &datafusion::logical_expr::ReturnFieldArgs<'_>,
    input_field: &Field,
    function_name: &str,
) -> Result<Arc<Metadata>> {
    if args.arg_fields.len() == 1 {
        return Ok(field_metadata(input_field)?);
    }

    let srid = match args.scalar_arguments.get(1) {
        Some(Some(ScalarValue::Int32(Some(value)))) => Some(i64::from(*value)),
        Some(Some(ScalarValue::Int64(Some(value)))) => Some(*value),
        Some(Some(ScalarValue::Int32(None) | ScalarValue::Int64(None) | ScalarValue::Null)) => {
            return Ok(metadata_for_srid(0));
        }
        Some(Some(value)) => {
            return Err(DataFusionError::Internal(format!(
                "{function_name} received an unsupported SRID value: {value:?}"
            )));
        }
        // DataFusion wraps NULL casts in a non-literal expression. Keep planning metadata
        // pessimistic and enforce literal semantics at execution time when a non-null SRID is
        // observed.
        Some(None) | None => None,
    };

    match srid {
        Some(srid) => Ok(metadata_for_srid(normalize_srid(srid)?)),
        None => Ok(metadata_for_srid(0)),
    }
}
