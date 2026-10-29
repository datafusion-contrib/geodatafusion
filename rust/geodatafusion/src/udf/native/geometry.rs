//! Shared geometry-array construction that preserves top-level geometry identity.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, GenericListArray, UnionArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::extension::EXTENSION_TYPE_NAME_KEY;
use arrow_schema::{DataType, Field};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion::scalar::ScalarValue;
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait,
    GeometryType as NativeGeometryType, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};
use geoarrow_array::array::{
    GeometryArray, GeometryCollectionArray, MultiPolygonArray, from_arrow_array,
};
use geoarrow_array::builder::{GeometryBuilder, GeometryCollectionBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, WrapArray};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::type_id::GeometryTypeId;
use geoarrow_schema::{
    CoordType, Crs, CrsType, Dimension, GeoArrowType, GeometryCollectionType, GeometryType,
    Metadata, MultiPolygonType,
};
use wkt::Wkt;
use wkt::types::{Coord, Dimension as WktDimension, LineString, Polygon};

use crate::error::{GeoDataFusionError, GeoDataFusionResult};
use crate::udf::native::owned_geometry::normalize_empty_multipolygon_members;

pub(crate) const SRID_UNKNOWN: i32 = 0;
pub(crate) const SRID_MAXIMUM: i32 = 999_999;
pub(crate) const SRID_USER_MAXIMUM: i32 = 998_999;

/// Convert an Arrow geometry column into a GeoArrow array.
///
/// WKT storage parses through the unbounded `wkt` reader on value access, so
/// every consumer that can read WKT values converts through this boundary;
/// native and WKB storage pass through without copying. Slices, NULLs, and
/// metadata therefore stay exactly as the caller supplied them.
pub(crate) fn geometry_array_from_arrow(
    array: &dyn Array,
    field: &Field,
) -> GeoDataFusionResult<Arc<dyn GeoArrowArray>> {
    let geo_type = GeoArrowType::from_arrow_field(field)?;
    ensure_wkt_input_depth(&geo_type, array)?;
    Ok(from_arrow_array(array, field)?)
}

/// Same boundary for callers that already resolved the GeoArrow type.
pub(crate) fn geometry_array_from_type(
    geo_type: &GeoArrowType,
    array: &dyn Array,
) -> GeoDataFusionResult<Arc<dyn GeoArrowArray>> {
    ensure_wkt_input_depth(geo_type, array)?;
    Ok(geo_type.wrap_array(array)?)
}

fn ensure_wkt_input_depth(geo_type: &GeoArrowType, array: &dyn Array) -> GeoDataFusionResult<()> {
    if matches!(
        geo_type,
        GeoArrowType::Wkt(_) | GeoArrowType::LargeWkt(_) | GeoArrowType::WktView(_)
    ) {
        crate::udf::native::io::wkt::ensure_wkt_array_depth(array)?;
    }
    Ok(())
}

/// Normalize an SRID using PostGIS's `clamp_srid` behavior.
pub(crate) fn normalize_srid(srid: i64) -> GeoDataFusionResult<i32> {
    let srid = i32::try_from(srid).map_err(|_| {
        datafusion::error::DataFusionError::Execution(format!(
            "SRID {srid} is outside the integer range"
        ))
    })?;
    if srid <= SRID_UNKNOWN {
        Ok(SRID_UNKNOWN)
    } else if srid <= SRID_MAXIMUM {
        Ok(srid)
    } else {
        Ok(SRID_USER_MAXIMUM + 1 + srid % (SRID_MAXIMUM - SRID_USER_MAXIMUM - 1))
    }
}

/// Build GeoArrow CRS metadata for an already-normalized SRID.
///
/// SRID 0 is the unknown CRS, so it must not be represented as `EPSG:0`.
pub(crate) fn metadata_for_srid(srid: i32) -> Arc<Metadata> {
    if srid == SRID_UNKNOWN {
        Arc::new(Metadata::default())
    } else {
        Arc::new(Metadata::new(
            Crs::from_authority_code(format!("EPSG:{srid}")),
            None,
        ))
    }
}

/// Whether a field has no GeoArrow extension metadata.
///
/// DataFusion can coerce a SQL NULL literal to the physical storage type of
/// the first geometry signature candidate. Such a field is a null geometry
/// placeholder, not a real geometry with an unknown CRS.
pub(crate) fn is_null_geometry_field(field: &Field) -> bool {
    field.data_type() == &DataType::Null || !field.metadata().contains_key(EXTENSION_TYPE_NAME_KEY)
}

/// Read the EPSG SRID declared by GeoArrow metadata.
pub(crate) fn srid_from_metadata(
    metadata: &Metadata,
    function_name: &str,
) -> GeoDataFusionResult<i32> {
    let Some(value) = metadata.crs().crs_value() else {
        return Ok(0);
    };
    let Some(value) = value.as_str() else {
        return Err(unsupported_crs(function_name, &value.to_string()));
    };

    let srid = match metadata.crs().crs_type() {
        Some(CrsType::AuthorityCode) => value
            .strip_prefix("EPSG:")
            .ok_or_else(|| unsupported_crs(function_name, value))?,
        Some(CrsType::Srid) => value,
        _ => return Err(unsupported_crs(function_name, value)),
    };
    let value = srid
        .parse::<i64>()
        .map_err(|_| unsupported_crs(function_name, value))?;
    normalize_srid(value)
}

fn unsupported_crs(function_name: &str, value: &str) -> GeoDataFusionError {
    DataFusionError::NotImplemented(format!(
        "{function_name} requires an EPSG authority code, got {value}"
    ))
    .into()
}

/// Read the SRID declared by a GeoArrow field.
pub(crate) fn field_srid(field: &Field, function_name: &str) -> GeoDataFusionResult<i32> {
    let geoarrow_type = GeoArrowType::from_arrow_field(field)?;
    srid_from_metadata(geoarrow_type.metadata(), function_name)
}

/// The SRID argument of a UDF at execution time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SridArgument {
    /// No SRID argument was provided; the output field metadata determines
    /// the CRS.
    Absent,
    /// The SRID argument is NULL, so every output row is NULL.
    Null,
    /// The SRID argument carries one SRID for the whole field.
    Value(i32),
}

impl SridArgument {
    pub(crate) fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }
}

/// Resolve and validate the SRID argument of a UDF at execution time.
///
/// The FFI boundary materializes scalar arguments as constant arrays, so a
/// constant array whose normalized value matches the CRS already declared on
/// the output field is still treated as a scalar. Per-row SRIDs cannot be
/// represented by Arrow field metadata and stay unsupported.
pub(crate) fn resolve_srid_argument(
    args: &ScalarFunctionArgs,
    srid_index: usize,
    function_name: &str,
) -> GeoDataFusionResult<SridArgument> {
    let Some(argument) = args.args.get(srid_index) else {
        return Ok(SridArgument::Absent);
    };

    match argument {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(value))) => {
            resolve_srid_value(*value, &args.return_field, function_name)
        }
        ColumnarValue::Scalar(ScalarValue::Int32(Some(value))) => {
            resolve_srid_value(i64::from(*value), &args.return_field, function_name)
        }
        ColumnarValue::Scalar(
            ScalarValue::Int64(None) | ScalarValue::Int32(None) | ScalarValue::Null,
        ) => Ok(SridArgument::Null),
        ColumnarValue::Scalar(other) => Err(DataFusionError::Internal(format!(
            "{function_name} received a non-integer SRID scalar {other}"
        ))
        .into()),
        ColumnarValue::Array(array) => resolve_srid_array(array, &args.return_field, function_name),
    }
}

fn resolve_srid_array(
    array: &arrow_array::ArrayRef,
    return_field: &Field,
    function_name: &str,
) -> GeoDataFusionResult<SridArgument> {
    match array.data_type() {
        DataType::Null => Ok(SridArgument::Null),
        DataType::Int64 => resolve_srid_values(
            array.as_primitive::<Int64Type>().iter(),
            return_field,
            function_name,
        ),
        DataType::Int32 => resolve_srid_values(
            array
                .as_primitive::<Int32Type>()
                .iter()
                .map(|value| value.map(i64::from)),
            return_field,
            function_name,
        ),
        other => Err(DataFusionError::Internal(format!(
            "{function_name} received a non-integer SRID argument of type {other}"
        ))
        .into()),
    }
}

fn resolve_srid_value(
    value: i64,
    return_field: &Field,
    function_name: &str,
) -> GeoDataFusionResult<SridArgument> {
    let srid = normalize_srid(value)?;
    let declared = field_srid(return_field, function_name)?;
    if srid != declared {
        return Err(scalar_srid_error(function_name));
    }
    Ok(SridArgument::Value(srid))
}

fn resolve_srid_values(
    values: impl Iterator<Item = Option<i64>>,
    return_field: &Field,
    function_name: &str,
) -> GeoDataFusionResult<SridArgument> {
    let mut value = None;
    let mut saw_null = false;
    for item in values {
        match item {
            None => saw_null = true,
            Some(item) => match value {
                Some(existing) if existing != item => {
                    return Err(scalar_srid_error(function_name));
                }
                Some(_) => {}
                None => value = Some(item),
            },
        }
    }

    match (value, saw_null) {
        (None, _) => Ok(SridArgument::Null),
        (Some(value), false) => resolve_srid_value(value, return_field, function_name),
        (Some(_), true) => Err(scalar_srid_error(function_name)),
    }
}

fn scalar_srid_error(function_name: &str) -> GeoDataFusionError {
    DataFusionError::Execution(format!(
        "{function_name} requires SRID to be a scalar literal"
    ))
    .into()
}

pub(crate) fn field_metadata(field: &Field) -> GeoDataFusionResult<Arc<Metadata>> {
    if field.data_type() == &DataType::Null {
        return Ok(Arc::new(Metadata::default()));
    }
    Ok(Arc::new(Metadata::try_from(field)?))
}

/// Validates constraints that `StrictGeometryBuilder` relies on before pushing a geometry.
pub(crate) fn validate_geometry_for_strict_builder(
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoArrowResult<()> {
    let expected = geometry.dim();
    validate_geometry_for_strict_builder_with_expected(geometry, expected)
}

fn validate_geometry_for_strict_builder_with_expected(
    geometry: &impl GeometryTrait<T = f64>,
    expected: Dimensions,
) -> GeoArrowResult<()> {
    if !matches!(
        expected,
        Dimensions::Xy | Dimensions::Xyz | Dimensions::Xym | Dimensions::Xyzm
    ) {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "unsupported geometry dimension {expected:?}"
        )));
    }

    // Empty geometries carry no coordinates. GEOS emits 2D empty members inside
    // Z collections, and the builders fill those members using the enclosing
    // dimension, so their declared dimension is not an invariant to enforce.
    let empty = is_empty_geometry(geometry);
    if !empty && geometry.dim() != expected {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "geometry dimension {:?} does not match enclosing dimension {expected:?}",
            geometry.dim()
        )));
    }

    match geometry.as_type() {
        NativeGeometryType::Point(point) => {
            if let Some(coord) = point.coord() {
                validate_coord_dimension(expected, coord.dim())?;
            }
        }
        NativeGeometryType::LineString(line) => {
            validate_coord_dimensions(expected, line.coords())?;
        }
        NativeGeometryType::Polygon(polygon) => {
            if let Some(exterior) = polygon.exterior() {
                validate_coord_dimensions(expected, exterior.coords())?;
            }
            for interior in polygon.interiors() {
                validate_coord_dimensions(expected, interior.coords())?;
            }
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            for point in multi_point.points() {
                if is_empty_geometry(&point) {
                    continue;
                }
                if point.dim() != expected {
                    return Err(GeoArrowError::InvalidGeoArrow(format!(
                        "point dimension {:?} does not match enclosing dimension {expected:?}",
                        point.dim()
                    )));
                }
                if let Some(coord) = point.coord() {
                    validate_coord_dimension(expected, coord.dim())?;
                }
            }
        }
        NativeGeometryType::MultiLineString(multi_line) => {
            for line in multi_line.line_strings() {
                if is_empty_geometry(&line) {
                    continue;
                }
                if line.dim() != expected {
                    return Err(GeoArrowError::InvalidGeoArrow(format!(
                        "line string dimension {:?} does not match enclosing dimension {expected:?}",
                        line.dim()
                    )));
                }
                validate_coord_dimensions(expected, line.coords())?;
            }
        }
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            for polygon in multi_polygon.polygons() {
                if is_empty_geometry(&polygon) {
                    continue;
                }
                if polygon.dim() != expected {
                    return Err(GeoArrowError::InvalidGeoArrow(format!(
                        "polygon dimension {:?} does not match enclosing dimension {expected:?}",
                        polygon.dim()
                    )));
                }
                if let Some(exterior) = polygon.exterior() {
                    validate_coord_dimensions(expected, exterior.coords())?;
                }
                for interior in polygon.interiors() {
                    validate_coord_dimensions(expected, interior.coords())?;
                }
            }
        }
        NativeGeometryType::GeometryCollection(collection) => {
            for child in collection.geometries() {
                if matches!(child.as_type(), NativeGeometryType::GeometryCollection(_)) {
                    return Err(GeoArrowError::InvalidGeoArrow(
                        "nested GeometryCollection is not supported".to_string(),
                    ));
                }
                if is_empty_geometry(&child) {
                    continue;
                }
                validate_geometry_for_strict_builder_with_expected(&child, expected)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn is_empty_geometry(geometry: &impl GeometryTrait<T = f64>) -> bool {
    match geometry.as_type() {
        NativeGeometryType::Point(point) => point.coord().is_none(),
        NativeGeometryType::LineString(line_string) => line_string.num_coords() == 0,
        NativeGeometryType::Polygon(polygon) => {
            polygon.exterior().is_none_or(|ring| ring.num_coords() == 0)
                && polygon.interiors().all(|ring| ring.num_coords() == 0)
        }
        NativeGeometryType::MultiPoint(multi_point) => {
            multi_point.points().all(|point| point.coord().is_none())
        }
        NativeGeometryType::MultiLineString(multi_line_string) => multi_line_string
            .line_strings()
            .all(|line_string| line_string.num_coords() == 0),
        NativeGeometryType::MultiPolygon(multi_polygon) => {
            multi_polygon.polygons().all(|polygon| {
                polygon.exterior().is_none_or(|ring| ring.num_coords() == 0)
                    && polygon.interiors().all(|ring| ring.num_coords() == 0)
            })
        }
        NativeGeometryType::GeometryCollection(collection) => collection
            .geometries()
            .all(|child| is_empty_geometry(&child)),
        NativeGeometryType::Rect(_)
        | NativeGeometryType::Triangle(_)
        | NativeGeometryType::Line(_) => false,
    }
}

fn validate_coord_dimensions(
    expected: Dimensions,
    coords: impl Iterator<Item = impl CoordTrait<T = f64>>,
) -> GeoArrowResult<()> {
    for coord in coords {
        validate_coord_dimension(expected, coord.dim())?;
    }
    Ok(())
}

fn validate_coord_dimension(expected: Dimensions, actual: Dimensions) -> GeoArrowResult<()> {
    if expected == actual {
        return Ok(());
    }

    Err(GeoArrowError::InvalidGeoArrow(format!(
        "coordinate dimension {actual:?} does not match geometry dimension {expected:?}"
    )))
}

pub(crate) fn field_metadata_or_default(field: &Field) -> GeoDataFusionResult<Arc<Metadata>> {
    if field.extension_type_name().is_none() {
        return Ok(Arc::new(Metadata::default()));
    }
    field_metadata(field)
}

/// Returns true when an argument is a NULL literal, including DataFusion's
/// coerced representation of NULL as an all-null Struct without GeoArrow
/// extension metadata.
pub(crate) fn is_null_geometry_input(field: &Field, array: &dyn Array) -> bool {
    array.data_type() == &DataType::Null
        || (field.extension_type_name().is_none() && array.null_count() == array.len())
}

pub(crate) fn merge_metadata<'a>(
    fields: impl IntoIterator<Item = &'a Field>,
    function_name: &str,
) -> GeoDataFusionResult<Arc<Metadata>> {
    let mut output: Option<Arc<Metadata>> = None;
    for field in fields {
        // A SQL NULL literal has no geometry extension type. It is a strict
        // input, not an unknown real CRS, so it must not conflict with a
        // non-null geometry argument's metadata.
        if is_null_geometry_field(field) {
            continue;
        }
        let metadata = field_metadata(field)?;
        match &output {
            Some(current) if !metadata_compatible(current, &metadata) => {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "{function_name}: operation on mixed SRID geometries"
                ))
                .into());
            }
            Some(_) => {}
            None => output = Some(metadata),
        }
    }
    Ok(normalize_metadata(output.unwrap_or_default()))
}

/// Compare normalized CRS identity and edge interpretation.
///
/// `None` edges are the GeoArrow default (planar/linear), not a wildcard.
pub(crate) fn metadata_compatible(left: &Metadata, right: &Metadata) -> bool {
    let left_unknown = metadata_is_unknown(left);
    let right_unknown = metadata_is_unknown(right);
    let crs_compatible = if left_unknown || right_unknown {
        left_unknown && right_unknown
    } else {
        left.crs() == right.crs()
    };

    crs_compatible && left.edges() == right.edges()
}

fn metadata_is_unknown(metadata: &Metadata) -> bool {
    let Some(value) = metadata.crs().crs_value() else {
        return true;
    };
    matches!(value.as_str(), Some("EPSG:0" | "0"))
}

/// Normalize only the CRS of unknown SRIDs, retaining edge interpretation.
fn normalize_metadata(metadata: Arc<Metadata>) -> Arc<Metadata> {
    if metadata_is_unknown(&metadata) {
        Arc::new(Metadata::new(Crs::default(), metadata.edges()))
    } else {
        metadata
    }
}

/// Rewrites empty MultiPolygon members to zero-ring polygons.
///
/// GeoArrow's builder needs an empty exterior ring to avoid panicking, but that
/// representation has one ring. Rebuild the polygon offsets so each semantic
/// empty member repeats its offset and contributes no ring.
pub(crate) fn repair_multi_polygon_array(
    array: &MultiPolygonArray,
) -> GeoArrowResult<MultiPolygonArray> {
    let mut polygon_offsets = vec![0_i32];
    let mut ring_offsets = vec![0_i32];
    let mut ring_count = 0_i32;
    let mut ring_index = 0_i32;

    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        for polygon in array.value(row)?.polygons() {
            let exterior = polygon.exterior();
            let is_empty = exterior.as_ref().is_none_or(|ring| ring.num_coords() == 0)
                && polygon.num_interiors() == 0;
            if is_empty {
                polygon_offsets.push(ring_index);
                continue;
            }

            let mut num_rings = 0_i32;
            if let Some(exterior) = exterior {
                ring_count = add_ring_coords(ring_count, exterior.num_coords())?;
                ring_offsets.push(ring_count);
                num_rings += 1;
            }
            for interior in polygon.interiors() {
                ring_count = add_ring_coords(ring_count, interior.num_coords())?;
                ring_offsets.push(ring_count);
                num_rings += 1;
            }
            ring_index += num_rings;
            polygon_offsets.push(ring_index);
        }
    }

    MultiPolygonArray::try_new(
        array.coords().clone(),
        array.geom_offsets().clone(),
        OffsetBuffer::new(ScalarBuffer::from(polygon_offsets)),
        OffsetBuffer::new(ScalarBuffer::from(ring_offsets)),
        array.logical_nulls(),
        array.data_type().metadata().clone(),
    )
}

fn add_ring_coords(total: i32, num_coords: usize) -> GeoArrowResult<i32> {
    let num_coords = i32::try_from(num_coords).map_err(|_| {
        GeoArrowError::InvalidGeoArrow(
            "ring has more coordinates than GeoArrow can encode".to_string(),
        )
    })?;
    total.checked_add(num_coords).ok_or_else(|| {
        GeoArrowError::InvalidGeoArrow("ring offset exceeds GeoArrow's integer range".to_string())
    })
}

fn rebuild_geometry_collection_array(
    array: GeometryCollectionArray,
    map_multi_polygon: impl Fn(&MultiPolygonArray) -> GeoArrowResult<MultiPolygonArray>,
) -> GeoArrowResult<GeometryCollectionArray> {
    let GeoArrowType::GeometryCollection(typ) = array.data_type() else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "expected a GeometryCollectionArray".to_string(),
        ));
    };
    let arrow = array.to_array_ref();
    let list = arrow
        .as_any()
        .downcast_ref::<GenericListArray<i32>>()
        .ok_or_else(|| {
            GeoArrowError::InvalidGeoArrow(
                "GeometryCollectionArray was not backed by ListArray".to_string(),
            )
        })?;
    let union = list.values().as_union();
    let dimension = typ.dimension();
    let dimension_order = match dimension {
        Dimension::XY => 0_i8,
        Dimension::XYZ => 1,
        Dimension::XYM => 2,
        Dimension::XYZM => 3,
    };
    let multi_polygon_type_id = dimension_order * 10 + MultiPolygonType::GEOMETRY_TYPE_OFFSET;
    if !union.type_ids().contains(&multi_polygon_type_id) {
        return Ok(array);
    }

    let mut children = Vec::with_capacity(union.fields().len());
    for (type_id, _field) in union.fields().iter() {
        let child = union.child(type_id);
        if type_id % 10 == MultiPolygonType::GEOMETRY_TYPE_OFFSET && type_id / 10 == dimension_order
        {
            let multi_polygon = MultiPolygonArray::try_from((
                child.as_ref(),
                MultiPolygonType::new(dimension, Default::default())
                    .with_coord_type(typ.coord_type()),
            ))?;
            children.push(map_multi_polygon(&multi_polygon)?.into_array_ref());
        } else {
            children.push(child.clone());
        }
    }

    let repaired_union = UnionArray::try_new(
        union.fields().clone(),
        union.type_ids().clone(),
        union.offsets().cloned(),
        children,
    )
    .map_err(|error| {
        GeoArrowError::InvalidGeoArrow(format!(
            "failed to rebuild GeometryCollection union: {error}"
        ))
    })?;
    let values_field = match list.data_type() {
        DataType::List(field) => field.clone(),
        _ => {
            return Err(GeoArrowError::InvalidGeoArrow(
                "GeometryCollectionArray was not backed by a List type".to_string(),
            ));
        }
    };
    let repaired_list = GenericListArray::new(
        values_field,
        list.offsets().clone(),
        Arc::new(repaired_union),
        list.nulls().cloned(),
    );
    GeometryCollectionArray::try_from((&repaired_list, typ)).map_err(|error| {
        GeoArrowError::InvalidGeoArrow(format!(
            "failed to rebuild GeometryCollection list: {error}"
        ))
    })
}

/// Rebuilds a GeometryCollectionArray so nested MultiPolygon children use
/// zero-ring empty members instead of GeoArrow's builder-only empty ring.
pub(crate) fn repair_geometry_collection_array(
    array: GeometryCollectionArray,
) -> GeoArrowResult<GeometryCollectionArray> {
    rebuild_geometry_collection_array(array, repair_multi_polygon_array)
}

fn repair_geometry_array(array: GeometryArray) -> GeoArrowResult<GeometryArray> {
    let [xy, xyz, xym, xyzm] = [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ];
    let multi_polygons = [
        repair_multi_polygon_array(&array.multi_polygon_child(xy))?,
        repair_multi_polygon_array(&array.multi_polygon_child(xyz))?,
        repair_multi_polygon_array(&array.multi_polygon_child(xym))?,
        repair_multi_polygon_array(&array.multi_polygon_child(xyzm))?,
    ];
    let geometry_collections = [
        repair_geometry_collection_array(array.geometry_collection_child(xy))?,
        repair_geometry_collection_array(array.geometry_collection_child(xyz))?,
        repair_geometry_collection_array(array.geometry_collection_child(xym))?,
        repair_geometry_collection_array(array.geometry_collection_child(xyzm))?,
    ];
    let metadata = array.data_type().metadata().clone();

    Ok(GeometryArray::new(
        array.type_ids().clone(),
        array.offsets().clone(),
        [
            array.point_child(xy),
            array.point_child(xyz),
            array.point_child(xym),
            array.point_child(xyzm),
        ],
        [
            array.line_string_child(xy),
            array.line_string_child(xyz),
            array.line_string_child(xym),
            array.line_string_child(xyzm),
        ],
        [
            array.polygon_child(xy),
            array.polygon_child(xyz),
            array.polygon_child(xym),
            array.polygon_child(xyzm),
        ],
        [
            array.multi_point_child(xy),
            array.multi_point_child(xyz),
            array.multi_point_child(xym),
            array.multi_point_child(xyzm),
        ],
        [
            array.multi_line_string_child(xy),
            array.multi_line_string_child(xyz),
            array.multi_line_string_child(xym),
            array.multi_line_string_child(xyzm),
        ],
        multi_polygons,
        geometry_collections,
        metadata,
    ))
}

/// Builds a generic GeoArrow Geometry array without flattening GeometryCollections.
///
/// GeoArrow's generic `GeometryBuilder` currently represents a single-member
/// `GeometryCollection` as its child geometry. This builder keeps the collection in its
/// dedicated union child while delegating all other construction and null handling to
/// GeoArrow's builder.
#[derive(Debug)]
pub(crate) struct StrictGeometryBuilder {
    inner: GeometryBuilder,
    geometry_collections: [GeometryCollectionBuilder; 4],
    collection_lengths: [usize; 4],
    rows: Vec<StrictRow>,
    len: usize,
}

#[derive(Debug, Clone, Copy)]
enum StrictRow {
    Inner,
    Collection {
        dimension_index: usize,
        collection_index: i32,
    },
}

impl StrictGeometryBuilder {
    pub(crate) fn new(typ: GeometryType) -> Self {
        let coord_type = typ.coord_type();
        let metadata = typ.metadata().clone();
        let geometry_collections = std::array::from_fn(|index| {
            let dimension = dimension_from_index(index);
            GeometryCollectionBuilder::new(
                GeometryCollectionType::new(dimension, Default::default())
                    .with_coord_type(coord_type),
            )
        });

        Self {
            inner: GeometryBuilder::new(GeometryType::new(metadata).with_coord_type(coord_type)),
            geometry_collections,
            collection_lengths: [0; 4],
            rows: Vec::new(),
            len: 0,
        }
    }

    pub(crate) fn push_geometry(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        let Some(geometry) = geometry else {
            self.inner
                .push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            self.rows.push(StrictRow::Inner);
            self.len += 1;
            return Ok(());
        };

        validate_geometry_for_strict_builder(geometry)?;

        let normalized = normalize_empty_multipolygon_members(geometry);
        if let Some(owned) = normalized.as_ref() {
            let geo_traits::GeometryType::GeometryCollection(collection) = owned.as_type() else {
                self.inner.push_geometry(Some(owned))?;
                self.rows.push(StrictRow::Inner);
                self.len += 1;
                return Ok(());
            };
            return self.push_collection(collection);
        }

        let geo_traits::GeometryType::GeometryCollection(collection) = geometry.as_type() else {
            self.inner.push_geometry(Some(geometry))?;
            self.rows.push(StrictRow::Inner);
            self.len += 1;
            return Ok(());
        };
        self.push_collection(collection)
    }

    fn push_collection(&mut self, collection: &impl GeometryTrait<T = f64>) -> GeoArrowResult<()> {
        let (dimension_index, _) = dimension_from_geo_traits(collection.dim())?;
        let collection_index = self.collection_lengths[dimension_index];

        self.geometry_collections[dimension_index].push_geometry(Some(collection))?;
        self.collection_lengths[dimension_index] += 1;
        self.rows.push(StrictRow::Collection {
            dimension_index,
            collection_index: collection_index as i32,
        });
        self.len += 1;
        Ok(())
    }

    pub(crate) fn finish(self) -> GeometryArray {
        let inner = repair_geometry_array(self.inner.finish())
            .expect("strict geometry builder produced an invalid array");
        let mut type_ids = Vec::with_capacity(self.len);
        let mut offsets = Vec::with_capacity(self.len);
        let mut inner_row = 0;
        for row in &self.rows {
            match row {
                StrictRow::Inner => {
                    type_ids.push(inner.type_ids()[inner_row]);
                    offsets.push(inner.offsets()[inner_row]);
                    inner_row += 1;
                }
                StrictRow::Collection {
                    dimension_index,
                    collection_index,
                } => {
                    type_ids.push(self.geometry_collections[*dimension_index].geometry_type_id());
                    offsets.push(*collection_index);
                }
            }
        }
        assert_eq!(inner_row, inner.type_ids().len());

        let [xy, xyz, xym, xyzm] = [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ];
        let metadata = inner.data_type().metadata().clone();
        let geometry_collections = self.geometry_collections.map(|builder| {
            repair_geometry_collection_array(builder.finish())
                .expect("strict geometry builder produced an invalid GeometryCollection")
        });
        GeometryArray::new(
            ScalarBuffer::from(type_ids),
            ScalarBuffer::from(offsets),
            [xy, xyz, xym, xyzm].map(|dim| inner.point_child(dim)),
            [xy, xyz, xym, xyzm].map(|dim| inner.line_string_child(dim)),
            [xy, xyz, xym, xyzm].map(|dim| inner.polygon_child(dim)),
            [xy, xyz, xym, xyzm].map(|dim| inner.multi_point_child(dim)),
            [xy, xyz, xym, xyzm].map(|dim| inner.multi_line_string_child(dim)),
            [xy, xyz, xym, xyzm].map(|dim| inner.multi_polygon_child(dim)),
            geometry_collections,
            metadata,
        )
    }
}

pub(crate) fn null_geometry_array(typ: GeometryType, len: usize) -> GeoDataFusionResult<ArrayRef> {
    let mut builder = StrictGeometryBuilder::new(typ);
    for _ in 0..len {
        builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
    }
    Ok(Arc::new(builder.finish()).to_array_ref())
}

/// Expand a GeoArrow `Rect` array into the polygon that PostGIS exposes for `box2d`.
///
/// The `wkb` crate's Rect writer omits the ring point count, and the WKB, EWKB,
/// and GEOS writers otherwise only accept the seven GeoArrow geometry families.
/// Encoding a Box as a polygon in one shared place keeps those paths consistent.
///
/// Returns `None` when the input is not a Box array.
pub(crate) fn expand_rect_to_geometry(
    array: &dyn GeoArrowArray,
) -> GeoDataFusionResult<Option<GeometryArray>> {
    let GeoArrowType::Rect(typ) = array.data_type() else {
        return Ok(None);
    };

    let output_type =
        GeometryType::new(typ.metadata().clone()).with_coord_type(CoordType::Interleaved);
    let mut builder = StrictGeometryBuilder::new(output_type);
    for value in array.as_rect().iter() {
        match value.transpose()? {
            Some(rect) => builder.push_geometry(Some(&rect_to_polygon(&rect)?))?,
            None => builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?,
        }
    }
    Ok(Some(builder.finish()))
}

/// Convert one GeoArrow Rect into the POLYGON that PostGIS exposes for `box2d`.
///
/// PostGIS turns a full `box3d` with distinct minimum and maximum Z into a
/// POLYHEDRALSURFACE, which GeoArrow has no geometry family for. Reject that case
/// instead of silently flattening the box; a constant-Z box is a POLYGON Z and is
/// supported.
pub(crate) fn rect_to_polygon(rect: &impl RectTrait<T = f64>) -> GeoDataFusionResult<Wkt<f64>> {
    let dimension = rect_wkt_dimension(rect.dim())?;
    let min = rect.min();
    let max = rect.max();
    if matches!(dimension, WktDimension::XYZ | WktDimension::XYZM) && min.nth(2) != max.nth(2) {
        return Err(DataFusionError::NotImplemented(
            "3D Box inputs require a constant Z value; GeoArrow cannot represent the POLYHEDRALSURFACE that PostGIS builds from a full box3d".to_string(),
        )
        .into());
    }

    let lower_left = rect_coord(&min, min.x(), min.y(), dimension);
    let upper_left = rect_coord(&min, min.x(), max.y(), dimension);
    let upper_right = rect_coord(&max, max.x(), max.y(), dimension);
    let lower_right = rect_coord(&min, max.x(), min.y(), dimension);
    Ok(Wkt::Polygon(Polygon::new(
        vec![LineString::new(
            vec![lower_left, upper_left, upper_right, lower_right, lower_left],
            dimension,
        )],
        dimension,
    )))
}

fn rect_coord(
    coord: &impl CoordTrait<T = f64>,
    x: f64,
    y: f64,
    dimension: WktDimension,
) -> Coord<f64> {
    let z = match dimension {
        WktDimension::XYZ | WktDimension::XYZM => coord.nth(2),
        _ => None,
    };
    let m = match dimension {
        WktDimension::XYM => coord.nth(2),
        WktDimension::XYZM => coord.nth(3),
        _ => None,
    };
    Coord { x, y, z, m }
}

fn rect_wkt_dimension(dimension: Dimensions) -> GeoDataFusionResult<WktDimension> {
    match dimension {
        Dimensions::Xy | Dimensions::Unknown(2) => Ok(WktDimension::XY),
        Dimensions::Xyz | Dimensions::Unknown(3) => Ok(WktDimension::XYZ),
        Dimensions::Xym => Ok(WktDimension::XYM),
        Dimensions::Xyzm | Dimensions::Unknown(4) => Ok(WktDimension::XYZM),
        Dimensions::Unknown(other) => Err(DataFusionError::NotImplemented(format!(
            "GeoArrow cannot represent a {other}-dimensional Box"
        ))
        .into()),
    }
}

fn dimension_from_geo_traits(dim: Dimensions) -> GeoArrowResult<(usize, Dimension)> {
    match dim {
        Dimensions::Xy => Ok((0, Dimension::XY)),
        Dimensions::Xyz => Ok((1, Dimension::XYZ)),
        Dimensions::Xym => Ok((2, Dimension::XYM)),
        Dimensions::Xyzm => Ok((3, Dimension::XYZM)),
        Dimensions::Unknown(_) => Err(GeoArrowError::InvalidGeoArrow(format!(
            "unsupported geometry dimensions: {dim:?}"
        ))),
    }
}

fn dimension_from_index(index: usize) -> Dimension {
    match index {
        0 => Dimension::XY,
        1 => Dimension::XYZ,
        2 => Dimension::XYM,
        3 => Dimension::XYZM,
        _ => unreachable!("GeoArrow has four dimensions"),
    }
}

#[cfg(test)]
mod test {
    use arrow_array::Int64Array;
    use datafusion::config::ConfigOptions;
    use geoarrow_schema::{CoordType, Dimension, Edges, PointType};

    use super::*;

    fn point_field(metadata: Arc<Metadata>) -> Field {
        PointType::new(Dimension::XY, metadata)
            .with_coord_type(CoordType::Separated)
            .to_field("geom", true)
    }

    fn srid_args(return_field: Field, args: Vec<ColumnarValue>) -> ScalarFunctionArgs {
        ScalarFunctionArgs {
            args,
            arg_fields: vec![Arc::new(Field::new("srid", DataType::Int64, true))],
            number_rows: 2,
            return_field: Arc::new(return_field),
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    #[test]
    fn srid_argument_accepts_ffi_constant_array() {
        let args = srid_args(
            point_field(metadata_for_srid(4326)),
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                4326, 4326,
            ])))],
        );

        assert_eq!(
            resolve_srid_argument(&args, 0, "ST_Point").unwrap(),
            SridArgument::Value(4326)
        );
    }

    #[test]
    fn srid_argument_absent_skips_crs_parsing() {
        let args = srid_args(
            point_field(Arc::new(Metadata::new(
                Crs::from_authority_code("OGC:CRS84".to_string()),
                None,
            ))),
            vec![],
        );

        assert_eq!(
            resolve_srid_argument(&args, 0, "ST_Point").unwrap(),
            SridArgument::Absent
        );
    }

    #[test]
    fn srid_argument_null_skips_crs_parsing() {
        let args = srid_args(
            point_field(Arc::new(Metadata::new(
                Crs::from_authority_code("OGC:CRS84".to_string()),
                None,
            ))),
            vec![ColumnarValue::Scalar(ScalarValue::Null)],
        );

        assert_eq!(
            resolve_srid_argument(&args, 0, "ST_Point").unwrap(),
            SridArgument::Null
        );
    }

    #[test]
    fn srid_argument_rejects_varying_array() {
        let args = srid_args(
            point_field(metadata_for_srid(4326)),
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                4326, 3857,
            ])))],
        );

        assert!(resolve_srid_argument(&args, 0, "ST_Point").is_err());
    }

    #[test]
    fn srid_argument_rejects_mixed_null_array() {
        let args = srid_args(
            point_field(metadata_for_srid(4326)),
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                Some(4326),
                None,
            ])))],
        );

        assert!(resolve_srid_argument(&args, 0, "ST_Point").is_err());
    }

    #[test]
    fn srid_argument_rejects_undeclared_crs() {
        let args = srid_args(
            point_field(Arc::new(Metadata::default())),
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![4326])))],
        );

        assert!(resolve_srid_argument(&args, 0, "ST_Point").is_err());
    }

    #[test]
    fn srid_argument_treats_null_array_as_null() {
        let args = srid_args(
            point_field(Arc::new(Metadata::default())),
            vec![ColumnarValue::Array(Arc::new(Int64Array::from(vec![
                None, None,
            ])))],
        );

        assert_eq!(
            resolve_srid_argument(&args, 0, "ST_Point").unwrap(),
            SridArgument::Null
        );
    }

    #[test]
    fn srid_argument_normalizes_scalar_value() {
        let args = srid_args(
            point_field(metadata_for_srid(999001)),
            vec![ColumnarValue::Scalar(ScalarValue::Int64(Some(1000000)))],
        );

        assert_eq!(
            resolve_srid_argument(&args, 0, "ST_Point").unwrap(),
            SridArgument::Value(999001)
        );
    }

    #[test]
    fn unknown_crs_normalization_preserves_edges() {
        let field = point_field(Arc::new(Metadata::new(
            Crs::default(),
            Some(Edges::Spherical),
        )));
        let merged = merge_metadata([&field], "test").unwrap();

        assert!(merged.crs().crs_value().is_none());
        assert_eq!(merged.edges(), Some(Edges::Spherical));
    }

    #[test]
    fn explicit_epsg_zero_normalization_preserves_edges() {
        let field = point_field(Arc::new(Metadata::new(
            Crs::from_authority_code("EPSG:0".to_string()),
            Some(Edges::Spherical),
        )));
        let merged = merge_metadata([&field], "test").unwrap();

        assert!(merged.crs().crs_value().is_none());
        assert_eq!(merged.edges(), Some(Edges::Spherical));
    }

    #[test]
    fn metadata_compatibility_checks_crs_and_edges() {
        let unknown_planar = point_field(Arc::new(Metadata::default()));
        let unknown_spherical = point_field(Arc::new(Metadata::new(
            Crs::default(),
            Some(Edges::Spherical),
        )));
        let known_4326 = point_field(Arc::new(Metadata::new(
            Crs::from_authority_code("EPSG:4326".to_string()),
            None,
        )));

        assert!(merge_metadata([&unknown_planar, &unknown_planar], "test").is_ok());
        assert!(merge_metadata([&unknown_spherical, &unknown_spherical], "test").is_ok());
        assert!(merge_metadata([&unknown_planar, &unknown_spherical], "test").is_err());
        assert!(merge_metadata([&unknown_planar, &known_4326], "test").is_err());
        assert!(!metadata_compatible(
            &field_metadata(&unknown_planar).unwrap(),
            &field_metadata(&unknown_spherical).unwrap()
        ));
    }

    #[test]
    fn null_literal_does_not_conflict_with_known_crs() {
        let known = point_field(Arc::new(Metadata::new(
            Crs::from_authority_code("EPSG:4326".to_string()),
            None,
        )));
        let null = Field::new("null", DataType::Null, true);

        let merged = merge_metadata([&null, &known], "test").unwrap();
        assert_eq!(
            merged.crs().crs_value().and_then(|value| value.as_str()),
            Some("EPSG:4326")
        );

        let all_null = merge_metadata([&null], "test").unwrap();
        assert!(all_null.crs().crs_value().is_none());
    }
}
