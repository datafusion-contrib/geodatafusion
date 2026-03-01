//! Additional geometry accessor functions: ST_IsEmpty, ST_IsClosed, ST_IsSimple,
//! ST_IsRing, ST_IsCollection, ST_Dimension, ST_NumGeometries, ST_HasZ, ST_HasM,
//! ST_NRings.

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::{BooleanBuilder, Int32Builder, UInt32Builder};
use arrow_array::{ArrayRef, BooleanArray, Int32Array, UInt32Array};
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::*;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::Dimension;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

// =========================================================================
// ST_IsEmpty
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsEmpty;

impl Default for IsEmpty {
    fn default() -> Self {
        Self
    }
}

static IS_EMPTY_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsEmpty {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_isempty"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(is_empty_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(IS_EMPTY_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true if the geometry is an empty geometry (has no points).",
                "ST_IsEmpty(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn is_empty_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = is_empty_array(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn is_empty_array(array: &dyn GeoArrowArray) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(array, _is_empty_impl)
}

fn _is_empty_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            let g = geom?;
            let empty = match g.as_type() {
                GeometryType::Point(p) => p.coord().is_none(),
                GeometryType::LineString(ls) => ls.num_coords() == 0,
                GeometryType::Polygon(p) => {
                    p.exterior().map_or(true, |ext| ext.num_coords() == 0)
                }
                GeometryType::MultiPoint(mp) => mp.num_points() == 0,
                GeometryType::MultiLineString(mls) => mls.num_line_strings() == 0,
                GeometryType::MultiPolygon(mp) => mp.num_polygons() == 0,
                GeometryType::GeometryCollection(gc) => gc.num_geometries() == 0,
                _ => false,
            };
            builder.append_value(empty);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

// =========================================================================
// ST_IsClosed
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsClosed;

impl Default for IsClosed {
    fn default() -> Self {
        Self
    }
}

static IS_CLOSED_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsClosed {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_isclosed"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(is_closed_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(IS_CLOSED_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true if the geometry's start and end points are coincident.",
                "ST_IsClosed(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn is_closed_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = is_closed_array(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn is_closed_array(array: &dyn GeoArrowArray) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(array, _is_closed_impl)
}

fn _is_closed_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<BooleanArray> {
    use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let closed = is_geo_closed(&geo_geom);
            builder.append_value(closed);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn is_geo_closed(geom: &geo::Geometry) -> bool {
    match geom {
        geo::Geometry::Point(_) => true,
        geo::Geometry::LineString(ls) => is_linestring_closed(ls),
        geo::Geometry::Polygon(_) => true,
        geo::Geometry::MultiLineString(mls) => mls.0.iter().all(is_linestring_closed),
        _ => true,
    }
}

fn is_linestring_closed(ls: &geo::LineString) -> bool {
    if ls.0.len() < 2 {
        return false;
    }
    let first = ls.0.first().unwrap();
    let last = ls.0.last().unwrap();
    first.x == last.x && first.y == last.y
}

// =========================================================================
// ST_IsSimple
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsSimple;

impl Default for IsSimple {
    fn default() -> Self {
        Self
    }
}

static IS_SIMPLE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsSimple {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_issimple"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(is_simple_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(IS_SIMPLE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true if the geometry has no anomalous geometric points.",
                "ST_IsSimple(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn is_simple_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = is_simple_array(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn is_simple_array(array: &dyn GeoArrowArray) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(array, _is_simple_impl)
}

fn _is_simple_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<BooleanArray> {
    use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let simple = is_geometry_simple(&geo_geom);
            builder.append_value(simple);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn is_geometry_simple(geom: &geo::Geometry) -> bool {
    match geom {
        geo::Geometry::Point(_) => true,
        geo::Geometry::MultiPoint(mp) => {
            // Simple if no two points are equal
            for i in 0..mp.0.len() {
                for j in (i + 1)..mp.0.len() {
                    if mp.0[i] == mp.0[j] {
                        return false;
                    }
                }
            }
            true
        }
        geo::Geometry::LineString(ls) => is_linestring_simple(ls),
        geo::Geometry::MultiLineString(mls) => mls.0.iter().all(is_linestring_simple),
        geo::Geometry::Polygon(_) | geo::Geometry::MultiPolygon(_) => true,
        geo::Geometry::GeometryCollection(gc) => gc.0.iter().all(is_geometry_simple),
        _ => true,
    }
}

fn is_linestring_simple(ls: &geo::LineString) -> bool {
    use geo::line_intersection::{line_intersection, LineIntersection};

    if ls.0.len() < 2 {
        return true;
    }
    let lines: Vec<geo::Line> = ls.lines().collect();
    for i in 0..lines.len() {
        // Check against non-adjacent segments
        for j in (i + 2)..lines.len() {
            // Skip adjacent segments (i and i+1 share an endpoint)
            // Also skip the pair of first and last segment if the linestring is closed
            // (they share the start/end point by definition)
            if i == 0 && j == lines.len() - 1 && is_linestring_closed(ls) {
                continue;
            }
            if let Some(intersection) = line_intersection(lines[i], lines[j]) {
                match intersection {
                    LineIntersection::SinglePoint { intersection, .. } => {
                        // Shared endpoint between non-adjacent segments is OK
                        // only if it's at the ends of both segments
                        let is_endpoint_i =
                            intersection == lines[i].start || intersection == lines[i].end;
                        let is_endpoint_j =
                            intersection == lines[j].start || intersection == lines[j].end;
                        if !(is_endpoint_i && is_endpoint_j) {
                            return false;
                        }
                    }
                    LineIntersection::Collinear { .. } => return false,
                }
            }
        }
    }
    true
}

// =========================================================================
// ST_IsRing
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsRing;

impl Default for IsRing {
    fn default() -> Self {
        Self
    }
}

static IS_RING_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsRing {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_isring"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(is_ring_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(IS_RING_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true if the geometry is a closed and simple linestring (a ring).",
                "ST_IsRing(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn is_ring_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = is_ring_array(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn is_ring_array(array: &dyn GeoArrowArray) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(array, _is_ring_impl)
}

fn _is_ring_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<BooleanArray> {
    use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            let ring = match &geo_geom {
                geo::Geometry::LineString(ls) => {
                    ls.0.len() >= 4 && is_linestring_closed(ls) && is_linestring_simple(ls)
                }
                _ => false,
            };
            builder.append_value(ring);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

// =========================================================================
// ST_IsCollection
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsCollection;

impl Default for IsCollection {
    fn default() -> Self {
        Self
    }
}

static IS_COLLECTION_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsCollection {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_iscollection"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(is_collection_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(IS_COLLECTION_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true if the geometry is a collection type.",
                "ST_IsCollection(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn is_collection_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = is_collection_array(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn is_collection_array(array: &dyn GeoArrowArray) -> GeoArrowResult<BooleanArray> {
    downcast_geoarrow_array!(array, _is_collection_impl)
}

fn _is_collection_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            let g = geom?;
            let is_coll = matches!(
                g.as_type(),
                GeometryType::MultiPoint(_)
                    | GeometryType::MultiLineString(_)
                    | GeometryType::MultiPolygon(_)
                    | GeometryType::GeometryCollection(_)
            );
            builder.append_value(is_coll);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

// =========================================================================
// ST_Dimension
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StDimension;

impl Default for StDimension {
    fn default() -> Self {
        Self
    }
}

static DIMENSION_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StDimension {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_dimension"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(dimension_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DIMENSION_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the topological dimension of the geometry.",
                "ST_Dimension(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn dimension_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = dimension_array(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn dimension_array(array: &dyn GeoArrowArray) -> GeoArrowResult<Int32Array> {
    downcast_geoarrow_array!(array, _dimension_impl)
}

fn _dimension_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<Int32Array> {
    let mut builder = Int32Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            let g = geom?;
            let dim = match g.as_type() {
                GeometryType::Point(_) | GeometryType::MultiPoint(_) => 0,
                GeometryType::LineString(_)
                | GeometryType::MultiLineString(_)
                | GeometryType::Line(_) => 1,
                GeometryType::Polygon(_)
                | GeometryType::MultiPolygon(_)
                | GeometryType::Triangle(_)
                | GeometryType::Rect(_) => 2,
                GeometryType::GeometryCollection(gc) => {
                    let mut max_dim = -1i32;
                    for g in gc.geometries() {
                        let d = match g.as_type() {
                            GeometryType::Point(_) | GeometryType::MultiPoint(_) => 0,
                            GeometryType::LineString(_)
                            | GeometryType::MultiLineString(_) => 1,
                            GeometryType::Polygon(_)
                            | GeometryType::MultiPolygon(_) => 2,
                            _ => 0,
                        };
                        if d > max_dim {
                            max_dim = d;
                        }
                    }
                    max_dim
                }
            };
            builder.append_value(dim);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

// =========================================================================
// ST_NumGeometries
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct NumGeometries;

impl Default for NumGeometries {
    fn default() -> Self {
        Self
    }
}

static NUM_GEOMETRIES_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for NumGeometries {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_numgeometries"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(num_geometries_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(NUM_GEOMETRIES_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the number of elements in a geometry collection.",
                "ST_NumGeometries(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn num_geometries_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = num_geometries_array(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn num_geometries_array(array: &dyn GeoArrowArray) -> GeoArrowResult<UInt32Array> {
    downcast_geoarrow_array!(array, _num_geometries_impl)
}

fn _num_geometries_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            let g = geom?;
            let count = match g.as_type() {
                GeometryType::Point(_)
                | GeometryType::LineString(_)
                | GeometryType::Polygon(_) => 1u32,
                GeometryType::MultiPoint(mp) => mp.num_points() as u32,
                GeometryType::MultiLineString(mls) => mls.num_line_strings() as u32,
                GeometryType::MultiPolygon(mp) => mp.num_polygons() as u32,
                GeometryType::GeometryCollection(gc) => gc.num_geometries() as u32,
                _ => 1,
            };
            builder.append_value(count);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

// =========================================================================
// ST_NRings
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct NRings;

impl Default for NRings {
    fn default() -> Self {
        Self
    }
}

static NRINGS_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for NRings {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_nrings"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(nrings_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(NRINGS_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the total number of rings in a geometry.",
                "ST_NRings(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn nrings_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = nrings_array(&geo_array)?;
    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn nrings_array(array: &dyn GeoArrowArray) -> GeoArrowResult<UInt32Array> {
    downcast_geoarrow_array!(array, _nrings_impl)
}

fn _nrings_impl<'a>(array: &'a impl GeoArrowArrayAccessor<'a>) -> GeoArrowResult<UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            let g = geom?;
            let count = count_rings(&g);
            builder.append_value(count);
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn count_rings(geom: &impl GeometryTrait) -> u32 {
    match geom.as_type() {
        GeometryType::Polygon(p) => {
            let exterior = if p.exterior().is_some() { 1u32 } else { 0 };
            exterior + p.num_interiors() as u32
        }
        GeometryType::MultiPolygon(mp) => {
            (0..mp.num_polygons())
                .filter_map(|i| mp.polygon(i))
                .map(|p| count_rings_polygon(&p))
                .sum()
        }
        GeometryType::GeometryCollection(gc) => gc.geometries().map(|g| count_rings(&g)).sum(),
        _ => 0,
    }
}

fn count_rings_polygon(p: &impl PolygonTrait) -> u32 {
    let exterior = if p.exterior().is_some() { 1u32 } else { 0 };
    exterior + p.num_interiors() as u32
}

// =========================================================================
// ST_HasZ
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct HasZ;

impl Default for HasZ {
    fn default() -> Self {
        Self
    }
}

static HASZ_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for HasZ {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_hasz"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])
            .map_err(|e| datafusion::error::DataFusionError::Internal(e.to_string()))?;
        let dim = geo_array.data_type().dimension();
        let has_z = matches!(dim, Some(Dimension::XYZ) | Some(Dimension::XYZM));
        let result = BooleanArray::from(vec![Some(has_z); arrays[0].len()]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(HASZ_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true if the geometry has a Z coordinate dimension.",
                "ST_HasZ(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

// =========================================================================
// ST_HasM
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct HasM;

impl Default for HasM {
    fn default() -> Self {
        Self
    }
}

static HASM_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for HasM {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_hasm"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])
            .map_err(|e| datafusion::error::DataFusionError::Internal(e.to_string()))?;
        let dim = geo_array.data_type().dimension();
        let has_m = matches!(dim, Some(Dimension::XYM) | Some(Dimension::XYZM));
        let result = BooleanArray::from(vec![Some(has_m); arrays[0].len()]);
        Ok(ColumnarValue::Array(Arc::new(result)))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(HASM_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true if the geometry has an M (measure) coordinate dimension.",
                "ST_HasM(geometry)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int32Type, UInt32Type};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_is_empty_point() {
        let ctx = SessionContext::new();
        ctx.register_udf(IsEmpty.into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql("SELECT ST_IsEmpty(ST_GeomFromText('POINT(0 0)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(!batch.column(0).as_boolean().value(0));
    }

    #[tokio::test]
    async fn test_is_closed_ring() {
        let ctx = SessionContext::new();
        ctx.register_udf(IsClosed.into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql("SELECT ST_IsClosed(ST_GeomFromText('LINESTRING(0 0, 1 1, 1 0, 0 0)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(batch.column(0).as_boolean().value(0));
    }

    #[tokio::test]
    async fn test_is_closed_open() {
        let ctx = SessionContext::new();
        ctx.register_udf(IsClosed.into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql("SELECT ST_IsClosed(ST_GeomFromText('LINESTRING(0 0, 1 1, 1 0)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(!batch.column(0).as_boolean().value(0));
    }

    #[tokio::test]
    async fn test_dimension() {
        let ctx = SessionContext::new();
        ctx.register_udf(StDimension.into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql("SELECT ST_Dimension(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<Int32Type>().value(0), 2);
    }

    #[tokio::test]
    async fn test_num_geometries_multi() {
        let ctx = SessionContext::new();
        ctx.register_udf(NumGeometries.into());
        ctx.register_udf(GeomFromText::default().into());

        let df = ctx
            .sql("SELECT ST_NumGeometries(ST_GeomFromText('MULTIPOINT(0 0, 1 1, 2 2)'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(batch.column(0).as_primitive::<UInt32Type>().value(0), 3);
    }
}
