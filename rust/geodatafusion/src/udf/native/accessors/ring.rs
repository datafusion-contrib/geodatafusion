use std::sync::{Arc, OnceLock};

use arrow_array::builder::UInt32Builder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, UInt32Array};
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, MultiPolygonTrait, PolygonTrait, TriangleTrait,
};
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::common::{
    coord_to_wkt, geometry_and_index_signature, output_geometry_type, positive_index, wkt_dimension,
};
use crate::udf::native::geometry::geometry_array_from_arrow;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ExteriorRing {
    signature: Signature,
    coord_type: CoordType,
}

impl ExteriorRing {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input().clone(),
            coord_type,
        }
    }
}

impl Default for ExteriorRing {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static EXTERIOR_RING_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ExteriorRing {
    fn name(&self) -> &str {
        "st_exteriorring"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from geometry metadata in return_field_from_args."
                .to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<arrow_schema::Field>> {
        let geometry_type = output_geometry_type(&args.arg_fields[0], self.coord_type)?;
        Ok(geometry_type.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(exterior_ring_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(EXTERIOR_RING_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the exterior ring of a POLYGON as a LINESTRING. Return an empty \
                 LINESTRING for an empty polygon and NULL for non-polygon inputs.",
                "ST_ExteriorRing(polygon)",
            )
            .with_argument("polygon", "input polygon")
            .build()
        }))
    }
}

fn exterior_ring_impl(
    args: ScalarFunctionArgs,
    fallback_coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geometry_type = output_geometry_type(&args.arg_fields[0], fallback_coord_type)?;

    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, exterior_ring_array, geometry_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn exterior_ring_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    geometry_type: GeometryType,
) -> GeoDataFusionResult<geoarrow_array::array::GeometryArray> {
    use geo_traits::GeometryType::*;
    let mut builder = GeometryBuilder::new(geometry_type);

    for item in array.iter() {
        let Some(geom) = item else {
            builder.push_null();
            continue;
        };
        let geom = geom?;

        match geom.as_type() {
            Polygon(polygon) => match polygon.exterior() {
                Some(ring) => builder.push_geometry(Some(&ring))?,
                None => {
                    let empty = wkt::types::LineString::<f64>::empty(wkt_dimension(geom.dim()));
                    builder.push_geometry(Some(&empty))?;
                }
            },
            Triangle(triangle) => {
                let ring = triangle_ring(triangle);
                builder.push_geometry(Some(&ring))?;
            }
            Point(_)
            | LineString(_)
            | MultiPoint(_)
            | MultiLineString(_)
            | MultiPolygon(_)
            | GeometryCollection(_)
            | Rect(_)
            | Line(_) => builder.push_null(),
        }
    }

    Ok(builder.finish())
}

fn triangle_ring(triangle: &impl TriangleTrait<T = f64>) -> wkt::types::LineString {
    let first = coord_to_wkt(&triangle.first());
    let second = coord_to_wkt(&triangle.second());
    let third = coord_to_wkt(&triangle.third());
    wkt::types::LineString::new(
        vec![first, second, third, first],
        wkt_dimension(triangle.dim()),
    )
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct InteriorRingN {
    signature: Signature,
    coord_type: CoordType,
}

impl InteriorRingN {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: geometry_and_index_signature().clone(),
            coord_type,
        }
    }
}

impl Default for InteriorRingN {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static INTERIOR_RING_N_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for InteriorRingN {
    fn name(&self) -> &str {
        "st_interiorringn"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from geometry metadata in return_field_from_args."
                .to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<arrow_schema::Field>> {
        let geometry_type = output_geometry_type(&args.arg_fields[0], self.coord_type)?;
        Ok(geometry_type.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(interior_ring_n_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(INTERIOR_RING_N_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the 1-based Nth interior ring (hole) of a POLYGON as a LINESTRING. \
                 Return NULL if the geometry is not a polygon or the index is out of range.",
                "ST_InteriorRingN(polygon, n)",
            )
            .with_argument("polygon", "input polygon")
            .with_argument("n", "1-based ring index")
            .build()
        }))
    }
}

fn interior_ring_n_impl(
    args: ScalarFunctionArgs,
    fallback_coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let indices = arrays[1].as_primitive::<Int64Type>();
    let geometry_type = output_geometry_type(&args.arg_fields[0], fallback_coord_type)?;

    let geo_array_ref = geo_array.as_ref();
    let result =
        downcast_geoarrow_array!(geo_array_ref, interior_ring_n_array, indices, geometry_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn interior_ring_n_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    indices: &Int64Array,
    geometry_type: GeometryType,
) -> GeoDataFusionResult<geoarrow_array::array::GeometryArray> {
    use geo_traits::GeometryType::*;
    let mut builder = GeometryBuilder::new(geometry_type);

    for (item, index) in array.iter().zip(indices.iter()) {
        let Some(geom) = item else {
            builder.push_null();
            continue;
        };
        let geom = geom?;
        let Some(index) = index else {
            builder.push_null();
            continue;
        };
        let Some(zero_based) = positive_index(index, usize::MAX) else {
            builder.push_null();
            continue;
        };

        match geom.as_type() {
            Polygon(polygon) => {
                let Some(ring) = polygon.interior(zero_based) else {
                    builder.push_null();
                    continue;
                };
                builder.push_geometry(Some(&ring))?;
            }
            Point(_)
            | LineString(_)
            | MultiPoint(_)
            | MultiLineString(_)
            | MultiPolygon(_)
            | GeometryCollection(_)
            | Rect(_)
            | Triangle(_)
            | Line(_) => builder.push_null(),
        }
    }

    Ok(builder.finish())
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct NRings;

impl NRings {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NRings {
    fn default() -> Self {
        Self::new()
    }
}

static N_RINGS_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for NRings {
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
        Ok(n_rings_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(N_RINGS_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the number of rings in a polygonal geometry, counting exterior and \
                 interior rings. Non-polygonal geometries and empty geometries return 0.",
                "ST_NRings(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

fn n_rings_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, n_rings_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn n_rings_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            builder.append_value(n_rings(&geom?));
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn n_rings(geom: &impl GeometryTrait<T = f64>) -> u32 {
    use geo_traits::GeometryType::*;

    match geom.as_type() {
        Polygon(polygon) => {
            u32::from(polygon.exterior().is_some()) + polygon.num_interiors() as u32
        }
        MultiPolygon(g) => g.polygons().map(|polygon| n_rings(&polygon)).sum(),
        GeometryCollection(g) => g.geometries().map(|child| n_rings(&child)).sum(),
        Triangle(_) | Rect(_) => 1,
        Point(_) | LineString(_) | MultiPoint(_) | MultiLineString(_) | Line(_) => 0,
    }
}
