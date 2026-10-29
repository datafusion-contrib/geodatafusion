use std::sync::{Arc, OnceLock};

use arrow_array::builder::{BooleanBuilder, UInt8Builder, UInt32Builder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{BooleanArray, Int64Array, UInt8Array, UInt32Array};
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait,
};
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::common::{
    geometry_and_index_signature, output_geometry_type, positive_index,
};
use crate::udf::native::accessors::is_empty::is_geometry_topologically_empty;
use crate::udf::native::geometry::geometry_array_from_arrow;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Dimension;

impl Dimension {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Dimension {
    fn default() -> Self {
        Self::new()
    }
}

static DIMENSION_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Dimension {
    fn name(&self) -> &str {
        "st_dimension"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(dimension_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DIMENSION_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the topological dimension of the geometry: 0 for points, 1 for lines, \
                 2 for polygons, and the largest component dimension for a GEOMETRYCOLLECTION. \
                 Typed empty geometries retain their type dimension; an empty \
                 GEOMETRYCOLLECTION returns 0.",
                "ST_Dimension(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

fn dimension_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, dimension_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn dimension_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<UInt8Array> {
    let mut builder = UInt8Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            builder.append_value(topological_dimension(&geom?));
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn topological_dimension(geom: &impl GeometryTrait<T = f64>) -> u8 {
    use geo_traits::GeometryType::*;

    match geom.as_type() {
        Point(_) | MultiPoint(_) => 0,
        LineString(_) | MultiLineString(_) | Line(_) => 1,
        Polygon(_) | MultiPolygon(_) | Rect(_) | Triangle(_) => 2,
        GeometryCollection(collection) => collection
            .geometries()
            .map(|child| topological_dimension(&child))
            .max()
            .unwrap_or(0),
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsCollection;

impl IsCollection {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for IsCollection {
    fn default() -> Self {
        Self::new()
    }
}

static IS_COLLECTION_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsCollection {
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
        Some(IS_COLLECTION_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return TRUE if the geometry type is a collection type (GEOMETRYCOLLECTION or \
                 MULTI*), including empty collections and collections with one element.",
                "ST_IsCollection(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

fn is_collection_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, is_collection_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn is_collection_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            builder.append_value(is_collection(&geom?));
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn is_collection(geom: &impl GeometryTrait<T = f64>) -> bool {
    use geo_traits::GeometryType::*;

    matches!(
        geom.as_type(),
        MultiPoint(_) | MultiLineString(_) | MultiPolygon(_) | GeometryCollection(_)
    )
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct NumGeometries;

impl NumGeometries {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NumGeometries {
    fn default() -> Self {
        Self::new()
    }
}

static NUM_GEOMETRIES_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for NumGeometries {
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
        Some(NUM_GEOMETRIES_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the number of immediate elements in a GEOMETRYCOLLECTION or MULTI* \
                 geometry. Return 1 for non-empty atomic geometries and 0 for empty geometries.",
                "ST_NumGeometries(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

fn num_geometries_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, num_geometries_array)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn num_geometries_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<UInt32Array> {
    let mut builder = UInt32Builder::with_capacity(array.len());
    for item in array.iter() {
        if let Some(geom) = item {
            builder.append_value(num_geometries(&geom?));
        } else {
            builder.append_null();
        }
    }
    Ok(builder.finish())
}

fn num_geometries(geom: &impl GeometryTrait<T = f64>) -> u32 {
    use geo_traits::GeometryType::*;

    if is_geometry_topologically_empty(geom) {
        return 0;
    }

    match geom.as_type() {
        MultiPoint(g) => g.num_points() as u32,
        MultiLineString(g) => g.num_line_strings() as u32,
        MultiPolygon(g) => g.num_polygons() as u32,
        GeometryCollection(g) => g.num_geometries() as u32,
        Point(_) | LineString(_) | Polygon(_) | Rect(_) | Triangle(_) | Line(_) => 1,
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct GeometryN {
    signature: Signature,
    coord_type: CoordType,
}

impl GeometryN {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: geometry_and_index_signature().clone(),
            coord_type,
        }
    }
}

impl Default for GeometryN {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static GEOMETRY_N_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for GeometryN {
    fn name(&self) -> &str {
        "st_geometryn"
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
        Ok(geometry_n_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(GEOMETRY_N_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Return the 1-based Nth element of a GEOMETRYCOLLECTION, MULTIPOINT, \
                 MULTILINESTRING, or MULTIPOLYGON. A non-empty atomic geometry returns itself \
                 for index 1. Empty geometries and out-of-range indexes return NULL.",
                "ST_GeometryN(geometry, n)",
            )
            .with_argument("geometry", "input geometry")
            .with_argument("n", "1-based element index")
            .build()
        }))
    }
}

fn geometry_n_impl(
    args: ScalarFunctionArgs,
    fallback_coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let indices = arrays[1].as_primitive::<Int64Type>();
    let geometry_type = output_geometry_type(&args.arg_fields[0], fallback_coord_type)?;

    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, geometry_n_array, indices, geometry_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn geometry_n_array<'a>(
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

        if is_geometry_topologically_empty(&geom) {
            builder.push_null();
            continue;
        }
        let Some(index) = index else {
            builder.push_null();
            continue;
        };

        match geom.as_type() {
            Point(_) | LineString(_) | Polygon(_) | Rect(_) | Triangle(_) | Line(_) => {
                if positive_index(index, 1).is_some() {
                    builder.push_geometry(Some(&geom))?;
                } else {
                    builder.push_null();
                }
            }
            MultiPoint(g) => {
                let Some(index) = positive_index(index, g.num_points()) else {
                    builder.push_null();
                    continue;
                };
                push_option_geometry(&mut builder, g.point(index))?;
            }
            MultiLineString(g) => {
                let Some(index) = positive_index(index, g.num_line_strings()) else {
                    builder.push_null();
                    continue;
                };
                push_option_geometry(&mut builder, g.line_string(index))?;
            }
            MultiPolygon(g) => {
                let Some(index) = positive_index(index, g.num_polygons()) else {
                    builder.push_null();
                    continue;
                };
                push_option_geometry(&mut builder, g.polygon(index))?;
            }
            GeometryCollection(g) => {
                let Some(index) = positive_index(index, g.num_geometries()) else {
                    builder.push_null();
                    continue;
                };
                push_option_geometry(&mut builder, g.geometry(index))?;
            }
        }
    }

    Ok(builder.finish())
}

fn push_option_geometry(
    builder: &mut GeometryBuilder,
    geometry: Option<impl GeometryTrait<T = f64>>,
) -> GeoDataFusionResult<()> {
    match geometry {
        Some(geometry) => builder.push_geometry(Some(&geometry))?,
        None => builder.push_null(),
    }
    Ok(())
}
