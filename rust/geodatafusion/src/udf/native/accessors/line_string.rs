//! Accessors from LineString geometries

use std::sync::{Arc, OnceLock};

use arrow_array::Int64Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::array::{GeometryArray, LineStringArray, PointArray};
use geoarrow_array::builder::{GeometryBuilder, PointBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, PointType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::common::{
    geometry_and_index_signature, output_geometry_type, signed_index,
};
use crate::udf::native::geometry::geometry_array_from_arrow;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct StartPoint {
    coord_type: CoordType,
}

impl StartPoint {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for StartPoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static START_POINT_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for StartPoint {
    fn name(&self) -> &str {
        "st_startpoint"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(point_impl(args, self.coord_type, Mode::Start)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(START_POINT_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Returns the first point of a geometry in element order as a POINT. Returns NULL for EMPTY geometries and NULL input.", "ST_StartPoint(geometry)" )
                .with_argument("g1", "geometry")
                .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct EndPoint {
    coord_type: CoordType,
}

impl EndPoint {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for EndPoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static END_POINT_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for EndPoint {
    fn name(&self) -> &str {
        "st_endpoint"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(point_impl(args, self.coord_type, Mode::End)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(END_POINT_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Returns the last point of a LINESTRING as a POINT. Returns NULL for other geometry types, EMPTY geometries, and NULL input.", "ST_EndPoint(geometry)" )
                .with_argument("g1", "geometry")
                .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PointN {
    signature: Signature,
    coord_type: CoordType,
}

impl PointN {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: geometry_and_index_signature().clone(),
            coord_type,
        }
    }
}

impl Default for PointN {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_N_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointN {
    fn name(&self) -> &str {
        "st_pointn"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let geometry_type = output_geometry_type(&args.arg_fields[0], self.coord_type)?;
        Ok(geometry_type.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(point_n_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_N_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(DOC_SECTION_OTHER, "Return the Nth point in a single linestring or circular linestring in the geometry. Negative values are counted backwards from the end of the LineString, so that -1 is the last point. Returns NULL if there is no linestring in the geometry.", "ST_PointN(line_string, 1)" )
                .with_argument("g1", "geometry")
                .with_argument("n", "1-based point index; negative values count backwards")
                .build()
        }))
    }
}

fn point_n_impl(
    args: ScalarFunctionArgs,
    fallback_coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let indices = arrays[1].as_primitive::<Int64Type>();
    let geometry_type = output_geometry_type(&args.arg_fields[0], fallback_coord_type)?;

    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, point_n_array, indices, geometry_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn point_n_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    indices: &Int64Array,
    geometry_type: GeometryType,
) -> GeoDataFusionResult<GeometryArray> {
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

        match geom.as_type() {
            LineString(line_string) => {
                let Some(index) = signed_index(index, line_string.num_coords()) else {
                    builder.push_null();
                    continue;
                };
                let Some(coord) = line_string.coord(index) else {
                    builder.push_null();
                    continue;
                };
                let point = coord_to_point(coord);
                builder.push_geometry(Some(&point))?;
            }
            Point(_)
            | Polygon(_)
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

/// For LineString input, return fixed-dim Point output
/// For other geometry input, return variable-dim Geometry output
fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let input_field = GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref())?;
    match input_field {
        GeoArrowType::LineString(typ) => {
            let output_type =
                PointType::new(typ.dimension(), typ.metadata().clone()).with_coord_type(coord_type);
            Ok(Arc::new(output_type.to_field("", true)))
        }
        _ => {
            let output_type =
                GeometryType::new(input_field.metadata().clone()).with_coord_type(coord_type);
            Ok(Arc::new(output_type.to_field("", true)))
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Mode {
    Start,
    End,
}

fn point_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
    mode: Mode,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();

    let out: Arc<dyn GeoArrowArray> = match geo_array.data_type() {
        GeoArrowType::LineString(_) => Arc::new(impl_fixed_dim(
            geo_array.as_line_string(),
            coord_type,
            mode,
        )?),
        _ => Arc::new(downcast_geoarrow_array!(
            geo_array_ref,
            impl_variable_dim,
            coord_type,
            mode
        )?),
    };

    Ok(out.into_array_ref().into())
}

fn impl_fixed_dim(
    array: &LineStringArray,
    coord_type: CoordType,
    mode: Mode,
) -> GeoDataFusionResult<PointArray> {
    let typ = PointType::new(
        array.extension_type().dimension(),
        array.extension_type().metadata().clone(),
    )
    .with_coord_type(coord_type);
    let mut output_builder = PointBuilder::with_capacity(typ, array.len());

    for geom in array.iter() {
        if let Some(ls) = geom {
            output_builder.push_coord(get_linestring_coord(&ls?, mode).as_ref());
        } else {
            output_builder.push_null();
        }
    }

    Ok(output_builder.finish())
}

fn impl_variable_dim<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
    mode: Mode,
) -> GeoDataFusionResult<GeometryArray> {
    let typ = GeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut output_builder = GeometryBuilder::new(typ);

    for geom in array.iter() {
        if let Some(geom) = geom {
            if let Some(point) = geometry_point(&geom?, mode) {
                output_builder.push_geometry(Some(&point))?;
            } else {
                output_builder.push_null();
            }
        } else {
            output_builder.push_null();
        }
    }

    Ok(output_builder.finish())
}

fn geometry_point(geom: &impl GeometryTrait<T = f64>, mode: Mode) -> Option<wkt::types::Point> {
    match mode {
        Mode::Start => first_point(geom),
        Mode::End => last_point(geom),
    }
}

/// Returns the first point in element order without skipping empty parts,
/// matching the PostGIS ST_StartPoint behavior.
fn first_point(geom: &impl GeometryTrait<T = f64>) -> Option<wkt::types::Point> {
    use geo_traits::GeometryType::*;

    match geom.as_type() {
        Point(point) => point.coord().map(coord_to_point),
        LineString(line_string) => line_string.coord(0).map(coord_to_point),
        Polygon(polygon) => polygon
            .exterior()
            .and_then(|ring| ring.coord(0).map(coord_to_point)),
        MultiPoint(multi_point) => multi_point
            .point(0)
            .and_then(|point| point.coord().map(coord_to_point)),
        MultiLineString(multi_line_string) => multi_line_string
            .line_string(0)
            .and_then(|line_string| line_string.coord(0).map(coord_to_point)),
        MultiPolygon(multi_polygon) => multi_polygon.polygon(0).and_then(|polygon| {
            polygon
                .exterior()
                .and_then(|ring| ring.coord(0).map(coord_to_point))
        }),
        GeometryCollection(collection) => {
            collection.geometry(0).and_then(|child| first_point(&child))
        }
        Rect(rect) => Some(coord_to_point(rect.min())),
        Triangle(triangle) => Some(coord_to_point(triangle.first())),
        Line(line) => Some(coord_to_point(line.start())),
    }
}

/// PostGIS ST_EndPoint only supports LineString inputs.
fn last_point(geom: &impl GeometryTrait<T = f64>) -> Option<wkt::types::Point> {
    let geo_traits::GeometryType::LineString(line_string) = geom.as_type() else {
        return None;
    };
    let count = line_string.num_coords();
    if count == 0 {
        return None;
    }
    line_string.coord(count - 1).map(coord_to_point)
}

fn get_linestring_coord(
    geom: &impl LineStringTrait<T = f64>,
    mode: Mode,
) -> Option<impl CoordTrait<T = f64>> {
    match mode {
        Mode::Start => geom.coord(0),
        Mode::End => geom.coord(geom.num_coords() - 1),
    }
}

/// Convert an arbitrary coord to a Point
fn coord_to_point(coord: impl CoordTrait<T = f64>) -> wkt::types::Point {
    // `nth(2)` is the third ordinate, which is Z for XYZ and M for XYM, so the
    // dimension decides which slot of the output coordinate carries it.
    let (z, m) = match coord.dim() {
        geo_traits::Dimensions::Xyz | geo_traits::Dimensions::Unknown(3) => (coord.nth(2), None),
        geo_traits::Dimensions::Xym => (None, coord.nth(2)),
        geo_traits::Dimensions::Xyzm => (coord.nth(2), coord.nth(3)),
        _ => (None, None),
    };
    wkt::types::Point::from_coord(wkt::types::Coord {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test_point_n() {
        let ctx = SessionContext::new();
        ctx.register_udf(AsText::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(PointN::default().into());

        let cases: Vec<(&str, i64, Option<&str>)> = vec![
            ("LINESTRING(0 0, 1 1, 2 2)", 1, Some("POINT(0 0)")),
            ("LINESTRING(0 0, 1 1, 2 2)", 2, Some("POINT(1 1)")),
            ("LINESTRING(0 0, 1 1, 2 2)", -1, Some("POINT(2 2)")),
            (
                "LINESTRING(0 0, 1 1, 2 2, 3 3, 4 4)",
                -2,
                Some("POINT(3 3)"),
            ),
            (
                "LINESTRING Z (0 0 7, 1 1 8, 2 2 9)",
                2,
                Some("POINT Z(1 1 8)"),
            ),
            ("LINESTRING(0 0, 1 1, 2 2)", 0, None),
            ("LINESTRING(0 0, 1 1, 2 2)", 4, None),
            ("LINESTRING(0 0, 1 1, 2 2)", -4, None),
            ("LINESTRING EMPTY", 1, None),
            ("POLYGON((0 0, 1 0, 1 1, 0 0))", 1, None),
            ("MULTILINESTRING((0 0, 1 1))", 1, None),
            ("GEOMETRYCOLLECTION(POINT(0 0))", 1, None),
        ];

        for (wkt, index, expected) in cases {
            let sql = format!("SELECT ST_AsText(ST_PointN(ST_GeomFromText('{wkt}'), {index}))");
            let df = ctx.sql(&sql).await.unwrap();
            let batch = df.collect().await.unwrap().into_iter().next().unwrap();
            let col = batch.column(0).as_string::<i32>();
            match expected {
                Some(expected) => assert_eq!(col.value(0), expected, "failed for {wkt}, {index}"),
                None => assert!(col.is_null(0), "expected NULL for {wkt}, {index}"),
            }
        }
    }

    #[tokio::test]
    async fn test_point_n_null_and_column_index() {
        let ctx = SessionContext::new();
        ctx.register_udf(AsText::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(PointN::default().into());

        let df = ctx
            .sql("SELECT ST_AsText(ST_PointN(ST_GeomFromText(NULL), 1))")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(batch.column(0).as_string::<i32>().is_null(0));

        let df = ctx
            .sql(
                "SELECT n, ST_AsText(ST_PointN(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'), n)) \
                 FROM (VALUES (1), (2), (3), (-1)) AS t(n)",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let points = batch.column(1).as_string::<i32>();
        assert_eq!(points.value(0), "POINT(0 0)");
        assert_eq!(points.value(1), "POINT(1 1)");
        assert_eq!(points.value(2), "POINT(2 2)");
        assert_eq!(points.value(3), "POINT(2 2)");
    }
}
