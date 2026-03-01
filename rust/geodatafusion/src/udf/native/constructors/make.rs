//! Line, Polygon, and Envelope constructors

use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{CoordType, GeometryType, Metadata};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

// ---------------------------------------------------------------------------
// ST_MakeLine
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeLine {
    signature: Signature,
    coord_type: CoordType,
}

impl MakeLine {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for MakeLine {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_LINE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeLine {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_makeline"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata =
            Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
        let output_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(make_line_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_LINE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a LineString containing the points of two input geometries. Other geometries are decomposed into their component points.",
                "ST_MakeLine(geom1, geom2)",
            )
            .with_argument("geom1", "geometry")
            .with_argument("geom2", "geometry")
            .build()
        }))
    }
}

fn make_line_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let right_array = from_arrow_array(&arrays[1], &args.arg_fields[1])?;
    let result = make_line_arrays(&left_array, &right_array, coord_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn make_line_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(left, _make_line_left, right, coord_type)
}

fn _make_line_left<'a>(
    left: &'a impl GeoArrowArrayAccessor<'a>,
    right: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(right, _make_line_both, left, coord_type)
}

fn _make_line_both<'a, 'b>(
    right: &'b impl GeoArrowArrayAccessor<'b>,
    left: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(geom_type);

    for (l, r) in left.iter().zip(right.iter()) {
        match (l, r) {
            (Some(left_geom), Some(right_geom)) => {
                let left_geo = geometry_to_geo(&left_geom?)?;
                let right_geo = geometry_to_geo(&right_geom?)?;

                let mut coords = collect_points(&left_geo);
                coords.extend(collect_points(&right_geo));

                if coords.len() >= 2 {
                    let line = geo::LineString::new(coords);
                    builder.push_geometry(Some(&geo::Geometry::LineString(line)))?;
                } else {
                    builder.push_null();
                }
            }
            _ => builder.push_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Extract all coordinate points from a geometry.
fn collect_points(geom: &geo::Geometry) -> Vec<geo::Coord> {
    use geo::CoordsIter;
    geom.coords_iter().collect()
}

// ---------------------------------------------------------------------------
// ST_MakePolygon
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakePolygon {
    coord_type: CoordType,
}

impl MakePolygon {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for MakePolygon {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_POLYGON_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakePolygon {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_makepolygon"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata =
            Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
        let output_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(make_polygon_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_POLYGON_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a Polygon from a closed LineString shell.",
                "ST_MakePolygon(linestring)",
            )
            .with_argument("shell", "A closed LineString geometry")
            .build()
        }))
    }
}

fn make_polygon_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let result = make_polygon_array(&geo_array, coord_type)?;
    Ok(ColumnarValue::Array(result.into_array_ref()))
}

fn make_polygon_array(
    array: &dyn GeoArrowArray,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    downcast_geoarrow_array!(array, _make_polygon_impl, coord_type)
}

fn _make_polygon_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    let geom_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(geom_type);

    for item in array.iter() {
        if let Some(geom) = item {
            let geo_geom = geometry_to_geo(&geom?)?;
            match geo_geom {
                geo::Geometry::LineString(mut ls) => {
                    // Auto-close if not already closed (matches PostGIS behavior)
                    if ls.0.len() >= 2 && ls.0.first() != ls.0.last() {
                        ls.0.push(ls.0[0]);
                    }
                    let polygon = geo::Polygon::new(ls, vec![]);
                    builder.push_geometry(Some(&geo::Geometry::Polygon(polygon)))?;
                }
                _ => {
                    // For non-linestring inputs, push null
                    builder.push_null();
                }
            }
        } else {
            builder.push_null();
        }
    }

    Ok(Arc::new(builder.finish()))
}

// ---------------------------------------------------------------------------
// ST_MakeEnvelope
// ---------------------------------------------------------------------------

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeEnvelope {
    signature: Signature,
    coord_type: CoordType,
}

impl MakeEnvelope {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for MakeEnvelope {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_ENVELOPE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeEnvelope {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_makeenvelope"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let mut geom_type =
            GeometryType::new(Default::default()).with_coord_type(self.coord_type);

        // If a 5th argument (SRID) is provided as scalar, use it
        if let Some(srid) = args.scalar_arguments.get(4) {
            if let Some(datafusion::scalar::ScalarValue::Int64(srid_val)) = srid {
                let crs = geoarrow_schema::Crs::from_authority_code(format!(
                    "EPSG:{}",
                    match srid_val {
                        Some(v) => v,
                        None => return Ok(Arc::new(geom_type.to_field("", true))),
                    }
                ));
                geom_type = geom_type
                    .with_metadata(Arc::new(Metadata::new(crs, Default::default())));
            }
        }

        Ok(Arc::new(geom_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(make_envelope_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_ENVELOPE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a rectangular Polygon from minimum and maximum coordinate values. The polygon is formed in the coordinate plane with corners at (xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin), (xmin, ymin).",
                "ST_MakeEnvelope(xmin, ymin, xmax, ymax) or ST_MakeEnvelope(xmin, ymin, xmax, ymax, srid)",
            )
            .with_argument("xmin", "minimum x value")
            .with_argument("ymin", "minimum y value")
            .with_argument("xmax", "maximum x value")
            .with_argument("ymax", "maximum y value")
            .with_argument("srid", "optional integer SRID value")
            .build()
        }))
    }
}

fn make_envelope_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args[..4])?;
    let xmin = arrays[0].as_primitive::<Float64Type>();
    let ymin = arrays[1].as_primitive::<Float64Type>();
    let xmax = arrays[2].as_primitive::<Float64Type>();
    let ymax = arrays[3].as_primitive::<Float64Type>();

    let geom_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(geom_type);

    for i in 0..xmin.len() {
        if xmin.is_null(i) || ymin.is_null(i) || xmax.is_null(i) || ymax.is_null(i) {
            builder.push_null();
        } else {
            let x1 = xmin.value(i);
            let y1 = ymin.value(i);
            let x2 = xmax.value(i);
            let y2 = ymax.value(i);

            let polygon = geo::Polygon::new(
                geo::LineString::from(vec![
                    (x1, y1),
                    (x1, y2),
                    (x2, y2),
                    (x2, y1),
                    (x1, y1),
                ]),
                vec![],
            );
            builder.push_geometry(Some(&geo::Geometry::Polygon(polygon)))?;
        }
    }

    Ok(ColumnarValue::Array(builder.finish().to_array_ref()))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Float64Array, RecordBatch};
    use arrow_schema::{Field, Schema};
    use datafusion::prelude::SessionContext;
    use geoarrow_schema::Crs;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    #[tokio::test]
    async fn test_make_line() {
        let ctx = SessionContext::new();

        ctx.register_udf(MakeLine::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let sql_df = ctx
            .sql(r#"SELECT ST_AsText(ST_MakeLine(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('POINT(1 1)')));"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];

        let str_arr = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let wkt = str_arr.value(0);
        assert!(
            wkt.contains("LINESTRING"),
            "Expected LINESTRING in output, got: {}",
            wkt
        );
    }

    #[tokio::test]
    async fn test_make_polygon() {
        let ctx = SessionContext::new();

        ctx.register_udf(MakePolygon::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());

        let sql_df = ctx
            .sql(r#"SELECT ST_AsText(ST_MakePolygon(ST_GeomFromText('LINESTRING(0 0, 10 0, 10 10, 0 10, 0 0)')));"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];

        let str_arr = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let wkt = str_arr.value(0);
        assert!(
            wkt.contains("POLYGON"),
            "Expected POLYGON in output, got: {}",
            wkt
        );
    }

    #[tokio::test]
    async fn test_make_envelope() {
        let ctx = SessionContext::new();

        ctx.register_udf(MakeEnvelope::default().into());
        ctx.register_udf(AsText.into());

        let sql_df = ctx
            .sql(r#"SELECT ST_AsText(ST_MakeEnvelope(0.0, 0.0, 1.0, 1.0));"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];

        let str_arr = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let wkt = str_arr.value(0);
        assert!(
            wkt.contains("POLYGON"),
            "Expected POLYGON in output, got: {}",
            wkt
        );
    }

    #[tokio::test]
    async fn test_make_envelope_with_srid() {
        let ctx = SessionContext::new();

        ctx.register_udf(MakeEnvelope::default().into());

        let sql_df = ctx
            .sql(r#"SELECT ST_MakeEnvelope(0.0, 0.0, 1.0, 1.0, 4326);"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);

        let geom_type = output_field.try_extension_type::<GeometryType>().unwrap();
        assert_eq!(
            geom_type.metadata().crs(),
            &Crs::from_authority_code("EPSG:4326".to_string())
        );
    }

    #[tokio::test]
    async fn test_make_envelope_from_table() {
        let ctx = SessionContext::new();

        ctx.register_udf(MakeEnvelope::default().into());
        ctx.register_udf(AsText.into());

        let xmin: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let ymin: ArrayRef = Arc::new(Float64Array::from(vec![0.0]));
        let xmax: ArrayRef = Arc::new(Float64Array::from(vec![10.0]));
        let ymax: ArrayRef = Arc::new(Float64Array::from(vec![10.0]));

        let schema = Schema::new([
            Arc::new(Field::new("xmin", DataType::Float64, true)),
            Arc::new(Field::new("ymin", DataType::Float64, true)),
            Arc::new(Field::new("xmax", DataType::Float64, true)),
            Arc::new(Field::new("ymax", DataType::Float64, true)),
        ]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![xmin, ymin, xmax, ymax]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let sql_df = ctx
            .sql("SELECT ST_AsText(ST_MakeEnvelope(xmin, ymin, xmax, ymax)) FROM t;")
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];

        let str_arr = output_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let wkt = str_arr.value(0);
        assert!(
            wkt.contains("POLYGON"),
            "Expected POLYGON in output, got: {}",
            wkt
        );
    }
}
