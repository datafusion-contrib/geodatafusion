use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::{Array, BinaryViewArray, GenericBinaryArray, OffsetSizeTrait};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geo_traits::GeometryTrait;
use geoarrow_array::array::{GeometryArray, from_arrow_array};
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::error::GeoArrowError;
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata};
use geos::{Geom, Geometry};
use wkb::Endianness;
use wkb::reader::Wkb;
use wkb::writer::{WriteOptions, write_geometry};

use crate::data_types::any_geometry_type;
use crate::error::{GeoDataFusionError, GeoDataFusionResult};

/// A single geometry argument, optionally followed by the `directed` boolean.
static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    let geometry_types = any_geometry_type();
    let mut variants = Vec::with_capacity(geometry_types.len() * 2);
    for geometry_type in geometry_types {
        variants.push(TypeSignature::Exact(vec![geometry_type.clone()]));
        variants.push(TypeSignature::Exact(vec![geometry_type, DataType::Boolean]));
    }
    Signature::one_of(variants, Volatility::Immutable)
});

/// Sews together the component lines of a (multi)linestring.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineMerge {
    coord_type: CoordType,
}

impl LineMerge {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for LineMerge {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineMerge {
    fn name(&self) -> &str {
        "st_linemerge"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_merge_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a (set of) LineString(s) formed by sewing together the constituent line work of a MultiLineString. Lines are joined at endpoints where exactly two lines meet; lines are not merged across intersections of three or more lines. When `directed` is true, lines are only merged when their directions agree. Non-linear inputs yield an empty GeometryCollection. This function strips the M dimension.",
                "ST_LineMerge(geometry, directed)",
            )
            .with_argument("geom", "geometry")
            .with_argument("directed", "boolean")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

/// Parse the optional `directed` argument.
///
/// Absent or null is treated as `false`.
fn parse_directed(args: &ScalarFunctionArgs) -> GeoDataFusionResult<bool> {
    match args.args.get(1) {
        None => Ok(false),
        Some(arg) => match arg.cast_to(&DataType::Boolean, None)? {
            ColumnarValue::Scalar(ScalarValue::Boolean(directed)) => Ok(directed.unwrap_or(false)),
            // A cast to `Boolean` only ever yields a `Boolean` scalar.
            ColumnarValue::Scalar(_) => unreachable!("cast to Boolean yields a Boolean scalar"),
            ColumnarValue::Array(_) => Err(DataFusionError::NotImplemented(
                "Vectorized `directed` argument to ST_LineMerge is not yet implemented".to_string(),
            )
            .into()),
        },
    }
}

fn line_merge_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let directed = parse_directed(&args)?;

    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let to_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    let GeoArrowType::Geometry(output_type) = to_type else {
        return Err(GeoArrowError::IncorrectGeometryType(format!(
            "Expected Geometry output, got {to_type:?}"
        ))
        .into());
    };

    let mut processor = LineMergeProcessor::new(output_type, directed);
    process_input(geo_array.as_ref(), &mut processor)?;
    Ok(ColumnarValue::Array(processor.finish().to_array_ref()))
}

fn process_input(
    geo_array: &dyn GeoArrowArray,
    processor: &mut LineMergeProcessor,
) -> GeoDataFusionResult<()> {
    use GeoArrowType::*;

    match geo_array.data_type() {
        Point(_) => for_each_geometry(geo_array.as_point(), |geometry| {
            processor.push_geometry(geometry)
        }),
        LineString(_) => for_each_geometry(geo_array.as_line_string(), |geometry| {
            processor.push_geometry(geometry)
        }),
        Polygon(_) => for_each_geometry(geo_array.as_polygon(), |geometry| {
            processor.push_geometry(geometry)
        }),
        MultiPoint(_) => for_each_geometry(geo_array.as_multi_point(), |geometry| {
            processor.push_geometry(geometry)
        }),
        MultiLineString(_) => for_each_geometry(geo_array.as_multi_line_string(), |geometry| {
            processor.push_geometry(geometry)
        }),
        MultiPolygon(_) => for_each_geometry(geo_array.as_multi_polygon(), |geometry| {
            processor.push_geometry(geometry)
        }),
        GeometryCollection(_) => {
            for_each_geometry(geo_array.as_geometry_collection(), |geometry| {
                processor.push_geometry(geometry)
            })
        }
        Rect(_) => for_each_geometry(geo_array.as_rect(), |geometry| {
            processor.push_geometry(geometry)
        }),
        Geometry(_) => for_each_geometry(geo_array.as_geometry(), |geometry| {
            processor.push_geometry(geometry)
        }),
        // WKB inputs go directly to GEOS without first materializing a batch-wide copy.
        Wkb(_) => for_each_binary(geo_array.as_wkb::<i32>().inner(), |wkb| {
            processor.push_wkb(wkb)
        }),
        LargeWkb(_) => for_each_binary(geo_array.as_wkb::<i64>().inner(), |wkb| {
            processor.push_wkb(wkb)
        }),
        WkbView(_) => {
            let array = geo_array.as_wkb_view().to_array_ref();
            for_each_binary_view(array.as_binary_view(), |wkb| processor.push_wkb(wkb))
        }
        Wkt(_) => for_each_geometry(geo_array.as_wkt::<i32>(), |geometry| {
            processor.push_geometry(geometry)
        }),
        LargeWkt(_) => for_each_geometry(geo_array.as_wkt::<i64>(), |geometry| {
            processor.push_geometry(geometry)
        }),
        WktView(_) => for_each_geometry(geo_array.as_wkt_view(), |geometry| {
            processor.push_geometry(geometry)
        }),
    }
}

fn for_each_geometry<'a, A>(
    array: &'a A,
    mut process: impl FnMut(Option<&A::Item>) -> GeoDataFusionResult<()>,
) -> GeoDataFusionResult<()>
where
    A: GeoArrowArrayAccessor<'a>,
{
    for row in 0..array.len() {
        let geometry = array.get(row)?;
        process(geometry.as_ref())?;
    }
    Ok(())
}

fn for_each_binary<O: OffsetSizeTrait>(
    array: &GenericBinaryArray<O>,
    mut process: impl FnMut(Option<&[u8]>) -> GeoDataFusionResult<()>,
) -> GeoDataFusionResult<()> {
    for row in 0..array.len() {
        process(if array.is_null(row) {
            None
        } else {
            Some(array.value(row))
        })?;
    }
    Ok(())
}

fn for_each_binary_view(
    array: &BinaryViewArray,
    mut process: impl FnMut(Option<&[u8]>) -> GeoDataFusionResult<()>,
) -> GeoDataFusionResult<()> {
    for row in 0..array.len() {
        process(if array.is_null(row) {
            None
        } else {
            Some(array.value(row))
        })?;
    }
    Ok(())
}

struct LineMergeProcessor {
    // Retain only the final GeoArrow buffers plus one reusable input WKB buffer.
    builder: GeometryBuilder,
    directed: bool,
    wkb_buf: Vec<u8>,
}

impl LineMergeProcessor {
    fn new(output_type: GeometryType, directed: bool) -> Self {
        Self {
            builder: GeometryBuilder::new(output_type),
            directed,
            wkb_buf: Vec::new(),
        }
    }

    fn push_geometry(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoDataFusionResult<()> {
        let Some(geometry) = geometry else {
            self.builder.push_null();
            return Ok(());
        };

        self.wkb_buf.clear();
        write_geometry(
            &mut self.wkb_buf,
            geometry,
            &WriteOptions {
                endianness: Endianness::LittleEndian,
            },
        )
        .map_err(wkb_error)?;

        let geos_geometry = Geometry::new_from_wkb(&self.wkb_buf)?;
        if geos_geometry.is_empty()? {
            // PostGIS returns the original geometry for empty input, whereas GEOS
            // would collapse it to an empty GeometryCollection.
            self.builder.push_geometry(Some(geometry))?;
        } else {
            self.push_merged(geos_geometry)?;
        }
        Ok(())
    }

    fn push_wkb(&mut self, wkb: Option<&[u8]>) -> GeoDataFusionResult<()> {
        let Some(wkb) = wkb else {
            self.builder.push_null();
            return Ok(());
        };

        let geos_geometry = Geometry::new_from_wkb(wkb)?;
        if geos_geometry.is_empty()? {
            let geometry = Wkb::try_new(wkb).map_err(wkb_error)?;
            self.builder.push_geometry(Some(&geometry))?;
        } else {
            self.push_merged(geos_geometry)?;
        }
        Ok(())
    }

    fn push_merged(&mut self, geometry: Geometry) -> GeoDataFusionResult<()> {
        let merged = if self.directed {
            geometry.line_merge_directed()?
        } else {
            geometry.line_merge()?
        };
        let merged_wkb = merged.to_wkb()?;
        let merged = Wkb::try_new(&merged_wkb).map_err(wkb_error)?;
        self.builder.push_geometry(Some(&merged))?;
        Ok(())
    }

    fn finish(self) -> GeometryArray {
        self.builder.finish()
    }
}

fn wkb_error(error: impl std::fmt::Display) -> GeoDataFusionError {
    GeoArrowError::Wkb(error.to_string()).into()
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{Array, BinaryArray};
    use arrow_schema::Field;
    use datafusion::config::ConfigOptions;
    use datafusion::prelude::SessionContext;
    use geoarrow_schema::crs::Crs;
    use geoarrow_schema::{Dimension, WkbType};

    use super::*;
    use crate::udf::native::io::{AsBinary, AsText, GeomFromText};

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(LineMerge::default().into());
        ctx.register_udf(AsBinary.into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());
        ctx
    }

    #[tokio::test]
    async fn test_st_linemerge() {
        let ctx = ctx();

        // Explicitly noted examples come from the PostGIS documentation (CC-BY-SA-3.0).
        let cases = vec![
            (
                "MULTILINESTRING((10 160, 60 120), (120 140, 60 120), (120 140, 180 120))",
                "LINESTRING(10 160,60 120,120 140,180 120)",
                "PostGIS doc example: lines meeting two-at-a-time sew into one LineString",
            ),
            (
                "MULTILINESTRING((10 160, 60 120), (120 140, 60 120), (120 140, 180 120), (100 180, 120 140))",
                "MULTILINESTRING((10 160,60 120,120 140),(100 180,120 140),(120 140,180 120))",
                "degree-3 node: merge does not cross a junction of three lines",
            ),
            (
                "MULTILINESTRING((-29 -27,-30 -29.7,-36 -31,-45 -33),(-45.2 -33.2,-46 -32))",
                "MULTILINESTRING((-45.2 -33.2,-46 -32),(-29 -27,-30 -29.7,-36 -31,-45 -33))",
                "disjoint components are returned unchanged (as a MultiLineString)",
            ),
            (
                "LINESTRING(0 0, 1 1)",
                "LINESTRING(0 0,1 1)",
                "a lone LineString round-trips unchanged",
            ),
            (
                "POINT(0 0)",
                "GEOMETRYCOLLECTION EMPTY",
                "input with no line work yields an empty GeometryCollection",
            ),
            (
                "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
                "LINESTRING(0 0,1 0,1 1,0 1,0 0)",
                "PostGIS gotcha: polygons pass through to GEOS unfiltered, so the boundary ring is returned as a closed LineString rather than an empty GeometryCollection",
            ),
            (
                "LINESTRING EMPTY",
                "LINESTRING EMPTY",
                "PostGIS gotcha: empty input is returned as-is rather than collapsed to an empty GeometryCollection",
            ),
        ];

        for (input, expected, description) in cases {
            let sql = format!(
                "SELECT ST_AsText(ST_LineMerge(ST_GeomFromText('{}')))",
                input
            );
            let df = ctx
                .sql(&sql)
                .await
                .unwrap_or_else(|_| panic!("Failed to execute SQL for {}", description));

            let batch = df.collect().await.unwrap().into_iter().next().unwrap();
            let val = batch.column(0).as_string::<i32>().value(0);

            assert_eq!(val, expected, "Failed on {}: {}", description, input);
        }
    }

    #[tokio::test]
    async fn test_st_linemerge_directed() {
        let ctx = ctx();

        // Same input, contrasting the directed flag: with TRUE the disagreeing segments stay
        // split; with FALSE (the default) they all merge into a single LineString.
        let input = "MULTILINESTRING((60 30, 10 70), (120 50, 60 30), (120 50, 180 30))";
        let cases = vec![
            (
                "TRUE",
                "MULTILINESTRING((120 50,60 30,10 70),(120 50,180 30))",
                "directed: segments whose directions disagree are not merged",
            ),
            (
                "FALSE",
                "LINESTRING(180 30,120 50,60 30,10 70)",
                "undirected: same input merges fully when direction is ignored",
            ),
        ];

        for (directed, expected, description) in cases {
            let sql = format!(
                "SELECT ST_AsText(ST_LineMerge(ST_GeomFromText('{}'), {}))",
                input, directed
            );
            let df = ctx
                .sql(&sql)
                .await
                .unwrap_or_else(|_| panic!("Failed to execute SQL for {description}"));

            let batch = df.collect().await.unwrap().into_iter().next().unwrap();
            let val = batch.column(0).as_string::<i32>().value(0);

            assert_eq!(
                val, expected,
                "Failed on {description}: directed={directed}",
            );
        }
    }

    #[tokio::test]
    async fn test_st_linemerge_null() {
        let ctx = ctx();

        // Null geometries propagate to null, including alongside non-null rows in the same array.
        let df = ctx
            .sql(
                "WITH t(wkt) AS (VALUES ('LINESTRING(0 0, 1 1)'), (CAST(NULL AS TEXT))) \
                 SELECT ST_AsText(ST_LineMerge(ST_GeomFromText(wkt))) FROM t ORDER BY wkt NULLS LAST",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0).as_string::<i32>();

        assert_eq!(col.value(0), "LINESTRING(0 0,1 1)");
        assert!(
            col.is_null(1),
            "null input should propagate to a null output"
        );
    }

    #[tokio::test]
    async fn test_st_linemerge_input_encodings() {
        let ctx = ctx();
        let input = "MULTILINESTRING((0 0, 1 1), (1 1, 2 2))";
        let expected = "LINESTRING(0 0,1 1,2 2)";
        let expressions = [
            format!("ST_LineMerge(ST_GeomFromText('{input}'))"),
            format!("ST_LineMerge(ST_AsBinary(ST_GeomFromText('{input}')))"),
            format!("ST_LineMerge('{input}')"),
        ];

        for expression in expressions {
            let df = ctx
                .sql(&format!("SELECT ST_AsText({expression})"))
                .await
                .unwrap();
            let batch = df.collect().await.unwrap().into_iter().next().unwrap();
            let value = batch.column(0).as_string::<i32>().value(0);
            assert_eq!(value, expected, "failed for {expression}");
        }
    }

    #[tokio::test]
    async fn test_st_linemerge_wkb_empty_null_and_invalid() {
        let ctx = ctx();
        let df = ctx
            .sql(
                "WITH t(id, wkt) AS (\
                   VALUES \
                     (1, 'MULTILINESTRING((0 0, 1 1), (1 1, 2 2))'), \
                     (2, 'LINESTRING EMPTY'), \
                     (3, CAST(NULL AS TEXT))\
                 ) \
                 SELECT ST_AsText(ST_LineMerge(ST_AsBinary(ST_GeomFromText(wkt)))) \
                 FROM t ORDER BY id",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let values = batch.column(0).as_string::<i32>();
        assert_eq!(values.value(0), "LINESTRING(0 0,1 1,2 2)");
        assert_eq!(values.value(1), "LINESTRING EMPTY");
        assert!(values.is_null(2));

        let invalid_wkb = [0x01, 0x02, 0x00, 0x00, 0x00];
        let input = BinaryArray::from(vec![Some(invalid_wkb.as_slice())]);
        let input_field = Arc::new(
            Field::new("geometry", DataType::Binary, true).with_extension_type(WkbType::default()),
        );
        let udf = LineMerge::default();
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&input_field),
                scalar_arguments: &[],
            })
            .unwrap();
        let error = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(Arc::new(input))],
                arg_fields: vec![input_field],
                number_rows: 1,
                return_field,
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap_err();
        assert!(
            matches!(&error, DataFusionError::External(_)),
            "unexpected invalid-WKB error: {error}"
        );
    }

    #[test]
    fn test_st_linemerge_preserves_output_layout_and_metadata() {
        let crs = Crs::from_authority_code("EPSG:4326".to_string());
        let metadata = Arc::new(Metadata::new(crs.clone(), None));
        let input = geoarrow_array::test::linestring::array(CoordType::Separated, Dimension::XY)
            .with_metadata(metadata);
        let input_field = Arc::new(input.data_type().to_field("geometry", true));
        let udf = LineMerge::new(CoordType::Interleaved);
        let return_field = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: std::slice::from_ref(&input_field),
                scalar_arguments: &[],
            })
            .unwrap();
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(input.to_array_ref())],
            arg_fields: vec![input_field],
            number_rows: input.len(),
            return_field: return_field.clone(),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let ColumnarValue::Array(result) = udf.invoke_with_args(args).unwrap() else {
            panic!("expected an array result");
        };

        let result = from_arrow_array(&result, &return_field).unwrap();
        let GeoArrowType::Geometry(output_type) = result.data_type() else {
            panic!("expected a Geometry output");
        };
        assert_eq!(output_type.coord_type(), CoordType::Interleaved);
        assert_eq!(output_type.metadata().crs(), &crs);
    }
}
