use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::{Array, BinaryArray};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{WkbArray, from_arrow_array};
use geoarrow_array::cast::{from_wkb, to_wkb};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata};
use geos::{Geom, Geometry};

use crate::error::GeoDataFusionResult;

/// Sews together the component lines of a (multi)linestring.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineMerge {
    signature: Signature,
    coord_type: CoordType,
}

impl LineMerge {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            // A single geometry argument, optionally followed by the `directed` boolean.
            signature: Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for LineMerge {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineMerge {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_linemerge"
    }

    fn signature(&self) -> &Signature {
        &self.signature
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
    // Parse the directed argument
    let directed = parse_directed(&args)?;

    let arrays = ColumnarValue::values_to_arrays(&args.args[0..1])?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let metadata = geo_array.data_type().metadata().clone();

    // Bridge to GEOS via WKB.
    //
    // Assumptions:
    // - No arrow arrays (record batches?) larger than 2GB.
    // - A trip through WKB is probably the fastest path. The only other obvious option is using geo_types as an intermediate,
    //   but that is probably going to STILL require going out to WKB, since WKB looks to be the cheapest interchange format.
    let wkb_array = to_wkb::<i32>(geo_array.as_ref())?;

    let mut merged: Vec<Option<Vec<u8>>> = Vec::with_capacity(wkb_array.inner().len());
    for maybe_wkb in wkb_array.inner() {
        match maybe_wkb {
            // Null inputs propagate to null outputs.
            None => merged.push(None),
            Some(wkb) => {
                let geom = Geometry::new_from_wkb(wkb)?;
                if geom.is_empty()? {
                    // PostGIS returns the original geometry for empty input,
                    // whereas GEOS would collapse it to an empty GeometryCollection.
                    // Preserve the PostGIS behavior.
                    // Thanks to Dewy for pointing out from the SedonaDB implementation:
                    // https://github.com/apache/sedona-db/blob/cf8b9ceaf7a78c042bf73ab0e5040187046fe256/c/sedona-geos/src/st_line_merge.rs#L105-L131!
                    merged.push(Some(wkb.to_vec()));
                } else {
                    // Branch on directed or not
                    let result = if directed {
                        geom.line_merge_directed()?
                    } else {
                        geom.line_merge()?
                    };
                    merged.push(Some(result.to_wkb()?));
                }
            }
        }
    }

    let result_wkb = WkbArray::new(merged.into_iter().collect::<BinaryArray>(), metadata);
    let to_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    let result = from_wkb(&result_wkb, to_type)?;
    Ok(ColumnarValue::Array(result.to_array_ref()))
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(LineMerge::default().into());
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
}
