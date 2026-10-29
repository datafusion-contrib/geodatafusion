use std::sync::{LazyLock, OnceLock};

use arrow_array::ArrayRef;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use geoarrow_schema::CoordType;
use geos::{Geom, Geometry, GeometryTypes};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::geos::adapter;

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
                "Returns a (set of) LineString(s) formed by sewing together the constituent line work of a MultiLineString. Polygonal input is normalized to its boundary linework before merging, including polygonal members of a GeometryCollection; every boundary ring stays whole and is not merged with the surrounding linework. Lines are joined at endpoints where exactly two lines meet; lines are not merged across intersections of three or more lines. When `directed` is true, lines are only merged when their directions agree. Non-linear inputs yield an empty GeometryCollection. M ordinates are dropped before GEOS; empty input is returned unchanged and may retain M.",
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
    Ok(adapter::geometry_return_field_with_coord_type(
        &args,
        &[0],
        "ST_LineMerge",
        Some(coord_type),
    )?)
}

fn line_merge_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(&args, |row, arrays, original, geometry, writer| {
        if geometry.is_empty()? {
            // PostGIS returns the original empty geometry. GEOS would collapse
            // it to an empty GeometryCollection.
            return Ok(Some(original.to_vec()));
        }

        let result = merge_linework(geometry, directed_value(arrays, row)?)?;
        Ok(Some(writer.write(&result)?))
    })
}

/// Reads the optional `directed` flag for one row.
///
/// DataFusion broadcasts a literal argument to the row count before the
/// adapter runs, so a literal and a real boolean column take the same path.
/// The shared strict adapter turns an explicitly null argument into a null
/// result before this value reaches GEOS.
fn directed_value(arrays: &[ArrayRef], row: usize) -> GeoDataFusionResult<bool> {
    if arrays.len() < 2 {
        return Ok(false);
    }
    Ok(adapter::boolean_value(arrays, 1, row, "ST_LineMerge")?.unwrap_or(false))
}

/// Merges the linework of `geometry` after replacing every polygonal member
/// with the rings of its boundary.
///
/// GEOS 3.11's line merger ignores polygon members, so a GeometryCollection
/// would silently lose their rings. PostGIS merges the boundary linework
/// instead; collecting the line members recursively and boundaryizing the
/// polygonal members keeps that contract on every supported GEOS version.
/// Each ring is kept whole: a closed ring has no free endpoints to merge with
/// the surrounding linework.
fn merge_linework(geometry: &Geometry, directed: bool) -> GeoDataFusionResult<Geometry> {
    let mut parts = Vec::new();
    if !collect_linework(geometry, &mut parts)? {
        return Ok(if directed {
            geometry.line_merge_directed()?
        } else {
            geometry.line_merge()?
        });
    }

    let linework = Geometry::create_multiline_string(parts)?;
    Ok(if directed {
        linework.line_merge_directed()?
    } else {
        linework.line_merge()?
    })
}

/// Collects the mergeable line members of `geometry` in traversal order and
/// reports whether any polygonal member was seen.
fn collect_linework(geometry: &impl Geom, parts: &mut Vec<Geometry>) -> GeoDataFusionResult<bool> {
    let mut has_polygon = false;
    match geometry.geometry_type()? {
        GeometryTypes::Polygon | GeometryTypes::MultiPolygon => {
            has_polygon = true;
            collect_linework(&geometry.boundary()?, parts)?;
        }
        GeometryTypes::GeometryCollection => {
            for index in 0..geometry.get_num_geometries()? {
                has_polygon |= collect_linework(&geometry.get_geometry_n(index)?, parts)?;
            }
        }
        GeometryTypes::MultiLineString => {
            for index in 0..geometry.get_num_geometries()? {
                let line = geometry.get_geometry_n(index)?;
                if !line.is_empty()? {
                    parts.push(Geom::clone(&line)?);
                }
            }
        }
        GeometryTypes::LineString | GeometryTypes::LinearRing => {
            if !geometry.is_empty()? {
                parts.push(Geom::clone(geometry)?);
            }
        }
        GeometryTypes::Point | GeometryTypes::MultiPoint => {}
    }
    Ok(has_polygon)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{Array, BinaryArray, BooleanArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait};
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_schema::{GeoArrowType, Metadata, WkbType};

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
                "polygon input is normalized to its boundary ring before merging",
            ),
            (
                "POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,4 6,6 6,6 4,4 4))",
                "MULTILINESTRING((0 0,10 0,10 10,0 10,0 0),(4 4,4 6,6 6,6 4,4 4))",
                "polygon holes become separate boundary lines",
            ),
            (
                "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((3 0,4 0,4 1,3 1,3 0)))",
                "MULTILINESTRING((0 0,1 0,1 1,0 1,0 0),(3 0,4 0,4 1,3 1,3 0))",
                "multipolygon rings become boundary lines",
            ),
            (
                "GEOMETRYCOLLECTION(LINESTRING(0 0,1 0),LINESTRING(1 0,2 0))",
                "LINESTRING(0 0,1 0,2 0)",
                "line members of a GeometryCollection still merge",
            ),
            (
                "GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 1,0 0)))",
                "LINESTRING(0 0,1 0,1 1,0 1,0 0)",
                "a polygonal member of a GeometryCollection contributes its boundary ring",
            ),
            (
                "GEOMETRYCOLLECTION(LINESTRING(1 0,2 0),POLYGON((0 0,1 0,1 1,0 1,0 0)))",
                "MULTILINESTRING((1 0,2 0),(0 0,1 0,1 1,0 1,0 0))",
                "a ring is kept whole and does not merge with a line touching one of its vertices",
            ),
            (
                "GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 1,0 0)),LINESTRING(1 0,2 0))",
                "MULTILINESTRING((1 0,2 0),(0 0,1 0,1 1,0 1,0 0))",
                "member order does not change the merged GeometryCollection result",
            ),
            (
                "GEOMETRYCOLLECTION(POLYGON((0 0,10 0,10 10,0 10,0 0),(4 4,4 6,6 6,6 4,4 4)))",
                "MULTILINESTRING((0 0,10 0,10 10,0 10,0 0),(4 4,4 6,6 6,6 4,4 4))",
                "a polygonal member keeps every ring of its boundary",
            ),
            (
                "GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((3 0,4 0,4 1,3 1,3 0))),MULTILINESTRING((10 0,11 0)))",
                "MULTILINESTRING((10 0,11 0),(0 0,1 0,1 1,0 1,0 0),(3 0,4 0,4 1,3 1,3 0))",
                "multipolygon rings and multiline members merge together",
            ),
            (
                "GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 1,0 0)),LINESTRING(0 0,1 0),LINESTRING(1 0,2 0))",
                "MULTILINESTRING((0 0,1 0,1 1,0 1,0 0),(0 0,1 0,2 0))",
                "line members still merge with each other next to a boundary ring",
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

        let df = ctx
            .sql(
                "SELECT ST_LineMerge(ST_GeomFromText(
                    'POLYGON Z ((0 0 1,1 0 2,1 1 3,0 1 4,0 0 1))'
                ))",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let array = geoarrow_array::array::GeometryArray::try_from((
            batch.column(0).as_ref(),
            batch.schema_ref().field(0),
        ))
        .unwrap();
        let geometry = array.value(0).unwrap();
        assert_eq!(geometry.dim(), geo_traits::Dimensions::Xyz);
        match geometry.as_type() {
            geo_traits::GeometryType::LineString(line) => {
                let first = line.coord(0).unwrap();
                assert_eq!(first.nth(2), Some(1.0));
            }
            _ => panic!("unexpected Z line merge output"),
        }

        // A polygonal member of a Z GeometryCollection keeps its ring Z values.
        let df = ctx
            .sql(
                "SELECT ST_LineMerge(ST_GeomFromText(
                    'GEOMETRYCOLLECTION Z (
                        POLYGON Z ((0 0 1,1 0 2,1 1 3,0 1 4,0 0 1)),
                        LINESTRING Z (1 0 2,2 0 9)
                    )'
                ))",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let array = geoarrow_array::array::GeometryArray::try_from((
            batch.column(0).as_ref(),
            batch.schema_ref().field(0),
        ))
        .unwrap();
        let geometry = array.value(0).unwrap();
        assert_eq!(geometry.dim(), geo_traits::Dimensions::Xyz);
        match geometry.as_type() {
            geo_traits::GeometryType::MultiLineString(lines) => {
                assert_eq!(lines.num_line_strings(), 2);
                let line = lines.line_string(0).unwrap();
                assert_eq!(line.coord(0).unwrap().nth(2), Some(2.0));
                assert_eq!(line.coord(1).unwrap().nth(2), Some(9.0));
                let ring = lines.line_string(1).unwrap();
                assert_eq!(ring.coord(0).unwrap().nth(2), Some(1.0));
                assert_eq!(ring.num_coords(), 5);
            }
            _ => panic!("unexpected Z GeometryCollection line merge output"),
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

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_LineMerge(
                    ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'),
                    TRUE
                ))",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(
            batch.column(0).as_string::<i32>().value(0),
            "LINESTRING(0 0,1 0,1 1,0 1,0 0)"
        );

        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_LineMerge(
                    ST_GeomFromText(
                        'GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 1,0 0)),LINESTRING(1 0,2 0))'
                    ),
                    TRUE
                ))",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert_eq!(
            batch.column(0).as_string::<i32>().value(0),
            "MULTILINESTRING((1 0,2 0),(0 0,1 0,1 1,0 1,0 0))"
        );
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

    /// The native GeoArrow builder rejects a `GeometryCollection` nested inside
    /// another one, so `ST_GeomFromText` cannot produce this input. The
    /// recursive traversal contract is exercised through a real typed WKB
    /// field and a real `directed` column consumed by public SQL instead.
    #[tokio::test]
    async fn test_st_linemerge_nested_geometry_collection_from_typed_wkb() {
        // Independent WKT to WKB encoding; the fixture never goes through the
        // UDF under test.
        let geometry: wkt::Wkt<f64> =
            "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 1,0 0))))"
                .parse()
                .unwrap();
        let mut wkb = Vec::new();
        wkb::writer::write_geometry(&mut wkb, &geometry, &wkb::writer::WriteOptions::default())
            .unwrap();

        let field = GeoArrowType::Wkb(WkbType::new(Arc::new(Metadata::default())))
            .to_field("geometry", true);
        let directed = Field::new("directed", DataType::Boolean, true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new([Arc::new(field), Arc::new(directed)])),
            vec![
                Arc::new(BinaryArray::from(vec![wkb.as_slice(), wkb.as_slice()])),
                Arc::new(BooleanArray::from(vec![false, true])),
            ],
        )
        .unwrap();

        let ctx = ctx();
        ctx.register_batch("nested_wkb", batch).unwrap();

        let dataframe = ctx
            .sql("SELECT ST_AsText(ST_LineMerge(geometry, directed)) FROM nested_wkb")
            .await
            .unwrap();
        let batches = dataframe.collect().await.unwrap();
        let batch = batches.into_iter().next().unwrap();
        let lines = batch.column(0).as_string::<i32>();
        for row in 0..lines.len() {
            assert_eq!(
                lines.value(row),
                "LINESTRING(0 0,1 0,1 1,0 1,0 0)",
                "row {row}"
            );
        }
    }
}
