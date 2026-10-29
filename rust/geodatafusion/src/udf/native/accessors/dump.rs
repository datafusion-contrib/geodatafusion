use std::sync::{Arc, OnceLock};

use arrow_array::builder::{ArrayBuilder, Int32Builder, ListBuilder};
use arrow_array::{ArrayRef, ListArray, StructArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType, Metadata};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::is_empty::is_geometry_topologically_empty;

/// Decomposes a geometry into its atomic components (POINT, LINESTRING, POLYGON).
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Dump {
    coord_type: CoordType,
}

impl Dump {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Dump {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Dump {
    fn name(&self) -> &str {
        "st_dump"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from metadata in return_field_from_args.".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata =
            Arc::new(Metadata::try_from(args.arg_fields[0].as_ref()).unwrap_or_default());
        let output_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(output_field(&output_type))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(dump_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Decomposes a geometry into its atomic components (Point, LineString, Polygon). \
                 Multi-geometries and GeometryCollections are split (recursively) into their parts. \
                 Atomic geometries are returned unchanged. This includes Polygons, which are returned whole. \
                 Returns, per input row, a list of `(path, geom)` structs where \
                 `path` is the 1-based navigation path to the component (empty for an atomic input). \
                 Use `unnest` if you want to expand into a row per component instead of row per input geom. \
                 Empty inputs (recursively, per the same definition as ST_Empty) produce zero components.",
                "ST_Dump(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn path_values_field() -> FieldRef {
    Arc::new(Field::new("item", DataType::Int32, false))
}

fn path_field() -> Field {
    Field::new("path", DataType::List(path_values_field()), false)
}

fn item_field(geom_type: &GeometryType) -> FieldRef {
    Arc::new(Field::new(
        "item",
        DataType::Struct(item_struct_fields(geom_type)),
        false,
    ))
}

fn item_struct_fields(geom_type: &GeometryType) -> Fields {
    Fields::from(vec![path_field(), geom_type.to_field("geom", true)])
}

fn output_field(geom_type: &GeometryType) -> FieldRef {
    Arc::new(Field::new("", DataType::List(item_field(geom_type)), true))
}

fn dump_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .expect("should have one input argument or else planning will fail");
    let geo_array = from_arrow_array(&array, &args.arg_fields[0])?;
    let geom_type =
        GeometryType::new(geo_array.data_type().metadata().clone()).with_coord_type(coord_type);

    let geo_array_ref = geo_array.as_ref();
    let list_array = downcast_geoarrow_array!(geo_array_ref, dump_array, &geom_type)?;

    Ok(ColumnarValue::Array(Arc::new(list_array)))
}

/// The main decomposition loop.
fn dump_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    geom_type: &GeometryType,
) -> GeoDataFusionResult<ListArray> {
    let mut geom_builder = GeometryBuilder::new(geom_type.clone());
    let mut path_builder = ListBuilder::new(Int32Builder::new()).with_field(path_values_field());
    let mut path_stack = Vec::new();
    let mut row_lengths = Vec::with_capacity(array.len());
    let mut row_validity = NullBufferBuilder::new(array.len());

    for maybe_geom in array.iter() {
        let start = path_builder.len();
        match maybe_geom.transpose()? {
            Some(geom) => {
                row_validity.append_non_null();
                // Topologically empty geometries produce an empty, non-null component list.
                if !is_geometry_topologically_empty(&geom) {
                    dump_geometry(&geom, &mut geom_builder, &mut path_builder, &mut path_stack)?;
                }
            }
            None => row_validity.append_null(),
        }
        row_lengths.push(path_builder.len() - start);
    }

    let path_array = path_builder.finish();
    let geom_array = geom_builder.finish().into_array_ref();
    let struct_array = StructArray::new(
        item_struct_fields(geom_type),
        vec![Arc::new(path_array) as ArrayRef, geom_array],
        None,
    );
    Ok(ListArray::new(
        item_field(geom_type),
        OffsetBuffer::<i32>::from_lengths(row_lengths),
        Arc::new(struct_array),
        row_validity.finish(),
    ))
}

/// Visitor for every non-topologically-empty geometry
/// which emits atomic geoms directly, and recursively dumps children in containers.
fn dump_geometry(
    geom: &impl GeometryTrait<T = f64>,
    geom_builder: &mut GeometryBuilder,
    path_builder: &mut ListBuilder<Int32Builder>,
    path_stack: &mut Vec<i32>,
) -> GeoDataFusionResult<()> {
    use geo_traits::GeometryType::*;

    match geom.as_type() {
        Point(_) | LineString(_) | Polygon(_) | Rect(_) | Triangle(_) | Line(_) => {
            geom_builder.push_geometry(Some(geom))?;
            path_builder.append_value(path_stack.iter().map(|&i| Some(i)));
        }
        MultiPoint(g) => dump_multi_children(g.points(), geom_builder, path_builder, path_stack)?,
        MultiLineString(g) => {
            dump_multi_children(g.line_strings(), geom_builder, path_builder, path_stack)?
        }
        MultiPolygon(g) => {
            dump_multi_children(g.polygons(), geom_builder, path_builder, path_stack)?
        }
        GeometryCollection(g) => {
            dump_multi_children(g.geometries(), geom_builder, path_builder, path_stack)?
        }
    }
    Ok(())
}

/// Process children of a multi-geometry container.
fn dump_multi_children(
    children: impl Iterator<Item = impl GeometryTrait<T = f64>>,
    geom_builder: &mut GeometryBuilder,
    path_builder: &mut ListBuilder<Int32Builder>,
    path_stack: &mut Vec<i32>,
) -> GeoDataFusionResult<()> {
    for (i, child) in children.enumerate() {
        path_stack.push(i as i32 + 1);
        dump_geometry(&child, geom_builder, path_builder, path_stack)?;
        path_stack.pop();
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_schema::DataType;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::cast::to_wkt;

    use super::*;
    use crate::udf::native::io::{AsText, GeomFromText};

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_udf(Dump::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsText.into());
        ctx
    }

    /// Run `ST_Dump` on a single WKT input and return the result decoded as WKT.
    async fn dump_rows(ctx: &SessionContext, wkt: &str) -> Vec<(Vec<i32>, String)> {
        let sql = format!("SELECT ST_Dump(ST_GeomFromText('{wkt}'))");
        let df = ctx.sql(&sql).await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let list = batch.column(0).as_list::<i32>();
        assert!(!list.is_null(0), "unexpected NULL dump for {wkt}");

        let components = list.value(0);
        let structs = components.as_struct();
        let path_col = structs.column(0).as_list::<i32>();

        let DataType::Struct(fields) = structs.data_type() else {
            panic!("ST_Dump must return a list of structs");
        };
        let geo = from_arrow_array(structs.column(1).as_ref(), fields[1].as_ref()).unwrap();
        let wkt_ref = to_wkt::<i32>(geo.as_ref()).unwrap().into_array_ref();
        let wkts = wkt_ref.as_string::<i32>();

        (0..structs.len())
            .map(|i| {
                let path = path_col
                    .value(i)
                    .as_primitive::<Int32Type>()
                    .values()
                    .to_vec();
                (path, wkts.value(i).to_string())
            })
            .collect()
    }

    #[tokio::test]
    async fn test_atomic_round_trips() {
        let ctx = ctx();
        assert_eq!(
            dump_rows(&ctx, "POINT(1 2)").await,
            vec![(vec![], "POINT(1 2)".to_string())],
            "Point is atomic and should round-trip to itself.",
        );

        assert_eq!(
            dump_rows(&ctx, "LINESTRING(0 0,1 1)").await,
            vec![(vec![], "LINESTRING(0 0,1 1)".to_string())],
            "LineString is atomic and should round-trip to itself.",
        );

        assert_eq!(
            dump_rows(&ctx, "POLYGON((0 0,0 3,3 3,3 0,0 0),(1 1,1 2,2 2,2 1,1 1))").await,
            vec![(
                vec![],
                "POLYGON((0 0,0 3,3 3,3 0,0 0),(1 1,1 2,2 2,2 1,1 1))".to_string()
            )],
            "Polygon is atomic and should round-trip to itself.",
        );
    }

    #[tokio::test]
    async fn test_multi_geometries_decompose() {
        let ctx = ctx();
        assert_eq!(
            dump_rows(&ctx, "MULTIPOINT(0 0,1 1)").await,
            vec![
                (vec![1], "POINT(0 0)".to_string()),
                (vec![2], "POINT(1 1)".to_string()),
            ],
        );

        assert_eq!(
            dump_rows(&ctx, "MULTILINESTRING((0 0,1 1),(2 2,3 3))").await,
            vec![
                (vec![1], "LINESTRING(0 0,1 1)".to_string()),
                (vec![2], "LINESTRING(2 2,3 3)".to_string()),
            ],
        );

        assert_eq!(
            dump_rows(
                &ctx,
                "MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((10 10,10 20,20 20,20 10,10 10)))"
            )
            .await,
            vec![
                (vec![1], "POLYGON((0 0,0 1,1 1,1 0,0 0))".to_string()),
                (
                    vec![2],
                    "POLYGON((10 10,10 20,20 20,20 10,10 10))".to_string()
                ),
            ],
        );
    }

    #[tokio::test]
    async fn test_collection_decomposition() {
        let ctx = ctx();
        assert_eq!(
            dump_rows(&ctx, "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 2))").await,
            vec![
                (vec![1], "POINT(0 0)".to_string()),
                (vec![2], "LINESTRING(1 1,2 2)".to_string()),
            ],
            "GeometryCollection should be decomposed into components"
        );

        assert_eq!(
            dump_rows(
                &ctx,
                "GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((10 10,10 20,20 20,20 10,10 10))),POINT(5 5))"
            )
            .await,
            vec![
                (vec![1, 1], "POLYGON((0 0,0 1,1 1,1 0,0 0))".to_string()),
                (vec![1, 2], "POLYGON((10 10,10 20,20 20,20 10,10 10))".to_string()),
                (vec![2], "POINT(5 5)".to_string()),
            ],
            "Nesting a MultiPolygon in a GeometryCollection should unroll into atomic Polygons"
        );
    }

    #[tokio::test]
    async fn test_dimension_preservation() {
        let ctx = ctx();
        assert_eq!(
            dump_rows(&ctx, "MULTIPOINT Z (0 0 1,1 1 2)").await,
            vec![
                (vec![1], "POINT Z(0 0 1)".to_string()),
                (vec![2], "POINT Z(1 1 2)".to_string()),
            ],
        );
        assert_eq!(
            dump_rows(&ctx, "MULTIPOINT ZM (0 0 1 5,1 1 2 6)").await,
            vec![
                (vec![1], "POINT ZM(0 0 1 5)".to_string()),
                (vec![2], "POINT ZM(1 1 2 6)".to_string()),
            ],
        );
    }

    #[tokio::test]
    async fn test_dump_empty_inputs_yield_zero_rows() {
        let ctx = ctx();
        assert_eq!(dump_rows(&ctx, "POINT EMPTY").await, vec![]);
        assert_eq!(dump_rows(&ctx, "LINESTRING EMPTY").await, vec![]);
        assert_eq!(dump_rows(&ctx, "POLYGON EMPTY").await, vec![]);
        assert_eq!(dump_rows(&ctx, "MULTIPOINT EMPTY").await, vec![]);
        assert_eq!(dump_rows(&ctx, "MULTILINESTRING EMPTY").await, vec![]);
        assert_eq!(dump_rows(&ctx, "MULTIPOLYGON EMPTY").await, vec![]);
        assert_eq!(dump_rows(&ctx, "GEOMETRYCOLLECTION EMPTY").await, vec![]);
    }

    #[tokio::test]
    async fn test_dump_recursive_empty_collection_yields_zero_rows() {
        let ctx = ctx();
        let wkt = "GEOMETRYCOLLECTION(POINT EMPTY, LINESTRING EMPTY, POLYGON EMPTY, MULTIPOINT EMPTY, MULTILINESTRING EMPTY, MULTIPOLYGON EMPTY)";
        assert_eq!(dump_rows(&ctx, wkt).await, vec![]);
    }

    #[tokio::test]
    async fn test_dump_mixed_empty_and_nonempty_children_emits_empty_leaves() {
        let ctx = ctx();
        assert_eq!(
            dump_rows(&ctx, "GEOMETRYCOLLECTION(POINT(0 0), POLYGON EMPTY)").await,
            vec![
                (vec![1], "POINT(0 0)".to_string()),
                (vec![2], "POLYGON EMPTY".to_string()),
            ],
            "A single non-empty geometry in a collection should make the collection non-empty, including empty children"
        );
    }

    #[tokio::test]
    async fn test_dump_null_yields_null() {
        let ctx = ctx();
        let df = ctx
            .sql("SELECT ST_Dump(ST_GeomFromText(NULL))")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let list = batch.column(0).as_list::<i32>();
        assert!(list.is_null(0), "ST_Dump(NULL) should be NULL");
    }

    #[tokio::test]
    async fn test_unnest_expands_to_multiple_rows() {
        let ctx = ctx();
        let df = ctx
            .sql("SELECT unnest(ST_Dump(ST_GeomFromText('MULTIPOINT(0 0,1 1)'))) AS comp")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 2,
            "unnest should expand a 2-component dump into 2 rows"
        );
        assert!(matches!(
            batches[0].column(0).data_type(),
            DataType::Struct(_)
        ));
    }

    #[tokio::test]
    async fn test_st_dump_field_access_sql() {
        let ctx = ctx();
        let df = ctx
            .sql(
                // This SQL illustrates how to get a PostGIS-style shape with multiple rows per non-atomic geometry.
                "SELECT ST_AsText(comp.geom) AS wkt, comp.path AS path \
                 FROM (SELECT unnest(ST_Dump(ST_GeomFromText('MULTIPOINT(0 0,1 1)'))) AS comp)",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();

        // `comp['geom']` keeps the geometry's extension type, so ST_AsText accepts it directly.
        let wkt = batch.column_by_name("wkt").unwrap().as_string::<i32>();
        assert_eq!(
            (0..wkt.len()).map(|i| wkt.value(i)).collect::<Vec<_>>(),
            vec!["POINT(0 0)", "POINT(1 1)"],
        );

        // `comp['path']` extracts the navigation-path list (a single 1-based index for each part).
        let path = batch.column_by_name("path").unwrap().as_list::<i32>();
        assert_eq!(path.len(), 2);
        assert_eq!(path.value(0).as_primitive::<Int32Type>().values(), &[1]);
        assert_eq!(path.value(1).as_primitive::<Int32Type>().values(), &[2]);
    }
}
