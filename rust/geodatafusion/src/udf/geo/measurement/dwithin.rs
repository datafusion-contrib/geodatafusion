use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::{DataType, Field};
use datafusion::arrow::compute::cast as cast_array;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{Column, ScalarValue};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Expr;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, WrapArray, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;

use crate::error::GeoDataFusionResult;
pub use crate::udf::geo::relationships::DWithin;
use crate::udf::native::bounding_box::util::{GeometryBounds, expand_bounds, geometry_bounds};

/// A conservative planar XY envelope for a spatial index candidate query.
///
/// `envelope` is `[min_x, min_y, max_x, max_y]`. The bounds are expanded
/// outward to cover the distance condition before the caller queries an index.
#[derive(Debug, Clone, PartialEq)]
pub struct DWithinIndexFilter {
    /// The indexed geometry column, including its optional qualifier.
    pub column: Column,
    /// A conservative planar XY envelope in coordinate-system units.
    pub envelope: [f64; 4],
}

/// Extract the indexable shape of a root `ST_DWithin` expression.
///
/// The helper accepts exactly one bare geometry column and one folded geometry
/// literal with a finite, non-negative constant radius. It does not recurse
/// through `AND`, `OR`, or `NOT`; callers should pass individual pushdown
/// conjuncts.
///
/// A returned envelope is the directed-rounded mathematical bounds condition
/// enforced by [`DWithin`]. `ST_DWithin` checks the same condition before it
/// invokes the distance kernel, so every `ST_DWithin` true row is a candidate
/// without assuming an error bound for that kernel. Providers must declare an
/// inexact pushdown and retain the original predicate for exact rechecking.
pub fn extract_dwithin_index_filter(expr: &Expr) -> Result<Option<DWithinIndexFilter>> {
    Ok(extract_dwithin_index_filter_impl(expr)?)
}

fn extract_dwithin_index_filter_impl(
    expr: &Expr,
) -> GeoDataFusionResult<Option<DWithinIndexFilter>> {
    let Expr::ScalarFunction(function) = expr else {
        return Ok(None);
    };

    if !function.func.inner().is::<DWithin>() || function.args.len() != 3 {
        return Ok(None);
    }

    let (column, geometry, metadata) = match (&function.args[0], &function.args[1]) {
        (Expr::Column(column), Expr::Literal(geometry, metadata)) => (column, geometry, metadata),
        (Expr::Literal(geometry, metadata), Expr::Column(column)) => (column, geometry, metadata),
        _ => return Ok(None),
    };

    let Expr::Literal(radius, _) = &function.args[2] else {
        return Ok(None);
    };

    let Some(bounds) = literal_geometry_bounds(geometry, metadata.as_ref())? else {
        return Ok(None);
    };
    let Some(radius) = literal_radius(radius)? else {
        return Ok(None);
    };

    let envelope = expand_bounds(bounds, radius);
    if !envelope.iter().all(|value| value.is_finite()) {
        return Ok(None);
    }

    Ok(Some(DWithinIndexFilter {
        column: column.clone(),
        envelope,
    }))
}

fn literal_geometry_bounds(
    value: &ScalarValue,
    metadata: Option<&FieldMetadata>,
) -> GeoDataFusionResult<Option<[f64; 4]>> {
    if value.is_null() {
        return Ok(None);
    }

    let array = value.to_array()?;
    let mut field = Field::new("geometry", value.data_type(), true);
    if let Some(metadata) = metadata {
        field = metadata.add_to_field(field);
    }

    let Ok(geo_type) = GeoArrowType::from_arrow_field(&field) else {
        return Ok(None);
    };
    let geo_array = geo_type.wrap_array(array.as_ref())?;
    let Some(Some(bounds)) = geo_array_bounds(geo_array.as_ref())?.into_iter().next() else {
        return Ok(None);
    };

    Ok(match bounds {
        GeometryBounds::Finite(bounds) => {
            Some([bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()])
        }
        GeometryBounds::Empty | GeometryBounds::NonFinite => None,
    })
}

fn literal_radius(value: &ScalarValue) -> GeoDataFusionResult<Option<f64>> {
    if value.is_null() || !is_supported_radius_type(&value.data_type()) {
        return Ok(None);
    }

    let array = value.to_array()?;
    let radius = radius_values(array.as_ref())?.into_iter().next().flatten();

    Ok(radius.filter(|radius| radius.is_finite() && *radius >= 0.0))
}

fn geo_array_bounds(array: &dyn GeoArrowArray) -> GeoDataFusionResult<Vec<Option<GeometryBounds>>> {
    downcast_geoarrow_array!(array, geo_array_bounds_impl)
}

fn geo_array_bounds_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<Vec<Option<GeometryBounds>>> {
    let mut bounds = Vec::with_capacity(array.len());

    for item in array.iter() {
        let Some(geometry) = item else {
            bounds.push(None);
            continue;
        };

        let geometry = geometry?;
        bounds.push(Some(geometry_bounds(&geometry)));
    }

    Ok(bounds)
}

fn array_is_all_null(array: &dyn Array) -> bool {
    matches!(array.data_type(), DataType::Null)
        || (!array.is_empty() && array.null_count() == array.len())
}

fn radius_values(array: &dyn Array) -> GeoDataFusionResult<Vec<Option<f64>>> {
    if array_is_all_null(array) {
        return Ok(vec![None; array.len()]);
    }

    if !is_supported_radius_type(array.data_type()) {
        return Err(DataFusionError::Execution(format!(
            "ST_DWithin radius must be numeric, got {}",
            array.data_type()
        ))
        .into());
    }

    let array = cast_array(array, &DataType::Float64)?;
    let array = array.as_primitive::<Float64Type>();
    Ok((0..array.len())
        .map(|index| (!array.is_null(index)).then(|| array.value(index)))
        .collect())
}

fn is_supported_radius_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, LazyLock, Mutex};

    use arrow_array::builder::BooleanBuilder;
    use arrow_array::cast::AsArray;
    use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{Schema, SchemaRef};
    use datafusion::arrow::compute::filter_record_batch;
    use datafusion::catalog::Session;
    use datafusion::common::TableReference;
    use datafusion::datasource::memory::MemTable;
    use datafusion::datasource::{TableProvider, TableType};
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        TableProviderFilterPushDown, Volatility,
    };
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::SessionContext;
    use geo::Distance;
    use geoarrow_array::GeoArrowArray;
    use geoarrow_array::array::{WktArray, from_arrow_array};
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_schema::{Dimension, Metadata, PointType};
    use wkt::wkt;

    use super::*;
    use crate::udf::native::bounding_box::util::total_bounds;

    async fn execute_bools(ctx: &SessionContext, sql: &str) -> Vec<Option<bool>> {
        let dataframe = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("failed to plan {sql}: {error}"));
        let batches = dataframe
            .collect()
            .await
            .unwrap_or_else(|error| panic!("failed to execute {sql}: {error}"));
        let column = batches[0].column(0).as_boolean();
        (0..column.len())
            .map(|index| (!column.is_null(index)).then(|| column.value(index)))
            .collect()
    }

    async fn assert_query_error(ctx: &SessionContext, sql: &str, expected: &str) {
        let error = match ctx.sql(sql).await {
            Ok(dataframe) => dataframe.collect().await.unwrap_err(),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains(expected),
            "expected error containing {expected:?}, got {error}"
        );
    }

    #[derive(Debug, Eq, PartialEq, Hash)]
    struct FakeDWithin;

    static FAKE_SIGNATURE: LazyLock<Signature> =
        LazyLock::new(|| Signature::any(3, Volatility::Immutable));

    impl ScalarUDFImpl for FakeDWithin {
        fn name(&self) -> &str {
            "st_dwithin"
        }

        fn signature(&self) -> &Signature {
            &FAKE_SIGNATURE
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Boolean)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Err(DataFusionError::Internal(
                "fake ST_DWithin must not be invoked".to_string(),
            ))
        }
    }

    fn folded_point_literal() -> (ScalarValue, FieldMetadata) {
        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!(POINT(0.0 0.0))));
        let geometry = builder.finish();
        let field = geometry.data_type().to_field("geometry", true);
        let value = ScalarValue::try_from_array(&geometry.to_array_ref(), 0).unwrap();
        (value, FieldMetadata::from(field.metadata()))
    }

    #[test]
    fn test_extract_dwithin_identity_qualifier_and_unsupported_shapes() {
        let dwithin = ScalarUDF::from(DWithin::new());
        let column = Column::new(Some(TableReference::bare("shapes")), "geometry");
        let (geometry, metadata) = folded_point_literal();
        let radius = Expr::Literal(ScalarValue::Float64(Some(1.0)), None);

        let expression = dwithin.call(vec![
            Expr::Column(column.clone()),
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            radius.clone(),
        ]);
        let extracted = extract_dwithin_index_filter(&expression)
            .unwrap()
            .expect("qualified DWithin should be extractable");
        assert_eq!(extracted.column, column);

        let reversed = dwithin.call(vec![
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            Expr::Column(column.clone()),
            radius.clone(),
        ]);
        let extracted = extract_dwithin_index_filter(&reversed)
            .unwrap()
            .expect("reversed DWithin should be extractable");
        assert_eq!(extracted.column, column);

        let fake = ScalarUDF::from(FakeDWithin).call(vec![
            Expr::Column(column.clone()),
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            radius.clone(),
        ]);
        assert!(
            extract_dwithin_index_filter(&fake).unwrap().is_none(),
            "a different UDF named ST_DWithin must not match"
        );

        let both_columns = dwithin.call(vec![
            Expr::Column(column.clone()),
            Expr::Column(Column::new_unqualified("other_geometry")),
            radius.clone(),
        ]);
        assert!(
            extract_dwithin_index_filter(&both_columns)
                .unwrap()
                .is_none()
        );

        let dynamic_radius = dwithin.call(vec![
            Expr::Column(column.clone()),
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            Expr::Column(Column::new_unqualified("radius")),
        ]);
        assert!(
            extract_dwithin_index_filter(&dynamic_radius)
                .unwrap()
                .is_none()
        );

        let non_finite_radius = dwithin.call(vec![
            Expr::Column(column.clone()),
            Expr::Literal(geometry.clone(), Some(metadata.clone())),
            Expr::Literal(ScalarValue::Float64(Some(f64::NAN)), None),
        ]);
        assert!(
            extract_dwithin_index_filter(&non_finite_radius)
                .unwrap()
                .is_none()
        );

        let disjunction = Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
            Box::new(expression.clone()),
            datafusion::logical_expr::Operator::Or,
            Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
        ));
        assert!(
            extract_dwithin_index_filter(&disjunction)
                .unwrap()
                .is_none()
        );

        let negation = Expr::Not(Box::new(expression));
        assert!(extract_dwithin_index_filter(&negation).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_dwithin_scalar_semantics() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(3, 4), 5)").await;
        assert_eq!(values, vec![Some(true)]);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(3, 4), 4)").await;
        assert_eq!(values, vec![Some(false)]);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(3, 4), ST_Point(0, 0), 5)").await;
        assert_eq!(values, vec![Some(true)]);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(0, 0), 0)").await;
        assert_eq!(values, vec![Some(true)]);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), 0)").await;
        assert_eq!(values, vec![Some(false)]);
    }

    #[tokio::test]
    async fn test_dwithin_non_point_geometries() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        let values = execute_bools(
            &ctx,
            "SELECT ST_DWithin(\
                ST_Point(1, 1), \
                ST_GeomFromText('LINESTRING(0 0, 2 0)'), \
                1\
            )",
        )
        .await;
        assert_eq!(values, vec![Some(true)]);

        let values = execute_bools(
            &ctx,
            "SELECT ST_DWithin(\
                ST_Point(1, 1), \
                ST_GeomFromText('LINESTRING(0 0, 2 0)'), \
                0.9999999999999999\
            )",
        )
        .await;
        assert_eq!(values, vec![Some(false)]);

        let values = execute_bools(
            &ctx,
            "SELECT ST_DWithin(\
                ST_Point(1, 1), \
                ST_GeomFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'), \
                0\
            )",
        )
        .await;
        assert_eq!(values, vec![Some(true)]);
    }

    #[tokio::test]
    async fn test_dwithin_null_and_empty() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        let values =
            execute_bools(&ctx, "SELECT ST_DWithin(NULL, ST_Point(0, 0), 1) IS NULL").await;
        assert_eq!(values, vec![Some(true)]);

        let values = execute_bools(
            &ctx,
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(0, 0), CAST(NULL AS DOUBLE)) IS NULL",
        )
        .await;
        assert_eq!(values, vec![Some(true)]);

        let values = execute_bools(
            &ctx,
            "SELECT ST_DWithin(\
                ST_GeomFromText('POINT EMPTY'), \
                ST_Point(0, 0), \
                1\
            )",
        )
        .await;
        assert_eq!(values, vec![Some(false)]);

        let values = execute_bools(
            &ctx,
            "SELECT ST_DWithin(\
                ST_GeomFromText('POLYGON EMPTY'), \
                ST_GeomFromText('POLYGON EMPTY'), \
                0\
            )",
        )
        .await;
        assert_eq!(values, vec![Some(false)]);
    }

    #[tokio::test]
    async fn test_dwithin_array_scalar_combinations() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        let mut builder = PointBuilder::new(PointType::new(Dimension::XY, Default::default()));
        builder.push_point(Some(&wkt!(POINT(0.0 0.0))));
        builder.push_point(Some(&wkt!(POINT(3.0 4.0))));
        builder.push_point(Some(&wkt!(POINT(6.0 8.0))));
        builder.push_null();
        let geometry = builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                geometry.into_array_ref(),
            ],
        )
        .unwrap();
        ctx.register_batch("points", batch).unwrap();

        let batches = ctx
            .sql(
                "SELECT \
                    ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 5), \
                    ST_DWithin(ST_GeomFromText('POINT(0 0)'), geometry, 5) \
                 FROM points \
                 ORDER BY id",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &batches[0];

        for column_index in 0..2 {
            let column = batch.column(column_index).as_boolean();
            assert!(column.value(0));
            assert!(column.value(1));
            assert!(!column.value(2));
            assert!(column.is_null(3));
        }
    }

    #[tokio::test]
    async fn test_dwithin_invalid_radius_and_malformed_input() {
        let ctx = SessionContext::new();
        crate::register(&ctx);

        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), -1)",
            "finite and non-negative",
        )
        .await;
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), CAST('NaN' AS DOUBLE))",
            "finite and non-negative",
        )
        .await;
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), CAST('Infinity' AS DOUBLE))",
            "finite and non-negative",
        )
        .await;
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(ST_Point(0, 0), ST_Point(1, 0), '1')",
            "Function 'st_dwithin' requires Float64, but received String",
        )
        .await;
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(\
                ST_Point(CAST('NaN' AS DOUBLE), 0), \
                ST_Point(0, 0), \
                1\
            )",
            "requires finite XY coordinates",
        )
        .await;
        assert_query_error(
            &ctx,
            "SELECT ST_DWithin(ST_GeomFromText('not a geometry'), ST_Point(0, 0), 1)",
            "",
        )
        .await;
    }

    #[tokio::test]
    async fn test_distance_boundary_does_not_drive_candidate_extraction() {
        let query_x = -1.0;
        let row_x = 2f64.powi(-54);
        let distance =
            geo::Euclidean.distance(geo::Point::new(query_x, 0.0), geo::Point::new(row_x, 0.0));

        assert_eq!(distance, 1.0);
        assert!(row_x > 0.0f64.next_up());

        let ctx = SessionContext::new();
        crate::register(&ctx);
        let batches = ctx
            .sql(
                "SELECT \
                    ST_Distance(\
                        ST_GeomFromText('POINT(-1 0)'), \
                        ST_GeomFromText('POINT(0.000000000000000055511151231257827021181583404541015625 0)')\
                    ), \
                    ST_DWithin(\
                        ST_GeomFromText('POINT(-1 0)'), \
                        ST_GeomFromText('POINT(0.000000000000000055511151231257827021181583404541015625 0)'), \
                        1\
                    )",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let batch = &batches[0];
        assert_eq!(batch.column(0).as_primitive::<Float64Type>().value(0), 1.0);
        assert!(!batch.column(1).as_boolean().value(0));
    }

    #[derive(Debug)]
    struct OptimizedDWithinTable {
        schema: SchemaRef,
        batch: RecordBatch,
        scanned_ids: Arc<Mutex<Vec<i32>>>,
        extracted_filters: Arc<Mutex<Vec<DWithinIndexFilter>>>,
    }

    impl OptimizedDWithinTable {
        fn new(
            batch: RecordBatch,
            scanned_ids: Arc<Mutex<Vec<i32>>>,
            extracted_filters: Arc<Mutex<Vec<DWithinIndexFilter>>>,
        ) -> Self {
            Self {
                schema: batch.schema(),
                batch,
                scanned_ids,
                extracted_filters,
            }
        }

        fn candidate_mask(&self, filter: &DWithinIndexFilter) -> Result<BooleanArray> {
            let geometry_field = self.schema.field_with_name("geometry")?;
            let geometry = from_arrow_array(
                self.batch
                    .column_by_name("geometry")
                    .expect("test batch has geometry"),
                geometry_field,
            )
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let mut mask = BooleanBuilder::with_capacity(geometry.len());
            let [min_x, min_y, max_x, max_y] = filter.envelope;

            for index in 0..geometry.len() {
                if geometry.is_null(index) {
                    mask.append_value(false);
                    continue;
                }

                let bounds = total_bounds(geometry.slice(index, 1).as_ref())
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                let intersects = [bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()]
                    .iter()
                    .all(|value| value.is_finite())
                    && bounds.minx() <= max_x
                    && min_x <= bounds.maxx()
                    && bounds.miny() <= max_y
                    && min_y <= bounds.maxy();
                mask.append_value(intersects);
            }

            Ok(mask.finish())
        }
    }

    impl TableProvider for OptimizedDWithinTable {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        fn supports_filters_pushdown(
            &self,
            filters: &[&Expr],
        ) -> Result<Vec<TableProviderFilterPushDown>> {
            let mut support = Vec::with_capacity(filters.len());
            for filter in filters {
                support.push(if extract_dwithin_index_filter(filter)?.is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                });
            }
            Ok(support)
        }

        fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
            &'life0 self,
            state: &'life1 dyn Session,
            projection: Option<&'life2 Vec<usize>>,
            filters: &'life3 [Expr],
            limit: Option<usize>,
        ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async move {
                let mut batch = self.batch.clone();

                for filter in filters {
                    let Some(filter) = extract_dwithin_index_filter(filter)? else {
                        continue;
                    };
                    self.extracted_filters.lock().unwrap().push(filter.clone());
                    let mask = self.candidate_mask(&filter)?;
                    batch = filter_record_batch(&batch, &mask)?;
                }

                let ids = batch
                    .column_by_name("id")
                    .expect("test batch has id")
                    .as_primitive::<arrow_array::types::Int32Type>();
                self.scanned_ids
                    .lock()
                    .unwrap()
                    .extend((0..ids.len()).map(|index| ids.value(index)));

                let table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
                table.scan(state, projection, &[], limit).await
            })
        }
    }

    fn optimized_test_batch() -> RecordBatch {
        let geometry = WktArray::new(
            StringArray::from(vec![
                Some("POINT(0 0)"),
                Some("POINT(1 0)"),
                Some("POINT(0.9 0.9)"),
                Some("POINT(2 0)"),
                Some("LINESTRING(0.5 -0.5, 0.5 0.5)"),
                Some("LINESTRING(1.2 -0.5, 1.2 0.5)"),
                None,
                Some("POINT EMPTY"),
                Some("POINT(0.000000000000000055511151231257827021181583404541015625 0)"),
            ]),
            Arc::new(Metadata::default()),
        );
        let schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("id", DataType::Int32, false)),
            Arc::new(geometry.data_type().to_field("geometry", true)),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from((1..=9).collect::<Vec<_>>())),
                geometry.into_array_ref(),
            ],
        )
        .unwrap()
    }

    fn collect_ids(batches: &[RecordBatch]) -> Vec<i32> {
        let mut ids = Vec::new();
        for batch in batches {
            let column = batch
                .column(0)
                .as_primitive::<arrow_array::types::Int32Type>();
            ids.extend((0..column.len()).map(|index| column.value(index)));
        }
        ids
    }

    async fn run_provider_query(sql: &str) -> (Vec<i32>, Vec<i32>, Vec<DWithinIndexFilter>) {
        let scanned_ids = Arc::new(Mutex::new(Vec::new()));
        let extracted_filters = Arc::new(Mutex::new(Vec::new()));
        let table = Arc::new(OptimizedDWithinTable::new(
            optimized_test_batch(),
            scanned_ids.clone(),
            extracted_filters.clone(),
        ));
        let ctx = SessionContext::new();
        crate::register(&ctx);
        ctx.register_table("shapes", table).unwrap();

        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut scanned_ids = scanned_ids.lock().unwrap().clone();
        scanned_ids.sort_unstable();
        let extracted_filters = extracted_filters.lock().unwrap().clone();
        (collect_ids(&batches), scanned_ids, extracted_filters)
    }

    async fn run_full_scan_query(sql: &str) -> Vec<i32> {
        let ctx = SessionContext::new();
        crate::register(&ctx);
        ctx.register_batch("shapes", optimized_test_batch())
            .unwrap();
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        collect_ids(&batches)
    }

    #[tokio::test]
    async fn test_optimized_provider_matches_full_scan() {
        let sql = "SELECT id FROM shapes \
                   WHERE ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) \
                   ORDER BY id";
        let full_scan = run_full_scan_query(sql).await;
        let (indexed, scanned, filters) = run_provider_query(sql).await;

        assert_eq!(full_scan, vec![1, 2, 5, 9]);
        assert_eq!(indexed, full_scan);
        // Row 3 is an in-bounds false positive removed by the exact recheck.
        assert_eq!(scanned, vec![1, 2, 3, 5, 9]);
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0].column.name, "geometry");
    }

    #[tokio::test]
    async fn test_optimized_provider_boundary_counterexample() {
        let sql = "SELECT id FROM shapes \
                   WHERE ST_DWithin(geometry, ST_GeomFromText('POINT(-1 0)'), 1) \
                   ORDER BY id";
        let full_scan = run_full_scan_query(sql).await;
        let (indexed, scanned, _) = run_provider_query(sql).await;

        assert_eq!(full_scan, vec![1]);
        assert_eq!(indexed, full_scan);
        assert_eq!(scanned, vec![1]);
    }

    #[tokio::test]
    async fn test_optimized_provider_root_only_pushdown() {
        let all_ids = (1..=9).collect::<Vec<_>>();

        let sql_or = "SELECT id FROM shapes \
                      WHERE ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) OR id = 6 \
                      ORDER BY id";
        let full_scan_or = run_full_scan_query(sql_or).await;
        let (indexed_or, scanned_or, _) = run_provider_query(sql_or).await;
        assert_eq!(indexed_or, full_scan_or);
        assert_eq!(scanned_or, all_ids);

        let sql_not = "SELECT id FROM shapes \
                       WHERE NOT ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) \
                       ORDER BY id";
        let full_scan_not = run_full_scan_query(sql_not).await;
        let (indexed_not, scanned_not, _) = run_provider_query(sql_not).await;
        assert_eq!(indexed_not, full_scan_not);
        assert_eq!(scanned_not, all_ids);

        let sql_and = "SELECT id FROM shapes \
                       WHERE ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 1) \
                         AND id >= 2 \
                       ORDER BY id";
        let full_scan_and = run_full_scan_query(sql_and).await;
        let (indexed_and, scanned_and, _) = run_provider_query(sql_and).await;
        assert_eq!(full_scan_and, vec![2, 5, 9]);
        assert_eq!(indexed_and, full_scan_and);
        assert_eq!(scanned_and, vec![1, 2, 3, 5, 9]);
    }
}
