//! Native-Rust length, perimeter, azimuth, and angle UDFs.

mod angle;
mod length;
mod measure;
mod perimeter;

pub use angle::{Angle, Azimuth};
pub use length::Length3D;
pub(crate) use measure::{Axis, is_null_array, linear_length, null_float64};
pub use perimeter::{Perimeter, Perimeter3D};

/// Register all native measurement UDFs.
pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(Angle.into());
    session_context.register_udf(Azimuth.into());
    session_context.register_udf(Length3D.into());
    session_context.register_udf(Perimeter.into());
    session_context.register_udf(Perimeter3D.into());
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::types::Float64Type;
    use arrow_array::{Array, RecordBatch};
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geoarrow_array::builder::PointBuilder;
    use geoarrow_array::{GeoArrowArray, IntoArrow};
    use geoarrow_schema::{CoordType, Crs, Dimension, Metadata, PointType};

    async fn scalar(context: &SessionContext, sql: &str) -> Option<f64> {
        let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
        let column = batches[0].column(0).as_primitive::<Float64Type>();
        (!column.is_null(0)).then(|| column.value(0))
    }

    fn context() -> SessionContext {
        let context = SessionContext::new();
        crate::register(&context);
        context
    }

    #[tokio::test]
    async fn test_native_measurement_contracts() {
        let context = context();

        assert_eq!(
            scalar(
                &context,
                "SELECT ST_3DLength(ST_GeomFromText('LINESTRING Z (0 0 0, 3 4 12)'))"
            )
            .await,
            Some(13.0)
        );
        assert_eq!(
            scalar(
                &context,
                "SELECT ST_3DLength(ST_GeomFromText('LINESTRING M (0 0 0, 3 4 12)'))"
            )
            .await,
            Some(5.0)
        );
        assert_eq!(
            scalar(
                &context,
                "SELECT ST_3DLength(ST_GeomFromText('POLYGON Z ((0 0 0, 3 0 0, 3 4 12, 0 0 0))'))"
            )
            .await,
            Some(0.0)
        );

        assert_eq!(
            scalar(
                &context,
                "SELECT ST_Perimeter(ST_GeomFromText('POLYGON((0 0, 3 0, 3 4, 0 0))'))"
            )
            .await,
            Some(12.0)
        );
        assert_eq!(
            scalar(
                &context,
                "SELECT ST_Perimeter2D(ST_GeomFromText('POLYGON Z ((0 0 0, 3 0 0, 3 4 12, 0 0 0))'))"
            )
            .await,
            Some(12.0)
        );
        assert_eq!(
            scalar(
                &context,
                "SELECT ST_3DPerimeter(ST_GeomFromText('POLYGON Z ((0 0 0, 3 0 0, 3 4 12, 0 0 0))'))"
            )
            .await,
            Some(28.64911064067352)
        );

        assert_eq!(
            scalar(
                &context,
                "SELECT ST_Length(ST_GeomFromText('POLYGON((0 0, 3 0, 3 4, 0 0))'))"
            )
            .await,
            Some(0.0)
        );
        assert_eq!(
            scalar(
                &context,
                "SELECT ST_Length(ST_GeomFromText('GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 0)),LINESTRING(0 0,3 4))'))"
            )
            .await,
            Some(5.0)
        );

        assert_eq!(
            scalar(
                &context,
                "SELECT ST_Azimuth(ST_Point(0, 0), ST_Point(1, -1))"
            )
            .await,
            Some(2.356194490192344)
        );
        assert_eq!(
            scalar(
                &context,
                "SELECT ST_Azimuth(ST_Point(0, 0), ST_Point(0, 0))"
            )
            .await,
            None
        );

        assert_eq!(
            scalar(
                &context,
                "SELECT ST_Angle(ST_Point(1, 0), ST_Point(0, 0), ST_Point(0, 1))"
            )
            .await,
            Some(4.71238898038469)
        );
        assert_eq!(
            scalar(
                &context,
                "SELECT ST_Angle(ST_GeomFromText('LINESTRING(0 0, 0 10, 10 0)'), ST_GeomFromText('LINESTRING(0 0, 0 1)'))"
            )
            .await,
            Some(4.71238898038469)
        );
        assert_eq!(
            scalar(
                &context,
                "SELECT ST_Angle(ST_GeomFromText('MULTILINESTRING((0 0, 1 0))'), ST_GeomFromText('LINESTRING(0 0, 0 1)'))"
            )
            .await,
            None
        );

        let nan = scalar(
            &context,
            "SELECT ST_Angle(ST_GeomFromWKB(decode('0101000000000000000000f87f0000000000000000', 'hex')), ST_Point(0, 0), ST_Point(0, 1))",
        )
        .await
        .unwrap();
        assert!(nan.is_nan());
    }

    #[tokio::test]
    async fn test_angle_errors_match_point_requirements() {
        let context = context();

        let error = context
            .sql(
                "SELECT ST_Angle(ST_GeomFromText('LINESTRING(0 0, 1 1)'), ST_Point(0, 0), ST_Point(0, 1))",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Argument must be POINT geometries"),
            "{error}"
        );

        let error = context
            .sql("SELECT ST_Angle(ST_GeomFromText('POINT EMPTY'), ST_Point(0, 0), ST_Point(0, 1))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("Empty geometry"), "{error}");

        let error = context
            .sql("SELECT ST_Azimuth(ST_GeomFromText('LINESTRING(0 0, 1 1)'), ST_Point(0, 0))")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Argument must be POINT geometries"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_literal_null_and_crs_boundaries() {
        let context = context();

        let literals = context
            .sql(
                "SELECT \
                    ST_Length(NULL) IS NULL, \
                    ST_Length2D(NULL) IS NULL, \
                    ST_3DLength(NULL) IS NULL, \
                    ST_Perimeter(NULL) IS NULL, \
                    ST_Perimeter2D(NULL) IS NULL, \
                    ST_3DPerimeter(NULL) IS NULL, \
                    ST_Azimuth(NULL, NULL) IS NULL, \
                    ST_Angle(NULL, NULL, NULL, NULL) IS NULL",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(
            (0..literals[0].num_columns())
                .all(|index| literals[0].column(index).as_boolean().value(0))
        );

        let known = point_array(4326);
        let unknown = point_array(0);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                known.extension_type().to_field("known", true),
                unknown.extension_type().to_field("unknown", true),
            ])),
            vec![known.into_array_ref(), unknown.into_array_ref()],
        )
        .unwrap();
        context.register_batch("measurement_points", batch).unwrap();

        let nulls_with_known = context
            .sql(
                "SELECT \
                    ST_Azimuth(NULL, known) IS NULL, \
                    ST_Angle(NULL, known, known) IS NULL \
                 FROM measurement_points",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        for batch in &nulls_with_known {
            for column in 0..batch.num_columns() {
                assert!(
                    (0..batch.num_rows())
                        .all(|row| { batch.column(column).as_boolean().value(row) })
                );
            }
        }

        let error = match context
            .sql("SELECT ST_Azimuth(unknown, known) FROM measurement_points")
            .await
        {
            Ok(dataframe) => dataframe.collect().await.unwrap_err().to_string(),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("mixed SRID"), "{error}");
    }

    fn point_array(srid: i32) -> geoarrow_array::array::PointArray {
        let metadata = if srid == 0 {
            Arc::new(Metadata::default())
        } else {
            Arc::new(Metadata::new(
                Crs::from_authority_code(format!("EPSG:{srid}")),
                None,
            ))
        };
        let typ = PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
        let points = [geo::Point::new(1.0, 2.0), geo::Point::new(1.0, 2.0)];
        PointBuilder::from_points(points.iter(), typ).finish()
    }
}
