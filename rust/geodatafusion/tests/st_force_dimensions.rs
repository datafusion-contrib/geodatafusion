use std::sync::Arc;

use arrow_array::{Array, BooleanArray, StringArray};
use arrow_schema::Field;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use geo_traits::{CoordTrait, LineStringTrait, PointTrait, PolygonTrait};
use geoarrow_array::array::{CoordBuffer, GeometryArray, PointArray, PolygonArray, RectArray};
use geoarrow_array::builder::{MultiPointBuilder, PointBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeometryType, Metadata, MultiPointType, PointType, PolygonType,
};
use geodatafusion::udf::native::editors::{Force2D, Force3D, Force3DM, Force3DZ, Force4D};
use wkt::types::{
    Coord, Dimension as WktDimension, MultiPoint as WktMultiPoint, Point as WktPoint,
};

#[test]
fn typed_points_preserve_crs_coord_type_and_apply_dimension_rules() {
    let crs = Crs::from_authority_code("EPSG:4326".to_string());
    for (input_dim, wkt_dim, coord_type) in [
        (Dimension::XY, WktDimension::XY, CoordType::Separated),
        (Dimension::XYZ, WktDimension::XYZ, CoordType::Interleaved),
        (Dimension::XYM, WktDimension::XYM, CoordType::Separated),
        (Dimension::XYZM, WktDimension::XYZM, CoordType::Interleaved),
    ] {
        let metadata = Arc::new(Metadata::new(crs.clone(), None));
        let point = sample_point(wkt_dim);
        let input = PointBuilder::from_points(
            std::slice::from_ref(&point).iter(),
            PointType::new(input_dim, Arc::clone(&metadata)).with_coord_type(coord_type),
        )
        .finish();

        let (field_2d, out_2d) = invoke_point_udf(&Force2D::new(coord_type), &input, &[]);
        let type_2d = field_2d.extension_type::<PointType>();
        assert_eq!(type_2d.dimension(), Dimension::XY);
        assert_eq!(type_2d.coord_type(), coord_type);
        assert_eq!(type_2d.metadata().crs(), &crs);
        assert_eq!(point_coords(&out_2d, 0), vec![1.0, 2.0]);

        let expected_z_default = match input_dim {
            Dimension::XYZ | Dimension::XYZM => 3.0,
            Dimension::XY | Dimension::XYM => 0.0,
        };
        let expected_z_custom = match input_dim {
            Dimension::XYZ | Dimension::XYZM => 3.0,
            Dimension::XY | Dimension::XYM => 9.0,
        };
        for (udf_z, scalar_args, expected_z) in [
            (&Force3DZ::new(coord_type), vec![], expected_z_default),
            (
                &Force3D::new(coord_type),
                vec![ScalarValue::Float64(Some(9.0))],
                expected_z_custom,
            ),
        ] {
            let (field_3dz, out_3dz) = invoke_point_udf(udf_z, &input, &scalar_args);
            let type_3dz = field_3dz.extension_type::<PointType>();
            assert_eq!(type_3dz.dimension(), Dimension::XYZ);
            assert_eq!(type_3dz.coord_type(), coord_type);
            assert_eq!(type_3dz.metadata().crs(), &crs);
            assert_eq!(point_coords(&out_3dz, 0), vec![1.0, 2.0, expected_z]);
        }

        let expected_m_default = match input_dim {
            Dimension::XYM | Dimension::XYZM => 4.0,
            Dimension::XY | Dimension::XYZ => 0.0,
        };
        let expected_m_custom = match input_dim {
            Dimension::XYM | Dimension::XYZM => 4.0,
            Dimension::XY | Dimension::XYZ => 11.0,
        };
        for (scalar_args, expected_m) in [
            (vec![], expected_m_default),
            (vec![ScalarValue::Float64(Some(11.0))], expected_m_custom),
        ] {
            let (field_3dm, out_3dm) =
                invoke_point_udf(&Force3DM::new(coord_type), &input, &scalar_args);
            let type_3dm = field_3dm.extension_type::<PointType>();
            assert_eq!(type_3dm.dimension(), Dimension::XYM);
            assert_eq!(type_3dm.coord_type(), coord_type);
            assert_eq!(type_3dm.metadata().crs(), &crs);
            assert_eq!(point_coords(&out_3dm, 0), vec![1.0, 2.0, expected_m]);
        }

        for (scalar_args, expected_z, expected_m) in [
            (vec![], expected_z_default, expected_m_default),
            (
                vec![ScalarValue::Float64(Some(9.0))],
                expected_z_custom,
                expected_m_default,
            ),
            (
                vec![
                    ScalarValue::Float64(Some(9.0)),
                    ScalarValue::Float64(Some(11.0)),
                ],
                expected_z_custom,
                expected_m_custom,
            ),
        ] {
            let (field_4d, out_4d) =
                invoke_point_udf(&Force4D::new(coord_type), &input, &scalar_args);
            let type_4d = field_4d.extension_type::<PointType>();
            assert_eq!(type_4d.dimension(), Dimension::XYZM);
            assert_eq!(type_4d.coord_type(), coord_type);
            assert_eq!(type_4d.metadata().crs(), &crs);
            assert_eq!(
                point_coords(&out_4d, 0),
                vec![1.0, 2.0, expected_z, expected_m]
            );
        }
    }
}

#[test]
fn non_nullable_geometry_with_nullable_parameter_produces_nullable_return_field() {
    let point = sample_point(WktDimension::XY);
    let input = PointBuilder::from_points(
        std::slice::from_ref(&point).iter(),
        PointType::new(Dimension::XY, Default::default()),
    )
    .finish();
    let geom_field = Arc::new(input.data_type().to_field("geometry", false));
    let z_field = Arc::new(Field::new("zvalue", arrow_schema::DataType::Float64, true));
    let udf = Force3DZ::default();

    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&geom_field), Arc::clone(&z_field)],
            scalar_arguments: &[None, None],
        })
        .unwrap();
    assert!(return_field.is_nullable());

    let args = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(input.to_array_ref()),
            ColumnarValue::Scalar(ScalarValue::Float64(None)),
        ],
        arg_fields: vec![geom_field, z_field],
        number_rows: 1,
        return_field: Arc::clone(&return_field),
        config_options: Arc::new(ConfigOptions::default()),
    };
    let ColumnarValue::Array(array) = udf.invoke_with_args(args).unwrap() else {
        panic!("expected array");
    };
    let out = PointArray::try_from((array.as_ref(), return_field.as_ref())).unwrap();
    assert!(out.is_null(0));
}

#[test]
fn rect_array_converts_to_closed_polygon_with_target_dimension_and_metadata() {
    let crs = Crs::from_authority_code("EPSG:3857".to_string());
    for (input_dim, wkt_dim, coord_type, expected_4d) in [
        (
            Dimension::XY,
            WktDimension::XY,
            CoordType::Separated,
            vec![
                vec![1.0, 2.0, 9.0, 11.0],
                vec![1.0, 6.0, 9.0, 11.0],
                vec![5.0, 6.0, 9.0, 11.0],
                vec![5.0, 2.0, 9.0, 11.0],
                vec![1.0, 2.0, 9.0, 11.0],
            ],
        ),
        (
            Dimension::XYZ,
            WktDimension::XYZ,
            CoordType::Interleaved,
            vec![
                vec![1.0, 2.0, 3.0, 11.0],
                vec![1.0, 6.0, 3.0, 11.0],
                vec![5.0, 6.0, 7.0, 11.0],
                vec![5.0, 2.0, 3.0, 11.0],
                vec![1.0, 2.0, 3.0, 11.0],
            ],
        ),
    ] {
        let metadata = Arc::new(Metadata::new(crs.clone(), None));
        let lower_point: WktPoint = WktPoint::new(
            Some(Coord {
                x: 1.0,
                y: 2.0,
                z: (input_dim == Dimension::XYZ).then_some(3.0),
                m: None,
            }),
            wkt_dim,
        );
        let upper_point: WktPoint = WktPoint::new(
            Some(Coord {
                x: 5.0,
                y: 6.0,
                z: (input_dim == Dimension::XYZ).then_some(7.0),
                m: None,
            }),
            wkt_dim,
        );
        let lower = PointBuilder::from_points(
            std::slice::from_ref(&lower_point).iter(),
            PointType::new(input_dim, Default::default()).with_coord_type(CoordType::Separated),
        )
        .finish();
        let upper = PointBuilder::from_points(
            std::slice::from_ref(&upper_point).iter(),
            PointType::new(input_dim, Default::default()).with_coord_type(CoordType::Separated),
        )
        .finish();
        let CoordBuffer::Separated(lower_coords) = lower.coords() else {
            unreachable!();
        };
        let CoordBuffer::Separated(upper_coords) = upper.coords() else {
            unreachable!();
        };
        let input = RectArray::new(
            lower_coords.clone(),
            upper_coords.clone(),
            None,
            Arc::clone(&metadata),
        );

        let (return_field, polygon_array) = invoke_rect_udf(
            &Force4D::new(coord_type),
            &input,
            &[
                ScalarValue::Float64(Some(9.0)),
                ScalarValue::Float64(Some(11.0)),
            ],
        );
        let poly_type = return_field.extension_type::<PolygonType>();
        assert_eq!(poly_type.dimension(), Dimension::XYZM);
        assert_eq!(poly_type.coord_type(), coord_type);
        assert_eq!(poly_type.metadata().crs(), &crs);

        let polygon = polygon_array.value(0).unwrap();
        let exterior = polygon.exterior().unwrap();
        let actual = (0..exterior.num_coords())
            .map(|idx| {
                let coord = exterior.coord(idx).unwrap();
                (0..Dimension::XYZM.size())
                    .map(|dim_idx| coord.nth_or_panic(dim_idx))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected_4d);
    }
}

#[test]
fn missing_extension_metadata_is_rejected() {
    let typ = MultiPointType::new(Dimension::XY, Default::default());
    let mut builder = MultiPointBuilder::new(typ);
    builder.push_multi_point(None::<&WktMultiPoint>).unwrap();
    let input = builder.finish();
    let input_array = input.to_array_ref();
    let bare_field = Arc::new(Field::new(
        "geometry",
        input_array.data_type().clone(),
        true,
    ));
    let udf = Force2D::default();

    let error = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&bare_field),
            scalar_arguments: &[],
        })
        .unwrap_err();
    assert!(error.to_string().contains("without extension metadata"));
}

#[tokio::test]
async fn sql_force_dimensions_cover_empty_points_collections_boxes_and_null_arrays() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);

    let batches = ctx
        .sql(
            r#"
            SELECT
                ST_AsText(ST_Force2D(ST_GeomFromText('POINT ZM EMPTY'))) AS empty_2d,
                ST_AsText(ST_Force3D(ST_GeomFromText('POINT EMPTY'), 5.0)) AS empty_3d,
                ST_AsText(ST_Force3DZ(ST_GeomFromText('POINT M EMPTY'), 5.0)) AS empty_3dz,
                ST_AsText(ST_Force3DM(ST_GeomFromText('POINT Z EMPTY'), 6.0)) AS empty_3dm,
                ST_AsText(ST_Force4D(ST_GeomFromText('POINT EMPTY'), 5.0, 6.0)) AS empty_4d,
                ST_AsText(ST_Force3DZ(ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2))'), 7.0)) AS single_collection,
                ST_AsText(ST_Force2D(ST_MakeBox2D(ST_Point(1, 2), ST_Point(3, 4)))) AS box2d_2d,
                ST_AsText(ST_Force3DZ(ST_3DMakeBox(ST_MakePoint(1, 2, 3), ST_MakePoint(4, 5, 6)))) AS box3d_3dz
            "#,
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = &batches[0];
    let value = |col: usize| {
        batch
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    assert_eq!(value(0), "POINT EMPTY");
    assert_eq!(value(1), "POINT Z EMPTY");
    assert_eq!(value(2), "POINT Z EMPTY");
    assert_eq!(value(3), "POINT M EMPTY");
    assert_eq!(value(4), "POINT ZM EMPTY");
    assert_eq!(value(5), "GEOMETRYCOLLECTION Z(POINT Z(1 2 7))");
    assert_eq!(value(6), "POLYGON((1 2,1 4,3 4,3 2,1 2))");
    assert_eq!(value(7), "POLYGON Z((1 2 3,1 5 3,4 5 6,4 2 3,1 2 3))");

    let null_batches = ctx
        .sql(
            r#"
            SELECT
                ST_Force3DZ(ST_Point(x, y), z) IS NULL AS is_null_row,
                ST_AsText(ST_Force3DZ(ST_Point(x, y), z)) AS wkt_row
            FROM (
                VALUES
                    (1.0, 2.0, 3.0),
                    (4.0, 5.0, NULL)
            ) AS t(x, y, z)
            "#,
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let null_batch = &null_batches[0];
    let is_null_col = null_batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    let wkt_col = null_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(!is_null_col.value(0));
    assert!(is_null_col.value(1));
    assert_eq!(wkt_col.value(0), "POINT Z(1 2 3)");
    assert!(wkt_col.is_null(1));

    let bare_null_batches = ctx
        .sql(
            r#"
            SELECT
                ST_Force2D(NULL) AS f2d,
                ST_Force3D(NULL) AS f3d,
                ST_Force3DZ(NULL, 1.0) AS f3dz,
                ST_Force3DM(NULL, 2.0) AS f3dm,
                ST_Force4D(NULL, 1.0, 2.0) AS f4d
            "#,
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let bare_null_batch = &bare_null_batches[0];
    let schema = bare_null_batch.schema();
    for name in ["f2d", "f3d", "f3dz", "f3dm", "f4d"] {
        let field = schema.field_with_name(name).unwrap();
        field.try_extension_type::<GeometryType>().unwrap();
        let geom_array = GeometryArray::try_from((
            bare_null_batch
                .column(schema.index_of(name).unwrap())
                .as_ref(),
            field,
        ))
        .unwrap();
        assert_eq!(geom_array.len(), 1);
        assert!(geom_array.is_null(0));
    }
}

fn invoke_point_udf<U: ScalarUDFImpl>(
    udf: &U,
    input: &PointArray,
    extra_scalars: &[ScalarValue],
) -> (Arc<Field>, PointArray) {
    let mut arg_fields = vec![Arc::new(input.data_type().to_field("geometry", true))];
    let mut scalar_arg_refs = vec![None];
    let mut args = vec![ColumnarValue::Array(input.to_array_ref())];
    for (idx, scalar) in extra_scalars.iter().enumerate() {
        arg_fields.push(Arc::new(Field::new(
            format!("arg_{idx}"),
            scalar.data_type(),
            scalar.is_null(),
        )));
        scalar_arg_refs.push(Some(scalar));
        args.push(ColumnarValue::Scalar(scalar.clone()));
    }
    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arg_refs,
        })
        .unwrap();
    let ColumnarValue::Array(array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: input.len(),
            return_field: Arc::clone(&return_field),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    else {
        panic!("expected an array result");
    };
    let output = PointArray::try_from((array.as_ref(), return_field.as_ref())).unwrap();
    (return_field, output)
}

fn invoke_rect_udf<U: ScalarUDFImpl>(
    udf: &U,
    input: &RectArray,
    extra_scalars: &[ScalarValue],
) -> (Arc<Field>, PolygonArray) {
    let mut arg_fields = vec![Arc::new(input.data_type().to_field("geometry", true))];
    let mut scalar_arg_refs = vec![None];
    let mut args = vec![ColumnarValue::Array(input.to_array_ref())];
    for (idx, scalar) in extra_scalars.iter().enumerate() {
        arg_fields.push(Arc::new(Field::new(
            format!("arg_{idx}"),
            scalar.data_type(),
            scalar.is_null(),
        )));
        scalar_arg_refs.push(Some(scalar));
        args.push(ColumnarValue::Scalar(scalar.clone()));
    }
    let return_field = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arg_refs,
        })
        .unwrap();
    let ColumnarValue::Array(array) = udf
        .invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: input.len(),
            return_field: Arc::clone(&return_field),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    else {
        panic!("expected an array result");
    };
    let output = PolygonArray::try_from((array.as_ref(), return_field.as_ref())).unwrap();
    (return_field, output)
}

fn point_coords(array: &PointArray, index: usize) -> Vec<f64> {
    let point = array.value(index).unwrap();
    let coord = point.coord().unwrap();
    (0..array.data_type().dimension().unwrap().size())
        .map(|dim_idx| coord.nth_or_panic(dim_idx))
        .collect()
}

fn sample_point(dimension: WktDimension) -> WktPoint {
    let (z, m) = match dimension {
        WktDimension::XY => (None, None),
        WktDimension::XYZ => (Some(3.0), None),
        WktDimension::XYM => (None, Some(4.0)),
        WktDimension::XYZM => (Some(3.0), Some(4.0)),
    };
    WktPoint::new(
        Some(Coord {
            x: 1.0,
            y: 2.0,
            z,
            m,
        }),
        dimension,
    )
}
