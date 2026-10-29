use std::sync::Arc;

use arrow_array::{Array, BooleanArray, StringArray};
use arrow_schema::Field;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use geo_traits::{CoordTrait, LineStringTrait, MultiPolygonTrait, PointTrait, PolygonTrait};
use geoarrow_array::array::{
    CoordBuffer, GeometryArray, GeometryCollectionArray, MultiPolygonArray, PointArray,
    PolygonArray, RectArray,
};
use geoarrow_array::builder::{MultiPointBuilder, PointBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{
    CoordType, Crs, Dimension, GeometryCollectionType, GeometryType, Metadata, MultiPointType,
    MultiPolygonType, PointType, PolygonType,
};
use geodatafusion::udf::native::editors::{Force2D, Force3D, Force3DM, Force3DZ, Force4D};
use geodatafusion::udf::native::io::{AsText, GeomFromText};
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
    let metadata = Arc::new(Metadata::new(crs.clone(), None));

    // 2D Rect -> 1-polygon PolygonArray
    let lower_2d = WktPoint::new(
        Some(Coord {
            x: 1.0,
            y: 2.0,
            z: None,
            m: None,
        }),
        WktDimension::XY,
    );
    let upper_2d = WktPoint::new(
        Some(Coord {
            x: 5.0,
            y: 6.0,
            z: None,
            m: None,
        }),
        WktDimension::XY,
    );
    let lower_arr_2d = PointBuilder::from_points(
        std::slice::from_ref(&lower_2d).iter(),
        PointType::new(Dimension::XY, Default::default()).with_coord_type(CoordType::Separated),
    )
    .finish();
    let upper_arr_2d = PointBuilder::from_points(
        std::slice::from_ref(&upper_2d).iter(),
        PointType::new(Dimension::XY, Default::default()).with_coord_type(CoordType::Separated),
    )
    .finish();
    let CoordBuffer::Separated(lower_coords_2d) = lower_arr_2d.coords() else {
        unreachable!();
    };
    let CoordBuffer::Separated(upper_coords_2d) = upper_arr_2d.coords() else {
        unreachable!();
    };
    let input_2d = RectArray::new(
        lower_coords_2d.clone(),
        upper_coords_2d.clone(),
        None,
        Arc::clone(&metadata),
    );

    let (return_field_2d, polygon_array_2d) = invoke_rect_2d_udf(
        &Force4D::new(CoordType::Separated),
        &input_2d,
        &[
            ScalarValue::Float64(Some(9.0)),
            ScalarValue::Float64(Some(11.0)),
        ],
    );
    let poly_type_2d = return_field_2d.extension_type::<PolygonType>();
    assert_eq!(poly_type_2d.dimension(), Dimension::XYZM);
    assert_eq!(poly_type_2d.coord_type(), CoordType::Separated);
    assert_eq!(poly_type_2d.metadata().crs(), &crs);
    let polygon_2d = polygon_array_2d.value(0).unwrap();
    let exterior_2d = polygon_2d.exterior().unwrap();
    let actual_2d = (0..exterior_2d.num_coords())
        .map(|idx| {
            let coord = exterior_2d.coord(idx).unwrap();
            (0..Dimension::XYZM.size())
                .map(|dim_idx| coord.nth_or_panic(dim_idx))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual_2d,
        vec![
            vec![1.0, 2.0, 9.0, 11.0],
            vec![1.0, 6.0, 9.0, 11.0],
            vec![5.0, 6.0, 9.0, 11.0],
            vec![5.0, 2.0, 9.0, 11.0],
            vec![1.0, 2.0, 9.0, 11.0],
        ]
    );

    // 3D Rect -> 6-face MultiPolygonArray tagged as PolyhedralSurface
    let lower_3d = WktPoint::new(
        Some(Coord {
            x: 1.0,
            y: 2.0,
            z: Some(3.0),
            m: None,
        }),
        WktDimension::XYZ,
    );
    let upper_3d = WktPoint::new(
        Some(Coord {
            x: 5.0,
            y: 6.0,
            z: Some(7.0),
            m: None,
        }),
        WktDimension::XYZ,
    );
    let lower_arr_3d = PointBuilder::from_points(
        std::slice::from_ref(&lower_3d).iter(),
        PointType::new(Dimension::XYZ, Default::default()).with_coord_type(CoordType::Separated),
    )
    .finish();
    let upper_arr_3d = PointBuilder::from_points(
        std::slice::from_ref(&upper_3d).iter(),
        PointType::new(Dimension::XYZ, Default::default()).with_coord_type(CoordType::Separated),
    )
    .finish();
    let CoordBuffer::Separated(lower_coords_3d) = lower_arr_3d.coords() else {
        unreachable!();
    };
    let CoordBuffer::Separated(upper_coords_3d) = upper_arr_3d.coords() else {
        unreachable!();
    };
    let input_3d = RectArray::new(
        lower_coords_3d.clone(),
        upper_coords_3d.clone(),
        None,
        Arc::clone(&metadata),
    );

    let (return_field_3d, mpoly_array_3d) = invoke_rect_3d_udf(
        &Force4D::new(CoordType::Interleaved),
        &input_3d,
        &[
            ScalarValue::Float64(Some(9.0)),
            ScalarValue::Float64(Some(11.0)),
        ],
    );
    let mpoly_type_3d = return_field_3d.extension_type::<MultiPolygonType>();
    assert_eq!(mpoly_type_3d.dimension(), Dimension::XYZM);
    assert_eq!(mpoly_type_3d.coord_type(), CoordType::Interleaved);
    assert_eq!(mpoly_type_3d.metadata().crs(), &crs);
    assert_eq!(
        return_field_3d
            .metadata()
            .get("geodatafusion:geometry_subtype")
            .map(String::as_str),
        Some("polyhedralsurface")
    );
    let mpoly_3d = mpoly_array_3d.value(0).unwrap();
    assert_eq!(mpoly_3d.num_polygons(), 6);
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
    assert_eq!(
        value(7),
        "POLYHEDRALSURFACE Z(((1 2 3,1 5 3,4 5 3,4 2 3,1 2 3)),((1 2 6,4 2 6,4 5 6,1 5 6,1 2 6)),((1 2 3,1 2 6,1 5 6,1 5 3,1 2 3)),((4 2 3,4 5 3,4 5 6,4 2 6,4 2 3)),((1 2 3,4 2 3,4 2 6,1 2 6,1 2 3)),((1 5 3,1 5 6,4 5 6,4 5 3,1 5 3)))"
    );

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

fn invoke_rect_2d_udf<U: ScalarUDFImpl>(
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

fn invoke_rect_3d_udf<U: ScalarUDFImpl>(
    udf: &U,
    input: &RectArray,
    extra_scalars: &[ScalarValue],
) -> (Arc<Field>, MultiPolygonArray) {
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
    let output = MultiPolygonArray::try_from((array.as_ref(), return_field.as_ref())).unwrap();
    (return_field, output)
}

#[tokio::test]
async fn force_preserves_box3d_as_polyhedral_surface() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batches = ctx
        .sql(
            "SELECT \
                ST_AsText(ST_Force2D(ST_3DMakeBox(ST_MakePoint(0, 1, 2), ST_MakePoint(3, 4, 5)))) AS f2d, \
                ST_AsText(ST_Force3D(ST_3DMakeBox(ST_MakePoint(0, 1, 2), ST_MakePoint(3, 4, 5)), 9)) AS f3d, \
                ST_AsText(ST_Force3DZ(ST_3DMakeBox(ST_MakePoint(0, 1, 2), ST_MakePoint(3, 4, 5)), 9)) AS f3dz, \
                ST_AsText(ST_Force3DM(ST_3DMakeBox(ST_MakePoint(0, 1, 2), ST_MakePoint(3, 4, 5)), 8)) AS f3dm, \
                ST_AsText(ST_Force4D(ST_3DMakeBox(ST_MakePoint(0, 1, 2), ST_MakePoint(3, 4, 5)), 7, 8)) AS f4d",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let col = |idx: usize| {
        batches[0]
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    assert_eq!(
        col(0),
        "POLYHEDRALSURFACE(((0 1,0 4,3 4,3 1,0 1)),((0 1,3 1,3 4,0 4,0 1)),((0 1,0 1,0 4,0 4,0 1)),((3 1,3 4,3 4,3 1,3 1)),((0 1,3 1,3 1,0 1,0 1)),((0 4,0 4,3 4,3 4,0 4)))"
    );
    assert_eq!(
        col(1),
        "POLYHEDRALSURFACE Z(((0 1 2,0 4 2,3 4 2,3 1 2,0 1 2)),((0 1 5,3 1 5,3 4 5,0 4 5,0 1 5)),((0 1 2,0 1 5,0 4 5,0 4 2,0 1 2)),((3 1 2,3 4 2,3 4 5,3 1 5,3 1 2)),((0 1 2,3 1 2,3 1 5,0 1 5,0 1 2)),((0 4 2,0 4 5,3 4 5,3 4 2,0 4 2)))"
    );
    assert_eq!(col(2), col(1));
    assert_eq!(
        col(3),
        "POLYHEDRALSURFACE M(((0 1 8,0 4 8,3 4 8,3 1 8,0 1 8)),((0 1 8,3 1 8,3 4 8,0 4 8,0 1 8)),((0 1 8,0 1 8,0 4 8,0 4 8,0 1 8)),((3 1 8,3 4 8,3 4 8,3 1 8,3 1 8)),((0 1 8,3 1 8,3 1 8,0 1 8,0 1 8)),((0 4 8,0 4 8,3 4 8,3 4 8,0 4 8)))"
    );
    assert_eq!(
        col(4),
        "POLYHEDRALSURFACE ZM(((0 1 2 8,0 4 2 8,3 4 2 8,3 1 2 8,0 1 2 8)),((0 1 5 8,3 1 5 8,3 4 5 8,0 4 5 8,0 1 5 8)),((0 1 2 8,0 1 5 8,0 4 5 8,0 4 2 8,0 1 2 8)),((3 1 2 8,3 4 2 8,3 4 5 8,3 1 5 8,3 1 2 8)),((0 1 2 8,3 1 2 8,3 1 5 8,0 1 5 8,0 1 2 8)),((0 4 2 8,0 4 5 8,3 4 5 8,3 4 2 8,0 4 2 8)))"
    );
}

#[tokio::test]
async fn force_preserves_nested_collections() {
    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    let batches = ctx
        .sql(
            "SELECT \
                ST_AsText(ST_Force4D(ST_GeomFromText('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2)))'), 3, 4)) AS single_nested, \
                ST_AsText(ST_Force4D(ST_GeomFromText('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2),POINT(3 4)))'), 5, 6)) AS multi_nested, \
                ST_AsText(ST_Force2D(ST_GeomFromText('GEOMETRYCOLLECTION Z(GEOMETRYCOLLECTION Z(POINT Z(1 2 3),LINESTRING Z(0 0 1,3 4 2)),POINT Z(5 6 7))'))) AS mixed_2d, \
                ST_AsText(ST_Force3DZ(ST_GeomFromText('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY,POINT(1 2))'), 9)) AS empty_inner_3dz, \
                ST_AsText(ST_Force3DM(ST_GeomFromWKB(ST_AsBinary(ST_GeomFromText('GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2),POINT(3 4)))'))), 8)) AS wkb_roundtrip_3dm",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let col = |idx: usize| {
        batches[0]
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    };
    assert_eq!(
        col(0),
        "GEOMETRYCOLLECTION ZM(GEOMETRYCOLLECTION ZM(POINT ZM(1 2 3 4)))"
    );
    assert_eq!(
        col(1),
        "GEOMETRYCOLLECTION ZM(GEOMETRYCOLLECTION ZM(POINT ZM(1 2 5 6),POINT ZM(3 4 5 6)))"
    );
    assert_eq!(
        col(2),
        "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,3 4)),POINT(5 6))"
    );
    assert_eq!(
        col(3),
        "GEOMETRYCOLLECTION Z(GEOMETRYCOLLECTION Z EMPTY,POINT Z(1 2 9))"
    );
    assert_eq!(
        col(4),
        "GEOMETRYCOLLECTION M(GEOMETRYCOLLECTION M(POINT M(1 2 8),POINT M(3 4 8)))"
    );

    // Also verify typed GeometryCollectionArray input preserves nested collections
    let wkt_input = StringArray::from(vec![
        Some("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2)))"),
        Some("GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(1 2),POINT(3 4)),POINT(5 6))"),
    ]);
    let wkt_field = Arc::new(Field::new("wkt", arrow_schema::DataType::Utf8, true));
    let geom_from_text = GeomFromText::new(CoordType::Separated);
    let geom_return_field = geom_from_text
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&wkt_field),
            scalar_arguments: &[None],
        })
        .unwrap();
    let ColumnarValue::Array(geom_raw) = geom_from_text
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(wkt_input))],
            arg_fields: vec![wkt_field],
            number_rows: 2,
            return_field: Arc::clone(&geom_return_field),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    else {
        panic!("expected array");
    };
    let geom_arr =
        GeometryArray::try_from((geom_raw.as_ref(), geom_return_field.as_ref())).unwrap();
    let typed_gc: GeometryCollectionArray =
        geom_arr.geometry_collection_child(Dimension::XY).clone();
    let typed_gc_field = Arc::new(typed_gc.data_type().to_field("gc", true));
    let force_4d = Force4D::new(CoordType::Separated);
    let z_scalar = ScalarValue::Float64(Some(3.0));
    let m_scalar = ScalarValue::Float64(Some(4.0));
    let z_field = Arc::new(Field::new("z", arrow_schema::DataType::Float64, false));
    let m_field = Arc::new(Field::new("m", arrow_schema::DataType::Float64, false));
    let forced_gc_field = force_4d
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &[
                Arc::clone(&typed_gc_field),
                Arc::clone(&z_field),
                Arc::clone(&m_field),
            ],
            scalar_arguments: &[None, Some(&z_scalar), Some(&m_scalar)],
        })
        .unwrap();
    let forced_gc_type = forced_gc_field.extension_type::<GeometryCollectionType>();
    assert_eq!(forced_gc_type.dimension(), Dimension::XYZM);
    let ColumnarValue::Array(forced_gc_raw) = force_4d
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(typed_gc.to_array_ref()),
                ColumnarValue::Scalar(z_scalar),
                ColumnarValue::Scalar(m_scalar),
            ],
            arg_fields: vec![typed_gc_field, z_field, m_field],
            number_rows: 2,
            return_field: Arc::clone(&forced_gc_field),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    else {
        panic!("expected array");
    };
    let as_text = AsText::new();
    let as_text_field = as_text
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: std::slice::from_ref(&forced_gc_field),
            scalar_arguments: &[None],
        })
        .unwrap();
    let ColumnarValue::Array(wkt_out_raw) = as_text
        .invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(forced_gc_raw)],
            arg_fields: vec![forced_gc_field],
            number_rows: 2,
            return_field: as_text_field,
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap()
    else {
        panic!("expected array");
    };
    let wkt_out = wkt_out_raw.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        wkt_out.value(0),
        "GEOMETRYCOLLECTION ZM(GEOMETRYCOLLECTION ZM(POINT ZM(1 2 3 4)))"
    );
    assert_eq!(
        wkt_out.value(1),
        "GEOMETRYCOLLECTION ZM(GEOMETRYCOLLECTION ZM(POINT ZM(1 2 3 4),POINT ZM(3 4 3 4)),POINT ZM(5 6 3 4))"
    );
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
