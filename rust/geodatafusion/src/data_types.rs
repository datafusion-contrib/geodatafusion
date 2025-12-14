use std::sync::LazyLock;

use arrow_schema::DataType;
use datafusion::logical_expr::{Signature, Volatility};
use geoarrow_schema::{
    BoxType, CoordType, Dimension, GeometryCollectionType, GeometryType, LineStringType,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};

fn any_geometry_type() -> Vec<DataType> {
    let expected_capacity = (2 * 4 * 7) + 2 + 4 + 3 + 3;
    let mut valid_types = Vec::with_capacity(expected_capacity);

    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            valid_types.push(
                PointType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                LineStringType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                PolygonType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                MultiPointType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                MultiLineStringType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                MultiPolygonType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
            valid_types.push(
                GeometryCollectionType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
        }
    }

    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        valid_types.push(
            GeometryType::new(Default::default())
                .with_coord_type(coord_type)
                .data_type(),
        );
    }

    for dim in [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ] {
        valid_types.push(BoxType::new(dim, Default::default()).data_type());
    }

    // Wkb
    valid_types.push(DataType::Binary);
    valid_types.push(DataType::LargeBinary);
    valid_types.push(DataType::BinaryView);

    // Wkt
    valid_types.push(DataType::Utf8);
    valid_types.push(DataType::LargeUtf8);
    valid_types.push(DataType::Utf8View);

    debug_assert_eq!(valid_types.len(), expected_capacity);

    valid_types
}

static ANY_SINGLE_GEOMETRY_TYPE_INPUT: LazyLock<Signature> =
    LazyLock::new(|| Signature::uniform(1, any_geometry_type(), Volatility::Immutable));

pub(crate) fn any_single_geometry_type_input() -> &'static Signature {
    &ANY_SINGLE_GEOMETRY_TYPE_INPUT
}

static ANY_POINT_TYPE: LazyLock<Vec<DataType>> = LazyLock::new(|| {
    let expected_capacity = 2 * 4;
    let mut valid_types = Vec::with_capacity(expected_capacity);

    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for dim in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            valid_types.push(
                PointType::new(dim, Default::default())
                    .with_coord_type(coord_type)
                    .data_type(),
            );
        }

        valid_types.push(
            GeometryType::new(Default::default())
                .with_coord_type(coord_type)
                .data_type(),
        );
    }

    debug_assert_eq!(valid_types.len(), expected_capacity);

    valid_types
});

pub(crate) fn any_point_type_input(arg_count: usize) -> Signature {
    Signature::uniform(arg_count, ANY_POINT_TYPE.clone(), Volatility::Immutable)
}
