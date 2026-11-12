use std::sync::LazyLock;

use arrow_array::new_null_array;
use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use geoarrow_schema::{
    BoxType, CoordType, Dimension, GeometryCollectionType, GeometryType, LineStringType,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};

pub(crate) fn any_geometry_type() -> Vec<DataType> {
    let expected_capacity = (2 * 4 * 7) + 2 + 4 + 3 + 3 + 1;
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

    // Bare SQL NULL.
    valid_types.push(DataType::Null);

    debug_assert_eq!(valid_types.len(), expected_capacity);

    valid_types
}

static ANY_GEOMETRY_TYPES: LazyLock<Vec<DataType>> = LazyLock::new(any_geometry_type);

static ANY_SINGLE_GEOMETRY_TYPE_INPUT: LazyLock<Signature> =
    LazyLock::new(|| Signature::uniform(1, ANY_GEOMETRY_TYPES.clone(), Volatility::Immutable));

pub(crate) fn any_single_geometry_type_input() -> &'static Signature {
    &ANY_SINGLE_GEOMETRY_TYPE_INPUT
}

/// Return a typed all-NULL result when the first UDF argument is a bare SQL NULL.
pub(crate) fn null_input_result(
    args: &ScalarFunctionArgs,
) -> datafusion::error::Result<Option<ColumnarValue>> {
    let is_null = args
        .arg_fields
        .iter()
        .any(|field| field.data_type() == &DataType::Null)
        || args
            .args
            .iter()
            .any(|arg| matches!(arg, ColumnarValue::Scalar(ScalarValue::Null)));
    if !is_null {
        return Ok(None);
    }
    Ok(Some(ColumnarValue::Array(new_null_array(
        args.return_field.data_type(),
        args.number_rows,
    ))))
}
