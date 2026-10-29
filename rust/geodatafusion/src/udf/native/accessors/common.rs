use std::sync::LazyLock;

use arrow_schema::{DataType, Field};
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use geo_traits::{CoordTrait, Dimensions};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

pub(super) fn geometry_and_index_signature() -> &'static Signature {
    static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
        let variants = any_geometry_type()
            .into_iter()
            .map(|geometry_type| TypeSignature::Exact(vec![geometry_type, DataType::Int64]))
            .collect();
        Signature::one_of(variants, Volatility::Immutable)
    });
    &SIGNATURE
}

pub(super) fn output_geometry_type(
    input_field: &Field,
    fallback_coord_type: CoordType,
) -> GeoDataFusionResult<GeometryType> {
    let input_type = GeoArrowType::from_arrow_field(input_field)?;
    let coord_type = input_type.coord_type().unwrap_or(fallback_coord_type);
    Ok(GeometryType::new(input_type.metadata().clone()).with_coord_type(coord_type))
}

pub(super) fn positive_index(index: i64, len: usize) -> Option<usize> {
    let zero_based = usize::try_from(index).ok()?.checked_sub(1)?;
    (zero_based < len).then_some(zero_based)
}

pub(super) fn signed_index(index: i64, len: usize) -> Option<usize> {
    if index > 0 {
        positive_index(index, len)
    } else if index < 0 {
        len.checked_sub(usize::try_from(index.unsigned_abs()).ok()?)
    } else {
        None
    }
}

pub(super) fn wkt_dimension(dim: Dimensions) -> wkt::types::Dimension {
    match dim {
        Dimensions::Xy | Dimensions::Unknown(2) => wkt::types::Dimension::XY,
        Dimensions::Xyz | Dimensions::Unknown(3) => wkt::types::Dimension::XYZ,
        Dimensions::Xym => wkt::types::Dimension::XYM,
        Dimensions::Xyzm | Dimensions::Unknown(4) => wkt::types::Dimension::XYZM,
        Dimensions::Unknown(_) => wkt::types::Dimension::XY,
    }
}

pub(super) fn coord_to_wkt(coord: &impl CoordTrait<T = f64>) -> wkt::types::Coord {
    wkt::types::Coord {
        x: coord.x(),
        y: coord.y(),
        z: coord.nth(2),
        m: coord.nth(3),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_semantics() {
        assert_eq!(positive_index(1, 3), Some(0));
        assert_eq!(positive_index(3, 3), Some(2));
        assert_eq!(positive_index(4, 3), None);
        assert_eq!(positive_index(0, 3), None);
        assert_eq!(positive_index(-1, 3), None);

        assert_eq!(signed_index(1, 3), Some(0));
        assert_eq!(signed_index(-1, 3), Some(2));
        assert_eq!(signed_index(-3, 3), Some(0));
        assert_eq!(signed_index(-4, 3), None);
        assert_eq!(signed_index(0, 3), None);
        assert_eq!(signed_index(i64::MIN, 3), None);
    }
}
