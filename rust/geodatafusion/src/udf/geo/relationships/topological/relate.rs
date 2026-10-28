use std::sync::{Arc, OnceLock};

use arrow_array::BooleanArray;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo::relate::IntersectionMatrix;
use geo::{
    CoordNum, CoordsIter, Geometry, GeometryCollection, MultiPoint, PreparedGeometry, Relate,
};
use geo_traits::to_geo::ToGeoPoint;
use geo_traits::{GeometryCollectionTrait, GeometryTrait, MultiPointTrait, PointTrait};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};

use crate::error::GeoDataFusionResult;

macro_rules! impl_relate_udf {
    ($struct_name:ident, $udf_name:expr, $documentation_name:ident, $callback:expr, $doc_text:expr, $doc_example:expr) => {
        #[derive(Debug, Eq, PartialEq, Hash)]
        pub struct $struct_name {
            signature: Signature,
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    signature: Signature::any(2, Volatility::Immutable),
                }
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self::new()
            }
        }

        static $documentation_name: OnceLock<Documentation> = OnceLock::new();

        impl ScalarUDFImpl for $struct_name {
            fn name(&self) -> &str {
                $udf_name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Boolean)
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let mut arrays = args.args.into_iter();
                Ok(relate_impl(
                    arrays.next().unwrap(),
                    &args.arg_fields[0],
                    arrays.next().unwrap(),
                    &args.arg_fields[1],
                    $callback,
                )?)
            }

            fn documentation(&self) -> Option<&Documentation> {
                Some($documentation_name.get_or_init(|| {
                    Documentation::builder(DOC_SECTION_OTHER, $doc_text, $doc_example)
                        .with_argument("geomA", "geometry")
                        .with_argument("geomB", "geometry")
                        .build()
                }))
            }
        }
    };
}

impl_relate_udf!(
    Intersects,
    "st_intersects",
    INTERSECTS_DOC,
    |matrix| matrix.is_intersects(),
    "Returns true if two geometries intersect. Geometries intersect if they have any point in common.",
    "ST_Intersects(geomA, geomB)"
);
impl_relate_udf!(
    Disjoint,
    "st_disjoint",
    DISJOINT_DOC,
    |matrix| matrix.is_disjoint(),
    "Returns true if two geometries are disjoint. Geometries are disjoint if they have no point in common.",
    "ST_Disjoint(geomA, geomB)"
);
impl_relate_udf!(
    Within,
    "st_within",
    WITHIN_DOC,
    |matrix| matrix.is_within(),
    "Returns TRUE if geometry A is within geometry B. A is within B if and only if all points of A lie inside (i.e. in the interior or boundary of) B (or equivalently, no points of A lie in the exterior of B), and the interiors of A and B have at least one point in common.",
    "ST_Within(geomA, geomB)"
);
impl_relate_udf!(
    Contains,
    "st_contains",
    CONTAINS_DOC,
    |matrix| matrix.is_contains(),
    "Returns TRUE if geometry A contains geometry B. A contains B if and only if all points of B lie inside (i.e. in the interior or boundary of) A (or equivalently, no points of B lie in the exterior of A), and the interiors of A and B have at least one point in common.",
    "ST_Contains(geomA, geomB)"
);
impl_relate_udf!(
    Equals,
    "st_equals",
    EQUALS_DOC,
    |matrix| matrix.is_equal_topo(),
    "Returns true if the given geometries are \"topologically equal\". Use this for a 'better' answer than '='. Topological equality means that the geometries have the same dimension, and their point-sets occupy the same space. This means that the order of vertices may be different in topologically equal geometries.",
    "ST_Equals(geomA, geomB)"
);
impl_relate_udf!(
    CoveredBy,
    "st_coveredby",
    COVERED_BY_DOC,
    |matrix| matrix.is_coveredby(),
    "Returns true if every point in Geometry/Geography A lies inside (i.e. intersects the interior or boundary of) Geometry/Geography B. Equivalently, tests that no point of A lies outside (in the exterior of) B.",
    "ST_CoveredBy(geomA, geomB)"
);
impl_relate_udf!(
    Covers,
    "st_covers",
    COVERS_DOC,
    |matrix| matrix.is_covers(),
    "Returns true if every point in Geometry/Geography B lies inside (i.e. intersects the interior or boundary of) Geometry/Geography A. Equivalently, tests that no point of B lies outside (in the exterior of) A.",
    "ST_Covers(geomA, geomB)"
);
impl_relate_udf!(
    Touches,
    "st_touches",
    TOUCHES_DOC,
    |matrix| matrix.is_touches(),
    "Returns TRUE if A and B intersect, but their interiors do not intersect. Equivalently, A and B have at least one point in common, and the common points lie in at least one boundary. For Point/Point inputs the relationship is always FALSE, since points do not have a boundary.",
    "ST_Touches(geomA, geomB)"
);

impl_relate_udf!(
    Crosses,
    "st_crosses",
    CROSSES_DOC,
    |matrix| matrix.is_crosses(),
    "Compares two geometry objects and returns true if their intersection \"spatially crosses\"; that is, the geometries have some, but not all interior points in common. The intersection of the interiors of the geometries must be non-empty and must have dimension less than the maximum dimension of the two input geometries, and the intersection of the two geometries must not equal either geometry. Otherwise, it returns false. The crosses relation is symmetric and irreflexive.",
    "ST_Crosses(geomA, geomB)"
);
impl_relate_udf!(
    Overlaps,
    "st_overlaps",
    OVERLAPS_DOC,
    |matrix| matrix.is_overlaps(),
    "Returns TRUE if geometry A and B \"spatially overlap\". Two geometries overlap if they have the same dimension, their interiors intersect in that dimension. and each has at least one point inside the other (or equivalently, neither one covers the other). The overlaps relation is symmetric and irreflexive.",
    "ST_Overlaps(geomA, geomB)"
);

fn relate_impl(
    left: ColumnarValue,
    left_field: &Field,
    right: ColumnarValue,
    right_field: &Field,
    relate_cb: impl Fn(IntersectionMatrix) -> bool,
) -> GeoDataFusionResult<ColumnarValue> {
    match (left, right) {
        (ColumnarValue::Scalar(left_scalar), ColumnarValue::Scalar(right_scalar)) => {
            let mut arrays =
                ColumnarValue::values_to_arrays(&[left_scalar.into(), right_scalar.into()])?
                    .into_iter();
            let left_array = ColumnarValue::Array(arrays.next().unwrap());
            let right_array = ColumnarValue::Array(arrays.next().unwrap());
            relate_impl(left_array, left_field, right_array, right_field, relate_cb)
        }
        (ColumnarValue::Array(left_arr), ColumnarValue::Array(right_arr)) => {
            let left_arr = from_arrow_array(&left_arr, left_field)?;
            let right_arr = from_arrow_array(&right_arr, right_field)?;
            let result = relate_arrays(&left_arr, &right_arr, relate_cb)?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Scalar(left_scalar), ColumnarValue::Array(right_array)) => {
            let left_scalar_array = ColumnarValue::values_to_arrays(&[left_scalar.into()])?;
            let left_geo_array = from_arrow_array(&left_scalar_array[0], left_field)?;
            let right_geo_array = from_arrow_array(&right_array, right_field)?;
            let result = relate_prepared_geometry(
                &right_geo_array,
                &left_geo_array,
                PreparedSide::Left,
                relate_cb,
            )?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        (ColumnarValue::Array(left_array), ColumnarValue::Scalar(right_scalar)) => {
            let right_scalar_array = ColumnarValue::values_to_arrays(&[right_scalar.into()])?;
            let right_geo_array = from_arrow_array(&right_scalar_array[0], right_field)?;
            let left_geo_array = from_arrow_array(&left_array, left_field)?;
            let result = relate_prepared_geometry(
                &left_geo_array,
                &right_geo_array,
                PreparedSide::Right,
                relate_cb,
            )?;
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
    }
}

/// Which argument of the relation the prepared (scalar) geometry is.
///
/// The DE-9IM matrix is not symmetric: `a.relate(b)` is the transpose of
/// `b.relate(a)`, so predicates such as `ST_Contains` and `ST_Within` give
/// different answers depending on the order the geometries are related in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreparedSide {
    /// The prepared geometry is the first argument, `geomA`.
    Left,
    /// The prepared geometry is the second argument, `geomB`.
    Right,
}

type GeoGeometryIter<'a> = Box<dyn Iterator<Item = Option<GeoArrowResult<Geometry<f64>>>> + 'a>;

/// Convert a GeoArrowArray into an iterator of geo::Geometry values.
///
/// Keeping the conversion inside the iterator preserves SQL NULL semantics: a
/// geometry that is never paired with a non-null geometry is never converted.
fn to_geo_iter<'a>(array: &'a dyn GeoArrowArray) -> GeoArrowResult<GeoGeometryIter<'a>> {
    downcast_geoarrow_array!(array, _to_geo_iter_impl)
}

fn _to_geo_iter_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<GeoGeometryIter<'a>> {
    Ok(Box::new(array.iter().map(|item| {
        item.map(|geometry| geometry.and_then(|geometry| geometry_to_geo_for_relate(&geometry)))
    })))
}

/// Convert a length-1 GeoArrowArray to a geo::Geometry scalar.
fn to_geo_scalar(arr: &dyn GeoArrowArray) -> GeoArrowResult<Option<geo::Geometry>> {
    let mut values = to_geo_iter(arr)?;
    match values.next() {
        Some(Some(geometry)) => Ok(Some(geometry?)),
        Some(None) | None => Ok(None),
    }
}

/// Convert a GeoArrow geometry to a geo::Geometry for topology operations.
///
/// geo-types cannot represent empty points, so an empty point becomes an empty
/// GeometryCollection and empty points are removed from multi-geometries and
/// collections. The resulting point set is unchanged, while every non-empty
/// geometry still uses the same conversion as the existing GeoArrow helpers.
fn geometry_to_geo_for_relate<T: CoordNum>(
    geometry: &impl GeometryTrait<T = T>,
) -> GeoArrowResult<Geometry<T>> {
    use geo_traits::GeometryType;

    match geometry.as_type() {
        GeometryType::Point(point) => {
            if point.coord().is_none() {
                Ok(Geometry::GeometryCollection(GeometryCollection::empty()))
            } else {
                geometry_to_geo(geometry)
            }
        }
        GeometryType::MultiPoint(multi_point) => {
            let points = multi_point
                .points()
                .filter_map(|point| point.try_to_point())
                .collect();
            Ok(Geometry::MultiPoint(MultiPoint::new(points)))
        }
        GeometryType::GeometryCollection(collection) => {
            let geometries = collection
                .geometries()
                .filter(|child| {
                    !matches!(child.as_type(), GeometryType::Point(point) if point.coord().is_none())
                })
                .map(|child| geometry_to_geo_for_relate(&child))
                .collect::<GeoArrowResult<_>>()?;
            Ok(Geometry::GeometryCollection(GeometryCollection::new_from(
                geometries,
            )))
        }
        _ => geometry_to_geo(geometry),
    }
}

fn validate_finite_coordinates(geometry: &Geometry<f64>) -> GeoDataFusionResult<()> {
    if let Some(coord) = geometry
        .coords_iter()
        .find(|coord| !coord.x.is_finite() || !coord.y.is_finite())
    {
        return Err(DataFusionError::Execution(format!(
            "relate requires finite coordinates, found ({}, {})",
            coord.x, coord.y
        ))
        .into());
    }

    Ok(())
}

fn relate_arrays(
    left: &dyn GeoArrowArray,
    right: &dyn GeoArrowArray,
    relate_cb: impl Fn(IntersectionMatrix) -> bool,
) -> GeoDataFusionResult<BooleanArray> {
    if left.len() != right.len() {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Input arrays must have the same length".to_string(),
        )
        .into());
    }

    let left_values = to_geo_iter(left)?;
    let right_values = to_geo_iter(right)?;
    let mut builder = BooleanBuilder::with_capacity(left.len());

    for (left, right) in left_values.zip(right_values) {
        match (left, right) {
            (Some(left), Some(right)) => {
                let left = left?;
                let right = right?;
                validate_finite_coordinates(&left)?;
                validate_finite_coordinates(&right)?;
                builder.append_value(relate_cb(left.relate(&right)));
            }
            _ => builder.append_null(),
        }
    }

    Ok(builder.finish())
}

fn relate_prepared_geometry(
    array: &dyn GeoArrowArray,
    scalar: &dyn GeoArrowArray,
    prepared_side: PreparedSide,
    relate_cb: impl Fn(IntersectionMatrix) -> bool,
) -> GeoDataFusionResult<BooleanArray> {
    if scalar.is_null(0) {
        return Ok(BooleanArray::new_null(array.len()));
    }

    let mut prepared = None;
    let mut builder = BooleanBuilder::with_capacity(array.len());

    for item in to_geo_iter(array)? {
        let Some(geometry) = item else {
            builder.append_null();
            continue;
        };
        let geometry = geometry?;
        validate_finite_coordinates(&geometry)?;

        if prepared.is_none() {
            let Some(scalar) = to_geo_scalar(scalar)? else {
                return Ok(BooleanArray::new_null(array.len()));
            };
            validate_finite_coordinates(&scalar)?;
            prepared = Some(PreparedGeometry::from(scalar));
        }

        let matrix = match prepared_side {
            PreparedSide::Left => prepared.as_ref().unwrap().relate(&geometry),
            PreparedSide::Right => geometry.relate(prepared.as_ref().unwrap()),
        };
        builder.append_value(relate_cb(matrix));
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use arrow_array::RecordBatch;
    use arrow_array::cast::AsArray;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::config::ConfigOptions;
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use geoarrow_array::array::{GenericWkbArray, PointArray};
    use geoarrow_array::builder::{PointBuilder, WkbBuilder};
    use geoarrow_schema::{Dimension, PointType, WkbType};

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[derive(Clone, Copy)]
    enum TestPoint {
        Empty,
        Coord(f64, f64),
        Null,
    }

    fn point_type() -> PointType {
        PointType::new(Dimension::XY, Default::default())
    }

    fn point_array(points: &[TestPoint]) -> PointArray {
        let mut builder = PointBuilder::new(point_type());
        for point in points {
            match point {
                TestPoint::Empty => builder.push_empty(),
                TestPoint::Coord(x, y) => {
                    builder.push_point(Some(&geo::Point::new(*x, *y)));
                }
                TestPoint::Null => builder.push_null(),
            }
        }
        builder.finish()
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = (pair[0] as char).to_digit(16).unwrap();
                let low = (pair[1] as char).to_digit(16).unwrap();
                ((high << 4) | low) as u8
            })
            .collect()
    }

    fn wkb_array(values: &[Option<&str>]) -> GenericWkbArray<i32> {
        let mut builder = WkbBuilder::<i32>::new(WkbType::default());
        for value in values {
            let bytes = value.map(decode_hex);
            builder.push_wkb(bytes.as_deref()).unwrap();
        }
        builder.finish()
    }

    fn geometry_field(array: &dyn GeoArrowArray) -> Field {
        array.data_type().to_field("geometry", true)
    }

    fn array_value(array: &dyn GeoArrowArray) -> ColumnarValue {
        ColumnarValue::Array(array.to_array_ref())
    }

    fn scalar_value(array: &dyn GeoArrowArray) -> ColumnarValue {
        let array = array.to_array_ref();
        ColumnarValue::Scalar(ScalarValue::try_from_array(array.as_ref(), 0).unwrap())
    }

    fn invoke_udf<U: ScalarUDFImpl>(
        udf: &U,
        left: ColumnarValue,
        left_field: &Field,
        right: ColumnarValue,
        right_field: &Field,
        number_rows: usize,
    ) -> Result<ColumnarValue> {
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![left, right],
            arg_fields: vec![Arc::new(left_field.clone()), Arc::new(right_field.clone())],
            number_rows,
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn invoke_bool<U: ScalarUDFImpl>(
        udf: &U,
        left: ColumnarValue,
        left_field: &Field,
        right: ColumnarValue,
        right_field: &Field,
        number_rows: usize,
    ) -> Vec<Option<bool>> {
        match invoke_udf(udf, left, left_field, right, right_field, number_rows).unwrap() {
            ColumnarValue::Array(array) => array.as_boolean().iter().collect(),
            other => panic!("unexpected relation result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_intersects_scalars_true() {
        let ctx = SessionContext::new();

        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Intersects(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING ( 0 0, 0 2 )'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        assert!(col.as_boolean().value(0));
    }

    #[tokio::test]
    async fn test_intersects_scalars_false() {
        let ctx = SessionContext::new();

        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Intersects(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText('LINESTRING ( 2 0, 0 2 )'));")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        assert!(!col.as_boolean().value(0));
    }

    #[tokio::test]
    async fn test_intersects_array_scalar() {
        let ctx = SessionContext::new();

        let point1 = wkt::wkt! { POINT(0.0 0.0) };
        let point2 = wkt::wkt! { POINT(2.0 2.0) };
        let point_arr = PointBuilder::from_points(
            [point1, point2].iter(),
            PointType::new(Dimension::XY, Default::default()),
        )
        .finish();

        let schema = Schema::new([Arc::new(point_arr.data_type().to_field("geometry", true))]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![point_arr.to_array_ref()]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let df = ctx
            .sql("SELECT ST_Intersects(ST_GeomFromText('LINESTRING ( 0 0, 0 2 )'), geometry) FROM t;")
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0);
        assert!(col.as_boolean().value(0));
        assert!(!col.as_boolean().value(1));
    }

    #[test]
    fn test_empty_point_semantics_across_columnar_combinations() {
        let empty = point_array(&[TestPoint::Empty]);
        let mixed = point_array(&[
            TestPoint::Empty,
            TestPoint::Coord(0.0, 0.0),
            TestPoint::Null,
        ]);
        let empty_field = geometry_field(&empty);
        let mixed_field = geometry_field(&mixed);

        assert_eq!(
            invoke_bool(
                &Equals::new(),
                array_value(&mixed),
                &mixed_field,
                array_value(&mixed),
                &mixed_field,
                mixed.len(),
            ),
            vec![Some(true), Some(true), None]
        );
        assert_eq!(
            invoke_bool(
                &Equals::new(),
                scalar_value(&empty),
                &empty_field,
                array_value(&mixed),
                &mixed_field,
                mixed.len(),
            ),
            vec![Some(true), Some(false), None]
        );
        assert_eq!(
            invoke_bool(
                &Equals::new(),
                array_value(&mixed),
                &mixed_field,
                scalar_value(&empty),
                &empty_field,
                mixed.len(),
            ),
            vec![Some(true), Some(false), None]
        );
        assert_eq!(
            invoke_bool(
                &Disjoint::new(),
                scalar_value(&empty),
                &empty_field,
                array_value(&mixed),
                &mixed_field,
                mixed.len(),
            ),
            vec![Some(true), Some(true), None]
        );
        assert_eq!(
            invoke_bool(
                &Covers::new(),
                scalar_value(&empty),
                &empty_field,
                array_value(&mixed),
                &mixed_field,
                mixed.len(),
            ),
            vec![Some(false), Some(false), None]
        );
    }

    #[test]
    fn test_empty_point_matches_other_empty_geometries() {
        let point = point_array(&[TestPoint::Empty]);
        // An empty polygon has the same point set as an empty point.
        let polygon = wkb_array(&[Some("010300000000000000")]);
        let point_field = geometry_field(&point);
        let polygon_field = geometry_field(&polygon);

        assert_eq!(
            invoke_bool(
                &Equals::new(),
                array_value(&point),
                &point_field,
                array_value(&polygon),
                &polygon_field,
                1,
            ),
            vec![Some(true)]
        );
        assert_eq!(
            invoke_bool(
                &Disjoint::new(),
                array_value(&point),
                &point_field,
                array_value(&polygon),
                &polygon_field,
                1,
            ),
            vec![Some(true)]
        );
        assert_eq!(
            invoke_bool(
                &Covers::new(),
                array_value(&point),
                &point_field,
                array_value(&polygon),
                &polygon_field,
                1,
            ),
            vec![Some(false)]
        );
    }

    #[test]
    fn test_empty_points_in_multi_geometries() {
        // MULTIPOINT(EMPTY, (0 0)) and MULTIPOINT(0 0).
        let multipoint_with_empty = wkb_array(&[Some(
            "0104000000020000000101000000000000000000f87f000000000000f87f010100000000000000000000000000000000000000",
        )]);
        let multipoint = wkb_array(&[Some(
            "010400000001000000010100000000000000000000000000000000000000",
        )]);

        // GEOMETRYCOLLECTION(POINT EMPTY, POINT(0 0)) and
        // GEOMETRYCOLLECTION(POINT(0 0)).
        let collection_with_empty = wkb_array(&[Some(
            "0107000000020000000101000000000000000000f87f000000000000f87f010100000000000000000000000000000000000000",
        )]);
        let collection = wkb_array(&[Some(
            "010700000001000000010100000000000000000000000000000000000000",
        )]);

        for (left, right, name) in [
            (
                &multipoint_with_empty as &dyn GeoArrowArray,
                &multipoint as &dyn GeoArrowArray,
                "multipoint",
            ),
            (&collection_with_empty, &collection, "geometry collection"),
        ] {
            let left_field = geometry_field(left);
            let right_field = geometry_field(right);
            assert_eq!(
                invoke_bool(
                    &Equals::new(),
                    array_value(left),
                    &left_field,
                    array_value(right),
                    &right_field,
                    1,
                ),
                vec![Some(true)],
                "{name}"
            );
            assert_eq!(
                invoke_bool(
                    &Covers::new(),
                    array_value(left),
                    &left_field,
                    array_value(right),
                    &right_field,
                    1,
                ),
                vec![Some(true)],
                "{name}"
            );
        }
    }

    #[test]
    fn test_nonfinite_coordinates_return_execution_errors() {
        let valid = point_array(&[TestPoint::Coord(0.0, 0.0)]);
        let valid_field = geometry_field(&valid);

        for (x, y) in [(f64::NAN, 0.0), (0.0, f64::INFINITY)] {
            let malformed = point_array(&[TestPoint::Coord(x, y)]);
            let malformed_field = geometry_field(&malformed);

            let error = invoke_udf(
                &Intersects::new(),
                array_value(&malformed),
                &malformed_field,
                array_value(&valid),
                &valid_field,
                1,
            )
            .unwrap_err();
            assert!(
                matches!(error, DataFusionError::Execution(_)),
                "left malformed coordinate ({x}, {y}): {error}"
            );

            let error = invoke_udf(
                &Intersects::new(),
                array_value(&valid),
                &valid_field,
                array_value(&malformed),
                &malformed_field,
                1,
            )
            .unwrap_err();
            assert!(
                matches!(error, DataFusionError::Execution(_)),
                "right malformed coordinate ({x}, {y}): {error}"
            );

            let error = invoke_udf(
                &Intersects::new(),
                scalar_value(&malformed),
                &malformed_field,
                array_value(&valid),
                &valid_field,
                1,
            )
            .unwrap_err();
            assert!(
                matches!(error, DataFusionError::Execution(_)),
                "scalar-first malformed coordinate ({x}, {y}): {error}"
            );

            let error = invoke_udf(
                &Intersects::new(),
                array_value(&valid),
                &valid_field,
                scalar_value(&malformed),
                &malformed_field,
                1,
            )
            .unwrap_err();
            assert!(
                matches!(error, DataFusionError::Execution(_)),
                "scalar-second malformed coordinate ({x}, {y}): {error}"
            );
        }
    }

    #[test]
    fn test_unused_malformed_scalars_and_null_rows_are_not_evaluated() {
        let malformed = point_array(&[TestPoint::Coord(f64::NAN, 0.0)]);
        let malformed_field = geometry_field(&malformed);
        let nulls = point_array(&[TestPoint::Null, TestPoint::Null]);
        let nulls_field = geometry_field(&nulls);

        assert_eq!(
            invoke_bool(
                &Intersects::new(),
                scalar_value(&malformed),
                &malformed_field,
                array_value(&nulls),
                &nulls_field,
                nulls.len(),
            ),
            vec![None, None]
        );
        assert_eq!(
            invoke_bool(
                &Intersects::new(),
                array_value(&nulls),
                &nulls_field,
                scalar_value(&malformed),
                &malformed_field,
                nulls.len(),
            ),
            vec![None, None]
        );

        let zero_rows = point_array(&[]);
        let zero_rows_field = geometry_field(&zero_rows);
        assert!(
            invoke_bool(
                &Intersects::new(),
                scalar_value(&malformed),
                &malformed_field,
                array_value(&zero_rows),
                &zero_rows_field,
                0,
            )
            .is_empty()
        );

        let null = point_array(&[TestPoint::Null]);
        let null_field = geometry_field(&null);
        assert_eq!(
            invoke_bool(
                &Intersects::new(),
                array_value(&malformed),
                &malformed_field,
                array_value(&null),
                &null_field,
                1,
            ),
            vec![None]
        );
    }

    #[test]
    fn test_relate_paths_agree_for_valid_points() {
        let left = point_array(&[TestPoint::Coord(0.0, 0.0), TestPoint::Coord(1.0, 1.0)]);
        let right = point_array(&[TestPoint::Coord(0.0, 0.0), TestPoint::Coord(2.0, 2.0)]);
        let left_scalar = point_array(&[TestPoint::Coord(0.0, 0.0)]);
        let right_scalar = point_array(&[TestPoint::Coord(0.0, 0.0)]);
        let left_field = geometry_field(&left);
        let right_field = geometry_field(&right);
        let left_scalar_field = geometry_field(&left_scalar);
        let right_scalar_field = geometry_field(&right_scalar);

        let array_array = vec![Some(true), Some(false)];
        assert_eq!(
            invoke_bool(
                &Intersects::new(),
                array_value(&left),
                &left_field,
                array_value(&right),
                &right_field,
                left.len(),
            ),
            array_array
        );
        assert_eq!(
            invoke_bool(
                &Intersects::new(),
                scalar_value(&left_scalar),
                &left_scalar_field,
                array_value(&right),
                &right_field,
                right.len(),
            ),
            array_array
        );
        assert_eq!(
            invoke_bool(
                &Intersects::new(),
                array_value(&left),
                &left_field,
                scalar_value(&right_scalar),
                &right_scalar_field,
                left.len(),
            ),
            array_array
        );
    }

    #[tokio::test]
    async fn test_empty_point_sql_integration() {
        let ctx = SessionContext::new();
        register_relate_udfs(&ctx);

        let df = ctx
            .sql(
                "SELECT \
                    ST_Equals(ST_GeomFromText('POINT EMPTY'), ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')) AS equals, \
                    ST_Disjoint(ST_GeomFromText('POINT EMPTY'), ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')) AS disjoint, \
                    ST_Covers(ST_GeomFromText('POINT EMPTY'), ST_GeomFromText('GEOMETRYCOLLECTION EMPTY')) AS covers",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(
            batch
                .column_by_name("equals")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("disjoint")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("covers")
                .unwrap()
                .as_boolean()
                .value(0)
        );
    }

    fn register_relate_udfs(ctx: &SessionContext) {
        ctx.register_udf(Contains::new().into());
        ctx.register_udf(CoveredBy::new().into());
        ctx.register_udf(Covers::new().into());
        ctx.register_udf(Crosses::new().into());
        ctx.register_udf(Disjoint::new().into());
        ctx.register_udf(Equals::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(Overlaps::new().into());
        ctx.register_udf(Touches::new().into());
        ctx.register_udf(Within::new().into());
    }

    /// A table `t` with one point inside the square `(0 0)`-`(4 4)` and one outside it.
    fn points_table(ctx: &SessionContext) {
        let inside = wkt::wkt! { POINT(1.0 1.0) };
        let outside = wkt::wkt! { POINT(5.0 5.0) };
        let point_arr = PointBuilder::from_points(
            [inside, outside].iter(),
            PointType::new(Dimension::XY, Default::default()),
        )
        .finish();

        let schema = Schema::new([Arc::new(point_arr.data_type().to_field("geometry", true))]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![point_arr.to_array_ref()]).unwrap();
        ctx.register_batch("t", batch).unwrap();
    }

    async fn query_booleans(ctx: &SessionContext, sql: &str) -> Vec<Option<bool>> {
        let df = ctx.sql(sql).await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        batch.column(0).as_boolean().iter().collect()
    }

    /// Each predicate must give the same answer whichever argument is the scalar, and the
    /// same answer as with both arguments as arrays.
    #[tokio::test]
    async fn test_scalar_argument_order() {
        let ctx = SessionContext::new();
        register_relate_udfs(&ctx);
        points_table(&ctx);

        let square = "ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')";
        // (sql, expected for the inside point, expected for the outside point)
        let cases = [
            (format!("ST_Contains({square}, geometry)"), true, false),
            (format!("ST_Within(geometry, {square})"), true, false),
            (format!("ST_Covers({square}, geometry)"), true, false),
            (format!("ST_CoveredBy(geometry, {square})"), true, false),
            // A point never contains, covers or lies around a polygon.
            (format!("ST_Contains(geometry, {square})"), false, false),
            (format!("ST_Within({square}, geometry)"), false, false),
            (format!("ST_Covers(geometry, {square})"), false, false),
            (format!("ST_CoveredBy({square}, geometry)"), false, false),
            (format!("ST_Intersects({square}, geometry)"), true, false),
            (format!("ST_Disjoint({square}, geometry)"), false, true),
        ];
        for (expr, inside, outside) in cases {
            let sql = format!("SELECT {expr} FROM t");
            assert_eq!(
                query_booleans(&ctx, &sql).await,
                vec![Some(inside), Some(outside)],
                "{expr}"
            );
        }
    }

    /// A scalar-first relation must match the array-array evaluation of the same SQL.
    #[tokio::test]
    async fn test_scalar_first_matches_array_array() {
        let ctx = SessionContext::new();
        register_relate_udfs(&ctx);
        points_table(&ctx);

        let square = "ST_GeomFromText('POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))')";
        for udf in [
            "ST_Contains",
            "ST_Within",
            "ST_Covers",
            "ST_CoveredBy",
            "ST_Intersects",
            "ST_Disjoint",
            "ST_Touches",
            "ST_Crosses",
            "ST_Overlaps",
            "ST_Equals",
        ] {
            let scalar_first = format!("SELECT {udf}({square}, geometry) FROM t");
            let array_array = format!(
                "SELECT {udf}(s.square, t.geometry) FROM t CROSS JOIN (SELECT {square} AS square) AS s"
            );
            let column_first = format!("SELECT {udf}(geometry, {square}) FROM t");
            let array_array_column_first = format!(
                "SELECT {udf}(t.geometry, s.square) FROM t CROSS JOIN (SELECT {square} AS square) AS s"
            );
            assert_eq!(
                query_booleans(&ctx, &scalar_first).await,
                query_booleans(&ctx, &array_array).await,
                "{udf}(scalar, array)"
            );
            assert_eq!(
                query_booleans(&ctx, &column_first).await,
                query_booleans(&ctx, &array_array_column_first).await,
                "{udf}(array, scalar)"
            );
        }
    }

    #[tokio::test]
    async fn test_null_scalar_is_null() {
        let ctx = SessionContext::new();
        register_relate_udfs(&ctx);
        points_table(&ctx);

        for sql in [
            "SELECT ST_Contains(ST_GeomFromText(CAST(NULL AS VARCHAR)), geometry) FROM t",
            "SELECT ST_Within(geometry, ST_GeomFromText(CAST(NULL AS VARCHAR))) FROM t",
            "SELECT ST_Intersects(ST_GeomFromText(CAST(NULL AS VARCHAR)), geometry) FROM t",
            "SELECT ST_Intersects(geometry, ST_GeomFromText(CAST(NULL AS VARCHAR))) FROM t",
        ] {
            assert_eq!(query_booleans(&ctx, sql).await, vec![None, None], "{sql}");
        }
    }

    /// https://postgis.net/docs/ST_Overlaps.html
    #[tokio::test]
    async fn test_point_linestring() {
        let ctx = SessionContext::new();

        ctx.register_udf(Contains::new().into());
        ctx.register_udf(Crosses::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(Overlaps::new().into());

        let sql_str = r#"
            SELECT ST_Overlaps(a,b) AS overlaps,       ST_Crosses(a,b) AS crosses,
                   ST_Intersects(a, b) AS intersects,  ST_Contains(b,a) AS b_contains_a
            FROM (SELECT
                ST_GeomFromText('POINT (100 100)') As a,
                ST_GeomFromText('LINESTRING (30 50, 40 160, 160 40, 180 160)') AS b) AS t
        "#;

        let df = ctx.sql(sql_str).await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(
            !batch
                .column_by_name("overlaps")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("crosses")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("intersects")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("b_contains_a")
                .unwrap()
                .as_boolean()
                .value(0)
        );
    }

    /// https://postgis.net/docs/ST_Overlaps.html
    #[tokio::test]
    async fn test_linestring_polygon() {
        let ctx = SessionContext::new();

        ctx.register_udf(Contains::new().into());
        ctx.register_udf(Crosses::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(Overlaps::new().into());

        let sql_str = r#"
            SELECT ST_Overlaps(a,b) AS overlaps,       ST_Crosses(a,b) AS crosses,
                   ST_Intersects(a, b) AS intersects,  ST_Contains(b,a) AS b_contains_a
            FROM (SELECT
                ST_GeomFromText('POLYGON ((40 170, 90 30, 180 100, 40 170))') AS a,
             ST_GeomFromText('LINESTRING(10 10, 190 190)') AS b) AS t
        "#;

        let df = ctx.sql(sql_str).await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(
            !batch
                .column_by_name("overlaps")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("crosses")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("intersects")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("b_contains_a")
                .unwrap()
                .as_boolean()
                .value(0)
        );
    }

    /// https://postgis.net/docs/ST_Overlaps.html
    #[tokio::test]
    async fn test_polygon_polygon() {
        let ctx = SessionContext::new();

        ctx.register_udf(Contains::new().into());
        ctx.register_udf(Crosses::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());
        ctx.register_udf(Intersects::new().into());
        ctx.register_udf(Overlaps::new().into());

        let sql_str = r#"
            SELECT ST_Overlaps(a,b) AS overlaps,       ST_Crosses(a,b) AS crosses,
                   ST_Intersects(a, b) AS intersects,  ST_Contains(b,a) AS b_contains_a
            FROM (SELECT
                ST_GeomFromText('POLYGON ((40 170, 90 30, 180 100, 40 170))') AS a,
             ST_GeomFromText('POLYGON ((110 180, 20 60, 130 90, 110 180))') AS b) AS t
        "#;

        let df = ctx.sql(sql_str).await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        assert!(
            batch
                .column_by_name("overlaps")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("crosses")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            batch
                .column_by_name("intersects")
                .unwrap()
                .as_boolean()
                .value(0)
        );
        assert!(
            !batch
                .column_by_name("b_contains_a")
                .unwrap()
                .as_boolean()
                .value(0)
        );
    }
}
