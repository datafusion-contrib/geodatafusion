use std::sync::{Arc, OnceLock};

use arrow_array::BooleanArray;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    CoordTrait, GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait,
    MultiPointTrait, MultiPolygonTrait, PointTrait,
};
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::is_empty::is_geometry_topologically_empty;
use crate::udf::native::geometry::geometry_array_from_type;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsClosed;

impl IsClosed {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for IsClosed {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsClosed {
    fn name(&self) -> &str {
        "st_isclosed"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(is_closed_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true when a geometry is non-empty, every part is non-empty, and every \
                 line-like part has coincident start and end points. ST_IsClosed(NULL) is NULL \
                 and EMPTY geometries are not closed, following PostGIS.",
                "ST_IsClosed(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

fn is_closed_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let array = ColumnarValue::values_to_arrays(&args.args)?
        .into_iter()
        .next()
        .unwrap();
    let geo_type = GeoArrowType::from_arrow_field(&args.arg_fields[0])?;
    let geo_array = geometry_array_from_type(&geo_type, &array)?;
    let geo_array_ref = geo_array.as_ref();

    let result = downcast_geoarrow_array!(geo_array_ref, impl_is_closed)?;

    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn impl_is_closed<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(array.len());

    for item in array.iter() {
        match item {
            Some(geom) => builder.append_value(is_closed(&geom?)),
            None => builder.append_null(),
        }
    }

    Ok(builder.finish())
}

/// PostGIS reports a geometry as closed when it is non-empty, every part is
/// non-empty, and every line-like part has coincident start and end points.
fn is_closed(geom: &impl GeometryTrait<T = f64>) -> bool {
    use geo_traits::GeometryType::*;

    if is_geometry_topologically_empty(geom) {
        return false;
    }
    match geom.as_type() {
        Point(_) | Polygon(_) | Rect(_) | Triangle(_) | Line(_) => true,
        LineString(line_string) => check_ls_closed(line_string),
        MultiPoint(points) => points.points().all(|point| point.coord().is_some()),
        MultiLineString(lines) => lines.line_strings().all(|line| is_closed(&line)),
        MultiPolygon(polygons) => polygons.polygons().all(|polygon| is_closed(&polygon)),
        GeometryCollection(collection) => collection.geometries().all(|child| is_closed(&child)),
    }
}

fn check_ls_closed(ls: &impl LineStringTrait<T = f64>) -> bool {
    let n = ls.num_coords();
    if n < 2 {
        return false;
    }
    if let (Some(first), Some(last)) = (ls.coord(0), ls.coord(n - 1)) {
        first.x() == last.x() && first.y() == last.y()
    } else {
        false
    }
}

#[cfg(test)]
mod test {
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::udf::native::io::GeomFromText;

    #[tokio::test]
    async fn test_st_isclosed() {
        let ctx = SessionContext::new();
        ctx.register_udf(IsClosed::new().into());
        ctx.register_udf(GeomFromText::new(Default::default()).into());

        let cases = vec![
            ("LINESTRING(0 0, 1 1, 0 1, 0 0)", true, "closed linestring"),
            ("LINESTRING(0 0, 1 1, 1 0)", false, "open linestring"),
            ("LINESTRING(0 0, 0 0)", true, "single segment closed"),
            ("LINESTRING EMPTY", false, "empty linestring is not closed"),
            (
                "MULTILINESTRING((0 0, 1 1, 0 0), (2 2, 3 3, 2 2))",
                true,
                "all closed",
            ),
            (
                "MULTILINESTRING((0 0, 1 1, 0 0), (2 2, 3 3, 2 3))",
                false,
                "one open",
            ),
            (
                "MULTILINESTRING EMPTY",
                false,
                "empty multilinestring is not closed",
            ),
            ("POINT(0 0)", true, "point is closed"),
            ("MULTIPOINT(0 0, 1 1)", true, "multipoint is closed"),
            ("POLYGON((0 0, 1 0, 1 1, 0 0))", true, "polygon is closed"),
            (
                "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))",
                true,
                "multipolygon is closed",
            ),
        ];

        for (wkt, expected, description) in cases {
            let sql = format!("SELECT ST_IsClosed(ST_GeomFromText('{}'))", wkt);
            let df = ctx
                .sql(&sql)
                .await
                .unwrap_or_else(|_| panic!("Failed to execute SQL for {}", description));

            let batch = df.collect().await.unwrap().into_iter().next().unwrap();
            let col = batch.column(0).as_boolean();

            assert_eq!(col.value(0), expected, "Failed on {}: {}", description, wkt);
        }
    }
}
