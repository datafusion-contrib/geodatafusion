use std::any::Any;
use std::sync::{Arc, OnceLock};

use arrow_array::BooleanArray;
use arrow_array::builder::BooleanBuilder;
use arrow_schema::DataType;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait};
use geoarrow_array::{GeoArrowArrayAccessor, WrapArray, downcast_geoarrow_array};
use geoarrow_schema::GeoArrowType;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
                "Tests if a LineStrings's start and end points are coincident.",
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
    let geo_array = geo_type.wrap_array(&array)?;
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
            Some(geom) => {
                let geom = geom?;
                let is_closed = match geom.as_type() {
                    geo_traits::GeometryType::LineString(ls) => check_ls_closed(ls),
                    geo_traits::GeometryType::MultiLineString(mls) => {
                        mls.num_line_strings() > 0
                            && mls.line_strings().all(|ls| check_ls_closed(&ls))
                    }
                    geo_traits::GeometryType::Point(_)
                    | geo_traits::GeometryType::MultiPoint(_)
                    | geo_traits::GeometryType::Polygon(_)
                    | geo_traits::GeometryType::MultiPolygon(_) => true,
                    _ => false,
                };
                builder.append_value(is_closed);
            }
            None => {
                builder.append_null();
            }
        }
    }

    Ok(builder.finish())
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
