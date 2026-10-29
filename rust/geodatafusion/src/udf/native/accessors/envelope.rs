use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo::{Coord, LineString, Point, Polygon};
use geo_traits::UnimplementedGeometry;
use geoarrow_array::array::GeometryArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::bounds::{GeometryExtent, geometry_extent};
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata, field_metadata_or_default, geometry_array_from_arrow,
    is_null_geometry_input, null_geometry_array,
};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Envelope {
    signature: Signature,
    coord_type: CoordType,
}

impl Envelope {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: any_single_geometry_type_input().clone(),
            coord_type,
        }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Envelope {
    fn name(&self) -> &str {
        "st_envelope"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let metadata = field_metadata(args.arg_fields[0].as_ref())?;
        let output_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        Ok(envelope_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the minimum bounding box of a geometry as a geometry. Degenerate envelopes are returned as Point or LineString geometries.",
                "ST_Envelope(geometry)",
            )
            .with_argument("g1", "geometry")
            .build()
        }))
    }
}

fn envelope_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let input_field = args.arg_fields[0].as_ref();
    if is_null_geometry_input(input_field, arrays[0].as_ref()) {
        let output_type =
            GeometryType::new(field_metadata_or_default(input_field)?).with_coord_type(coord_type);
        return Ok(null_geometry_array(output_type, arrays[0].len())?.into());
    }
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let output_type =
        GeometryType::new(geo_array.data_type().metadata().clone()).with_coord_type(coord_type);
    let geo_array_ref = geo_array.as_ref();
    let output = downcast_geoarrow_array!(geo_array_ref, envelope_array, &output_type)?;
    Ok(output.into_array_ref().into())
}

fn envelope_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeometryType,
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = StrictGeometryBuilder::new(output_type.clone());

    for geometry in array.iter() {
        let Some(geometry) = geometry.transpose()? else {
            builder.push_geometry(None::<&UnimplementedGeometry<f64>>)?;
            continue;
        };

        match geometry_extent(&geometry) {
            GeometryExtent::Empty => builder.push_geometry(Some(&geometry))?,
            GeometryExtent::NonFinite => {
                return Err(DataFusionError::Execution(
                    "ST_Envelope does not support non-finite coordinates".to_string(),
                )
                .into());
            }
            GeometryExtent::Finite(bounds) => {
                let [xmin, ymin, ..] = bounds.min();
                let [xmax, ymax, ..] = bounds.max();
                let envelope = if xmin == xmax && ymin == ymax {
                    geo::Geometry::Point(Point::new(xmin, ymin))
                } else if xmin == xmax || ymin == ymax {
                    geo::Geometry::LineString(LineString::new(vec![
                        Coord { x: xmin, y: ymin },
                        Coord { x: xmax, y: ymax },
                    ]))
                } else {
                    let ring = LineString::new(vec![
                        Coord { x: xmin, y: ymin },
                        Coord { x: xmin, y: ymax },
                        Coord { x: xmax, y: ymax },
                        Coord { x: xmax, y: ymin },
                        Coord { x: xmin, y: ymin },
                    ]);
                    geo::Geometry::Polygon(Polygon::new(ring, vec![]))
                };
                builder.push_geometry(Some(&envelope))?;
            }
        }
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod test {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register(&ctx);
        ctx
    }

    #[tokio::test]
    async fn envelope_matches_postgis_shapes_and_dimensions() {
        let ctx = ctx();
        let cases = [
            ("POINT(1 3)", "POINT(1 3)"),
            ("LINESTRING(0 0,1 3)", "POLYGON((0 0,0 3,1 3,1 0,0 0))"),
            ("LINESTRING(1 0,1 5)", "LINESTRING(1 0,1 5)"),
            (
                "LINESTRING Z (1 2 3,4 5 6)",
                "POLYGON((1 2,1 5,4 5,4 2,1 2))",
            ),
            ("POLYGON EMPTY", "POLYGON EMPTY"),
        ];

        for (input, expected) in cases {
            let batch = ctx
                .sql(&format!(
                    "SELECT ST_AsText(ST_Envelope(ST_GeomFromText('{input}')))"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
            assert_eq!(
                batch.column(0).as_string::<i32>().value(0),
                expected,
                "input: {input}"
            );
        }
    }

    #[tokio::test]
    async fn envelope_propagates_null() {
        let ctx = ctx();
        let batch = ctx
            .sql(
                "WITH t(wkt) AS (VALUES ('POINT(1 2)'), (CAST(NULL AS TEXT))) \
                 SELECT ST_AsText(ST_Envelope(ST_GeomFromText(wkt))) FROM t ORDER BY wkt NULLS LAST",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let values = batch.column(0).as_string::<i32>();
        assert_eq!(values.value(0), "POINT(1 2)");
        assert!(values.is_null(1));
    }
}
