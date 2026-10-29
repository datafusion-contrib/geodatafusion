//! Rectangular polygon constructor

use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::PolygonArray;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_array::capacity::PolygonCapacity;
use geoarrow_schema::{CoordType, Crs, Dimension, Metadata, PolygonType};
use wkt::types::{Coord, Dimension as WktDimension, LineString, Polygon as WktPolygon};

use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeEnvelope {
    signature: Signature,
    coord_type: CoordType,
}

impl MakeEnvelope {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for MakeEnvelope {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_ENVELOPE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakeEnvelope {
    fn name(&self) -> &str {
        "st_makeenvelope"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let typ =
            PolygonType::new(Dimension::XY, Default::default()).with_coord_type(self.coord_type);
        let typ =
            polygon_type_with_srid(typ, args.scalar_arguments.get(4).map(|srid| srid.cloned()))?;

        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let polygon_arr = create_polygon_array_from_args(&args)?;
        Ok(polygon_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_ENVELOPE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a rectangular Polygon from minimum and maximum coordinates.",
                "ST_MakeEnvelope(-71.104, 42.315, -71.0, 42.4, 4326)",
            )
            .with_argument("xmin", "minimum X coordinate")
            .with_argument("ymin", "minimum Y coordinate")
            .with_argument("xmax", "maximum X coordinate")
            .with_argument("ymax", "maximum Y coordinate")
            .with_argument("srid", "integer SRID value")
            .with_related_udf("st_point")
            .with_related_udf("st_makepoint")
            .build()
        }))
    }
}

fn polygon_type_with_srid(
    mut typ: PolygonType,
    srid: Option<Option<ScalarValue>>,
) -> Result<PolygonType> {
    match srid {
        None | Some(None) | Some(Some(ScalarValue::Int64(None))) => {}
        Some(Some(ScalarValue::Int64(Some(srid)))) => {
            let crs = Crs::from_authority_code(format!("EPSG:{srid}"));
            typ = typ.with_metadata(Arc::new(Metadata::new(crs, None)));
        }
        _ => return Err(invalid_srid_error()),
    }
    Ok(typ)
}

fn create_polygon_array_from_args(args: &ScalarFunctionArgs) -> Result<PolygonArray> {
    if srid_is_null(args)? {
        return Ok(create_null_polygon_array(
            &args.return_field,
            args.number_rows,
        )?);
    }

    let arrays = ColumnarValue::values_to_arrays(&args.args[..4])?;
    Ok(create_polygon_array(arrays, &args.return_field)?)
}

fn srid_is_null(args: &ScalarFunctionArgs) -> Result<bool> {
    match args.args.get(4) {
        None | Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(_)))) => Ok(false),
        Some(ColumnarValue::Scalar(ScalarValue::Int64(None))) => Ok(true),
        Some(ColumnarValue::Array(array)) => {
            // FFI materializes scalar arguments as arrays. Null arrays represent a
            // scalar NULL; non-null arrays must be uniform and match the field CRS.
            if array.data_type() != &DataType::Int64 {
                return Err(invalid_srid_error());
            }

            let srid = array.as_primitive::<Int64Type>();
            if srid.null_count() == srid.len() {
                return Ok(true);
            }
            if srid.null_count() != 0 {
                return Err(invalid_srid_error());
            }

            let Some(first) = srid.values().first() else {
                return Ok(false);
            };
            if srid.values().iter().any(|value| value != first) {
                return Err(invalid_srid_error());
            }

            let expected_crs = Crs::from_authority_code(format!("EPSG:{first}"));
            let polygon_type = args.return_field.extension_type::<PolygonType>();
            if polygon_type.metadata().crs() == &expected_crs {
                Ok(false)
            } else {
                Err(invalid_srid_error())
            }
        }
        _ => Err(invalid_srid_error()),
    }
}

fn invalid_srid_error() -> DataFusionError {
    DataFusionError::Internal("ST_MakeEnvelope only supports SRID as a scalar integer".to_string())
}

fn create_null_polygon_array(
    return_field: &Field,
    number_rows: usize,
) -> GeoDataFusionResult<PolygonArray> {
    let typ = return_field.extension_type::<PolygonType>();
    let mut builder =
        PolygonBuilder::with_capacity(typ.clone(), PolygonCapacity::new(0, 0, number_rows));
    for _ in 0..number_rows {
        builder.push_polygon(None::<&WktPolygon>)?;
    }
    Ok(builder.finish())
}

fn create_polygon_array(
    arrays: Vec<ArrayRef>,
    return_field: &Field,
) -> GeoDataFusionResult<PolygonArray> {
    let xmin = arrays[0].as_primitive::<Float64Type>();
    let ymin = arrays[1].as_primitive::<Float64Type>();
    let xmax = arrays[2].as_primitive::<Float64Type>();
    let ymax = arrays[3].as_primitive::<Float64Type>();

    let typ = return_field.extension_type::<PolygonType>();
    let mut builder = PolygonBuilder::with_capacity(
        typ.clone(),
        PolygonCapacity::new(xmin.len().saturating_mul(5), xmin.len(), xmin.len()),
    );

    // PostGIS preserves the supplied bounds and ring order even when bounds are
    // reversed or degenerate.
    for (((xmin, ymin), xmax), ymax) in xmin
        .iter()
        .zip(ymin.iter())
        .zip(xmax.iter())
        .zip(ymax.iter())
    {
        let (Some(xmin), Some(ymin), Some(xmax), Some(ymax)) = (xmin, ymin, xmax, ymax) else {
            builder.push_polygon(None::<&WktPolygon>)?;
            continue;
        };

        let exterior = LineString::new(
            vec![
                Coord {
                    x: xmin,
                    y: ymin,
                    z: None,
                    m: None,
                },
                Coord {
                    x: xmin,
                    y: ymax,
                    z: None,
                    m: None,
                },
                Coord {
                    x: xmax,
                    y: ymax,
                    z: None,
                    m: None,
                },
                Coord {
                    x: xmax,
                    y: ymin,
                    z: None,
                    m: None,
                },
                Coord {
                    x: xmin,
                    y: ymin,
                    z: None,
                    m: None,
                },
            ],
            WktDimension::XY,
        );
        let polygon = WktPolygon::new(vec![exterior], WktDimension::XY);
        builder.push_polygon(Some(&polygon))?;
    }

    Ok(builder.finish())
}
