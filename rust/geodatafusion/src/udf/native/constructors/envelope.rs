use std::sync::{Arc, OnceLock};

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_schema::{Dimension, PolygonType};
use wkt::types::{Coord as WktCoord, Dimension as WktDimension, LineString, Polygon};

use super::util::CoordValue;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{metadata_for_srid, normalize_srid, resolve_srid_argument};

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakeEnvelope {
    signature: Signature,
}

impl MakeEnvelope {
    pub fn new() -> Self {
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
        }
    }
}

impl Default for MakeEnvelope {
    fn default() -> Self {
        Self::new()
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
        Err(DataFusionError::Internal(
            "Return field is computed from the SRID argument.".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let srid = match args.scalar_arguments.get(4) {
            None => 0,
            Some(Some(ScalarValue::Int64(Some(value)))) => normalize_srid(*value)?,
            Some(Some(ScalarValue::Int64(None) | ScalarValue::Null)) => 0,
            Some(None) => 0,
            Some(Some(other)) => {
                return Err(DataFusionError::Plan(format!(
                    "ST_MakeEnvelope requires SRID to be Int64, got {other}"
                )));
            }
        };
        let typ = PolygonType::new(Dimension::XY, metadata_for_srid(srid));
        Ok(Arc::new(typ.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(make_envelope_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_ENVELOPE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates an axis-aligned rectangular Polygon from minimum and maximum X/Y coordinates. \
                 The optional SRID defaults to unknown (0). As in PostGIS, minimum coordinates are not sorted.",
                "ST_MakeEnvelope(xmin, ymin, xmax, ymax[, srid])",
            )
            .with_argument("xmin", "minimum X coordinate")
            .with_argument("ymin", "minimum Y coordinate")
            .with_argument("xmax", "maximum X coordinate")
            .with_argument("ymax", "maximum Y coordinate")
            .with_argument("srid", "optional scalar integer SRID")
            .with_related_udf("ST_TileEnvelope")
            .build()
        }))
    }
}

fn make_envelope_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays
        .iter()
        .any(|array| array.data_type() == &DataType::Null)
    {
        let typ = args.return_field.extension_type::<PolygonType>();
        let mut builder = PolygonBuilder::new(typ);
        for _ in 0..args.number_rows {
            builder.push_polygon(Option::<&Polygon<f64>>::None)?;
        }
        return Ok(ColumnarValue::Array(builder.finish().into_array_ref()));
    }
    let xmin = arrays[0].as_primitive::<Float64Type>();
    let ymin = arrays[1].as_primitive::<Float64Type>();
    let xmax = arrays[2].as_primitive::<Float64Type>();
    let ymax = arrays[3].as_primitive::<Float64Type>();
    let srid = arrays.get(4).map(|array| {
        array
            .as_primitive::<arrow_array::types::Int64Type>()
            .clone()
    });
    let _ = resolve_srid_argument(&args, 4, "ST_MakeEnvelope")?;
    let typ = args.return_field.extension_type::<PolygonType>();
    let mut builder = PolygonBuilder::new(typ);

    for row in 0..xmin.len() {
        if xmin.is_null(row) || ymin.is_null(row) || xmax.is_null(row) || ymax.is_null(row) {
            builder.push_polygon(Option::<&Polygon<f64>>::None)?;
            continue;
        }
        if srid.as_ref().is_some_and(|srid| srid.is_null(row)) {
            builder.push_polygon(Option::<&Polygon<f64>>::None)?;
            continue;
        }

        let polygon = envelope_polygon(
            xmin.value(row),
            ymin.value(row),
            xmax.value(row),
            ymax.value(row),
        );
        builder.push_polygon(Some(&polygon))?;
    }

    Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
}

fn envelope_polygon(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> Polygon<f64> {
    let coords = vec![
        WktCoord {
            x: xmin,
            y: ymin,
            z: None,
            m: None,
        },
        WktCoord {
            x: xmin,
            y: ymax,
            z: None,
            m: None,
        },
        WktCoord {
            x: xmax,
            y: ymax,
            z: None,
            m: None,
        },
        WktCoord {
            x: xmax,
            y: ymin,
            z: None,
            m: None,
        },
        WktCoord {
            x: xmin,
            y: ymin,
            z: None,
            m: None,
        },
    ];
    let ring = LineString::new(coords, WktDimension::XY);
    Polygon::new(vec![ring], WktDimension::XY)
}

pub(super) fn envelope_ring(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> Vec<CoordValue> {
    vec![
        CoordValue {
            x: xmin,
            y: ymin,
            z: None,
            m: None,
        },
        CoordValue {
            x: xmin,
            y: ymax,
            z: None,
            m: None,
        },
        CoordValue {
            x: xmax,
            y: ymax,
            z: None,
            m: None,
        },
        CoordValue {
            x: xmax,
            y: ymin,
            z: None,
            m: None,
        },
        CoordValue {
            x: xmin,
            y: ymin,
            z: None,
            m: None,
        },
    ]
}
