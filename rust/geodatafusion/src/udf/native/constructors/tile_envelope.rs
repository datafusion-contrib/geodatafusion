use std::sync::{Arc, OnceLock};

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_schema::{CoordType, Dimension, PolygonType};
use wkt::types::Polygon;

use super::envelope::envelope_ring;
use super::util::{CoordValue, wkt_dimension, wkt_line_string};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::metadata_for_srid;

const WEB_MERCATOR_HALF_WORLD: f64 = 20_037_508.342_789_244;
const WEB_MERCATOR_SRID: i32 = 3857;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct TileEnvelope {
    signature: Signature,
    coord_type: CoordType,
}

impl TileEnvelope {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Int64]),
                    TypeSignature::Exact(vec![
                        DataType::Int64,
                        DataType::Int64,
                        DataType::Int64,
                        DataType::Float64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for TileEnvelope {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static TILE_ENVELOPE_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for TileEnvelope {
    fn name(&self) -> &str {
        "st_tileenvelope"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from EPSG:3857 metadata.".to_string(),
        ))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        let typ = PolygonType::new(Dimension::XY, metadata_for_srid(WEB_MERCATOR_SRID))
            .with_coord_type(self.coord_type);
        Ok(Arc::new(typ.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(tile_envelope_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(TILE_ENVELOPE_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates an XYZ tile Polygon in EPSG:3857. The optional fourth argument is a fractional margin \
                 (0.125 expands by 12.5%; values below -0.5 are rejected). The PostGIS custom-bounds overload is not provided.",
                "ST_TileEnvelope(zoom, x, y[, margin])",
            )
            .with_argument("zoom", "tile zoom in the inclusive range 0..31")
            .with_argument("x", "tile X index at the given zoom")
            .with_argument("y", "tile Y index at the given zoom")
            .with_argument("margin", "optional finite fractional tile margin")
            .with_related_udf("ST_MakeEnvelope")
            .build()
        }))
    }
}

fn tile_envelope_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
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
    let zoom = arrays[0].as_primitive::<Int64Type>();
    let x = arrays[1].as_primitive::<Int64Type>();
    let y = arrays[2].as_primitive::<Int64Type>();
    let margin = arrays
        .get(3)
        .map(|array| array.as_primitive::<Float64Type>().clone());
    let typ = args.return_field.extension_type::<PolygonType>();
    let mut builder = PolygonBuilder::new(typ);

    for row in 0..zoom.len() {
        if zoom.is_null(row)
            || x.is_null(row)
            || y.is_null(row)
            || margin.as_ref().is_some_and(|array| array.is_null(row))
        {
            builder.push_polygon(Option::<&Polygon<f64>>::None)?;
            continue;
        }

        let ring = tile_envelope_ring(
            zoom.value(row),
            x.value(row),
            y.value(row),
            margin.as_ref().map_or(0.0, |array| array.value(row)),
        )?;
        let wkt_ring = wkt_line_string(ring, Dimension::XY);
        let polygon = Polygon::new(vec![wkt_ring], wkt_dimension(Dimension::XY));
        builder.push_polygon(Some(&polygon))?;
    }

    Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
}

pub(super) fn tile_envelope_ring(
    zoom: i64,
    x: i64,
    y: i64,
    margin: f64,
) -> GeoDataFusionResult<Vec<CoordValue>> {
    if !(0..32).contains(&zoom) {
        return Err(DataFusionError::Execution(format!(
            "ST_TileEnvelope: invalid tile zoom value {zoom}"
        ))
        .into());
    }
    let world_tile_size = 1_i64 << zoom;
    if !(0..world_tile_size).contains(&x) {
        return Err(DataFusionError::Execution(format!(
            "ST_TileEnvelope: invalid tile x value {x}"
        ))
        .into());
    }
    if !(0..world_tile_size).contains(&y) {
        return Err(DataFusionError::Execution(format!(
            "ST_TileEnvelope: invalid tile y value {y}"
        ))
        .into());
    }
    if !margin.is_finite() {
        return Err(DataFusionError::Execution(
            "ST_TileEnvelope: margin must be finite".to_string(),
        )
        .into());
    }
    if margin < -0.5 {
        return Err(DataFusionError::Execution(format!(
            "ST_TileEnvelope: margin must not be less than -0.5, got {margin}"
        ))
        .into());
    }

    let tile_geo_size = (WEB_MERCATOR_HALF_WORLD * 2.0) / world_tile_size as f64;
    let (x1, x2) = if 1.0 + margin * 2.0 > world_tile_size as f64 {
        (-WEB_MERCATOR_HALF_WORLD, WEB_MERCATOR_HALF_WORLD)
    } else {
        (
            -WEB_MERCATOR_HALF_WORLD + tile_geo_size * (x as f64 - margin),
            -WEB_MERCATOR_HALF_WORLD + tile_geo_size * (x as f64 + 1.0 + margin),
        )
    };
    let mut y1 = WEB_MERCATOR_HALF_WORLD - tile_geo_size * (y as f64 + 1.0 + margin);
    let mut y2 = WEB_MERCATOR_HALF_WORLD - tile_geo_size * (y as f64 - margin);
    let mut x1 = x1;
    let mut x2 = x2;

    y1 = y1.max(-WEB_MERCATOR_HALF_WORLD);
    y2 = y2.min(WEB_MERCATOR_HALF_WORLD);
    x1 = x1.max(-WEB_MERCATOR_HALF_WORLD);
    x2 = x2.min(WEB_MERCATOR_HALF_WORLD);

    Ok(envelope_ring(x1, y1, x2, y2))
}
