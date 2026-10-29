//! Accessors from Polygon geometries.

use std::sync::{Arc, LazyLock, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait, PolygonTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray, PolygonArray, from_arrow_array};
use geoarrow_array::builder::{GeometryBuilder, LineStringBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::scalar::LineString as LineStringScalar;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, LineStringType};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ExteriorRing {
    coord_type: CoordType,
}

impl ExteriorRing {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for ExteriorRing {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static EXTERIOR_RING_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();
static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    let mut valid_types = any_geometry_type();
    valid_types.push(DataType::Null);
    Signature::uniform(1, valid_types, Volatility::Immutable)
});

impl ScalarUDFImpl for ExteriorRing {
    fn name(&self) -> &str {
        "st_exteriorring"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(exterior_ring_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(EXTERIOR_RING_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the exterior ring of a POLYGON as a LINESTRING. Returns NULL for non-Polygon geometries.",
                "ST_ExteriorRing(polygon)",
            )
            .with_argument("polygon", "geometry")
            .build()
        }))
    }
}

/// For Polygon input, return a fixed-dimension LineString output.
/// For other geometry input, return a variable-dimension Geometry output.
fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    if args.arg_fields[0].data_type() == &DataType::Null {
        return Ok(null_geometry_output_type(coord_type));
    }

    let input_type = GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref())?;
    match input_type {
        GeoArrowType::Polygon(typ) => {
            let output_type = LineStringType::new(typ.dimension(), typ.metadata().clone())
                .with_coord_type(coord_type);
            Ok(Arc::new(output_type.to_field("", true)))
        }
        _ => {
            let output_type =
                GeometryType::new(input_type.metadata().clone()).with_coord_type(coord_type);
            Ok(Arc::new(output_type.to_field("", true)))
        }
    }
}

fn exterior_ring_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        return Ok(null_geometry_array(args.number_rows, coord_type)?
            .into_array_ref()
            .into());
    }

    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();

    let out: Arc<dyn GeoArrowArray> = match geo_array.data_type() {
        GeoArrowType::Polygon(_) => Arc::new(polygon_impl(geo_array.as_polygon(), coord_type)?),
        _ => Arc::new(downcast_geoarrow_array!(
            geo_array_ref,
            geometry_impl,
            coord_type
        )?),
    };

    Ok(out.into_array_ref().into())
}

fn null_geometry_output_type(coord_type: CoordType) -> FieldRef {
    Arc::new(
        GeometryType::new(Default::default())
            .with_coord_type(coord_type)
            .to_field("", true),
    )
}

fn null_geometry_array(
    number_rows: usize,
    coord_type: CoordType,
) -> GeoDataFusionResult<GeometryArray> {
    let output_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(output_type);
    for _ in 0..number_rows {
        builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
    }
    Ok(builder.finish())
}

fn polygon_impl(
    array: &PolygonArray,
    coord_type: CoordType,
) -> GeoDataFusionResult<LineStringArray> {
    let output_type = LineStringType::new(
        array.extension_type().dimension(),
        array.extension_type().metadata().clone(),
    )
    .with_coord_type(coord_type);
    let mut builder = LineStringBuilder::new(output_type);

    for item in array.iter() {
        match item.transpose()? {
            Some(polygon) => match polygon.exterior() {
                Some(ring) => builder.push_line_string(Some(&ring))?,
                None => builder.push_empty(),
            },
            None => {
                let null_ring: Option<&LineStringScalar<'_>> = None;
                builder.push_line_string(null_ring)?;
            }
        }
    }

    Ok(builder.finish())
}

fn geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoDataFusionResult<GeometryArray> {
    let output_type =
        GeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(output_type);

    for item in array.iter() {
        let ring = item
            .transpose()?
            .and_then(|geometry| match geometry.as_type() {
                geo_traits::GeometryType::Polygon(polygon) => Some(exterior_ring_as_wkt(polygon)),
                _ => None,
            });
        builder.push_geometry(ring.as_ref())?;
    }

    Ok(builder.finish())
}

fn exterior_ring_as_wkt(polygon: &impl PolygonTrait<T = f64>) -> wkt::types::LineString<f64> {
    let dimension = wkt_dimension(polygon.dim());
    match polygon.exterior() {
        Some(ring) => wkt::types::LineString::new(
            ring.coords().map(|coord| coord_to_wkt(&coord)).collect(),
            dimension,
        ),
        None => wkt::types::LineString::empty(dimension),
    }
}

fn coord_to_wkt(coord: &impl CoordTrait<T = f64>) -> wkt::types::Coord<f64> {
    let (z, m) = match coord.dim() {
        geo_traits::Dimensions::Xy => (None, None),
        geo_traits::Dimensions::Xyz => (coord.nth(2), None),
        geo_traits::Dimensions::Xym => (None, coord.nth(2)),
        geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(_) => {
            (coord.nth(2), coord.nth(3))
        }
    };
    wkt::types::Coord {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    }
}

fn wkt_dimension(dimension: geo_traits::Dimensions) -> wkt::types::Dimension {
    match dimension {
        geo_traits::Dimensions::Xy => wkt::types::Dimension::XY,
        geo_traits::Dimensions::Xyz => wkt::types::Dimension::XYZ,
        geo_traits::Dimensions::Xym => wkt::types::Dimension::XYM,
        geo_traits::Dimensions::Xyzm | geo_traits::Dimensions::Unknown(_) => {
            wkt::types::Dimension::XYZM
        }
    }
}
