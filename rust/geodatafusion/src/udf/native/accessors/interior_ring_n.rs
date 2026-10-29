//! Indexed accessors from Polygon geometries.

use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::Int64Array;
use arrow_schema::{DataType, FieldRef};
use datafusion::common::types::{NativeType, logical_int64};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};
use geo_traits::{CoordTrait, GeometryTrait, LineStringTrait, PolygonTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray, PolygonArray, from_arrow_array};
use geoarrow_array::builder::{GeometryBuilder, LineStringBuilder};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, Dimension, GeoArrowType, GeometryType, LineStringType};

use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct InteriorRingN {
    coord_type: CoordType,
}

impl InteriorRingN {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for InteriorRingN {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::coercible(
        vec![
            Coercion::new_exact(TypeSignatureClass::Any),
            Coercion::new_implicit(
                TypeSignatureClass::Native(logical_int64()),
                vec![TypeSignatureClass::Integer],
                NativeType::Int64,
            ),
        ],
        Volatility::Immutable,
    )
});
static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for InteriorRingN {
    fn name(&self) -> &str {
        "st_interiorringn"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from metadata in return_field_from_args.".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(interior_ring_n_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns the Nth interior ring of a POLYGON as a LINESTRING. The index is 1-based. Returns NULL for non-Polygon geometries and non-positive or out-of-range indices.",
                "ST_InteriorRingN(polygon, n)",
            )
            .with_argument("polygon", "geometry")
            .with_argument("n", "1-based ring index")
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

fn interior_ring_n_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays[0].data_type() == &DataType::Null {
        return Ok(null_geometry_array(args.number_rows, coord_type)?
            .into_array_ref()
            .into());
    }

    let indices = arrays[1]
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "ST_InteriorRingN expected an Int64 index array, got {}",
                arrays[1].data_type()
            ))
        })?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();

    let out: Arc<dyn GeoArrowArray> = match geo_array.data_type() {
        GeoArrowType::Polygon(_) => {
            Arc::new(polygon_impl(geo_array.as_polygon(), indices, coord_type)?)
        }
        _ => Arc::new(downcast_geoarrow_array!(
            geo_array_ref,
            geometry_impl,
            indices,
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
    indices: &Int64Array,
    coord_type: CoordType,
) -> GeoDataFusionResult<LineStringArray> {
    let output_type = LineStringType::new(
        array.extension_type().dimension(),
        array.extension_type().metadata().clone(),
    )
    .with_coord_type(coord_type);
    let mut builder = LineStringBuilder::new(output_type);

    for (item, index) in array.iter().zip(indices.iter()) {
        let ring = match item.transpose()? {
            Some(polygon) => index.and_then(|index| selected_interior_ring(&polygon, index)),
            None => None,
        };
        builder.push_line_string(ring.as_ref())?;
    }

    Ok(builder.finish())
}

fn geometry_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    indices: &Int64Array,
    coord_type: CoordType,
) -> GeoDataFusionResult<GeometryArray> {
    let output_type =
        GeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(output_type);

    for (item, index) in array.iter().zip(indices.iter()) {
        let ring = match item.transpose()? {
            Some(geometry) => match geometry.as_type() {
                geo_traits::GeometryType::Polygon(polygon) => index
                    .and_then(|index| selected_interior_ring(polygon, index))
                    .map(|ring| line_string_to_wkt(&ring))
                    .transpose()?,
                _ => None,
            },
            None => None,
        };
        builder.push_geometry(ring.as_ref())?;
    }

    Ok(builder.finish())
}

fn selected_interior_ring<'a, P>(polygon: &'a P, index: i64) -> Option<P::RingType<'a>>
where
    P: PolygonTrait<T = f64>,
{
    if index <= 0 {
        return None;
    }

    let zero_based = usize::try_from(index.checked_sub(1)?).ok()?;
    polygon.interior(zero_based)
}

fn line_string_to_wkt(
    line_string: &impl LineStringTrait<T = f64>,
) -> GeoDataFusionResult<wkt::types::LineString<f64>> {
    let dimension = Dimension::try_from(line_string.dim())?;
    Ok(wkt::types::LineString::new(
        line_string
            .coords()
            .map(|coord| coord_to_wkt(&coord, dimension))
            .collect(),
        wkt_dimension(dimension),
    ))
}

fn coord_to_wkt(coord: &impl CoordTrait<T = f64>, dimension: Dimension) -> wkt::types::Coord<f64> {
    let (z, m) = match dimension {
        Dimension::XY => (None, None),
        Dimension::XYZ => (coord.nth(2), None),
        Dimension::XYM => (None, coord.nth(2)),
        Dimension::XYZM => (coord.nth(2), coord.nth(3)),
    };
    wkt::types::Coord {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    }
}

fn wkt_dimension(dimension: Dimension) -> wkt::types::Dimension {
    match dimension {
        Dimension::XY => wkt::types::Dimension::XY,
        Dimension::XYZ => wkt::types::Dimension::XYZ,
        Dimension::XYM => wkt::types::Dimension::XYM,
        Dimension::XYZM => wkt::types::Dimension::XYZM,
    }
}
