use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{CoordTrait, RectTrait};
use geoarrow_array::array::RectArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{BoxType, Dimension};
use geos::{Geom, GeometryTypes};

use super::super::{adapter, m_dimension};
use super::clip_elevation::ClipElevation;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct ClipByBox2D {
    signature: Signature,
}

impl ClipByBox2D {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for ClipByBox2D {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for ClipByBox2D {
    fn name(&self) -> &str {
        "st_clipbybox2d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        ensure_box_argument(args.arg_fields, self.name())?;
        adapter::geometry_return_field(&args, &[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(clip_by_box_2d_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Clips a geometry to a box2d in the XY plane. Degenerate boxes and fully outside geometries produce NULL or typed empty output as GEOS does. Z ordinates are preserved: clip vertices keep the input Z interpolated along the input linework, and remaining vertices take the elevation of their cell in a three-by-three model over the input extent. M ordinates are dropped before GEOS.",
                "ST_ClipByBox2D(geom, box)",
            )
            .with_argument("geom", "geometry to clip")
            .with_argument("box", "box2d clipping extent")
            .build()
        }))
    }
}

fn ensure_box_argument(fields: &[FieldRef], name: &str) -> GeoDataFusionResult<BoxType> {
    if matches!(fields[1].data_type(), DataType::Null) {
        return Ok(BoxType::new(Dimension::XY, Default::default()));
    }
    fields[1].try_extension_type::<BoxType>().map_err(|error| {
        DataFusionError::Plan(format!(
            "{name} argument 2 must be a box2d, got {}: {error}",
            fields[1].data_type()
        ))
        .into()
    })
}

fn clip_by_box_2d_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    ensure_box_argument(&args.arg_fields, "ST_ClipByBox2D")?;

    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let rects = if matches!(args.arg_fields[1].data_type(), DataType::Null) {
        Vec::new()
    } else {
        let rects = RectArray::try_from((arrays[1].as_ref(), args.arg_fields[1].as_ref()))?;
        (0..rects.len())
            .map(|row| {
                if rects.is_null(row) {
                    Ok(None)
                } else {
                    let rect = rects.value(row)?;
                    Ok(Some((
                        rect.min().x(),
                        rect.min().y(),
                        rect.max().x(),
                        rect.max().y(),
                    )))
                }
            })
            .collect::<GeoDataFusionResult<Vec<_>>>()?
    };

    adapter::unary_geos(args, |row, _arrays, original, geometry, writer| {
        let Some((xmin, ymin, xmax, ymax)) = rects.get(row).copied().flatten() else {
            return Err(DataFusionError::Internal(
                "ST_ClipByBox2D lost a non-null box argument".to_string(),
            )
            .into());
        };
        if xmin == xmax || ymin == ymax {
            return Ok(None);
        }
        if geometry.is_empty()?
            && let Some(empty) = adapter::empty_2d_wkb(geometry.geometry_type()?)
        {
            return Ok(Some(empty.to_vec()));
        }

        let result = geometry.clip_by_rect(xmin, ymin, xmax, ymax)?;
        if result.is_empty()?
            && let Some(empty) = adapter::empty_2d_wkb(geometry.geometry_type()?)
        {
            return Ok(Some(empty.to_vec()));
        }

        let input_has_z = !matches!(
            geometry.get_coordinate_dimension()?,
            geos::CoordDimensions::TwoD
        );
        let wkb = writer.write(&result)?;
        if input_has_z && has_missing_z(&result)? {
            // GEOS 3.11's ClipByRect drops the Z ordinates of every clipped
            // member; restore them the way 3.14 and newer compute them. Z
            // ordinates that survived the clip are kept.
            let elevation = ClipElevation::new(original, xmin, ymin, xmax, ymax)?;
            return Ok(Some(m_dimension::restore_z(&wkb, &|x, y| {
                elevation.z_at(x, y)
            })?));
        }
        Ok(Some(wkb))
    })
}

/// Whether a clip result still contains XY coordinates that need Z ordinates.
///
/// GEOS reports a collection's dimension as the largest member dimension, so a
/// collection with mixed members has to be inspected member by member.
fn has_missing_z(geometry: &impl Geom) -> GeoDataFusionResult<bool> {
    if matches!(
        geometry.get_coordinate_dimension()?,
        geos::CoordDimensions::TwoD
    ) {
        return Ok(true);
    }
    match geometry.geometry_type()? {
        GeometryTypes::GeometryCollection
        | GeometryTypes::MultiPoint
        | GeometryTypes::MultiLineString
        | GeometryTypes::MultiPolygon => {
            for index in 0..geometry.get_num_geometries()? {
                if has_missing_z(&geometry.get_geometry_n(index)?)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => Ok(false),
    }
}
