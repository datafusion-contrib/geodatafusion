//! `ST_AsMVTGeom` implementation.

use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::extension::ExtensionType;
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{CoordTrait, PointTrait};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::{BoxType, CoordType, GeoArrowType, GeometryType};
use geos::{Geom, Geometry};
use wkb::reader::Wkb;

use crate::error::GeoDataFusionResult;
use crate::udf::geos::adapter::{GeosWkbWriter, prepare_geos_wkb};
use crate::udf::native::accessors::is_empty::is_geometry_topologically_empty;
use crate::udf::native::bounding_box::util::bounds::total_bounds;
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata_or_default, geometry_array_from_arrow,
    is_null_geometry_input,
};
use crate::udf::native::transformation::geometry::OwnedGeometry;

const DEFAULT_EXTENT: i64 = 4096;
const DEFAULT_BUFFER: i64 = 256;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AsMvtGeom {
    signature: Signature,
    coord_type: CoordType,
}

impl AsMvtGeom {
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            coord_type,
        }
    }
}

impl Default for AsMvtGeom {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for AsMvtGeom {
    fn name(&self) -> &str {
        "st_asmvtgeom"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=5).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(format!(
                "ST_AsMVTGeom expects between 2 and 5 arguments, got {}",
                arg_types.len()
            )));
        }

        let mut coerced = Vec::with_capacity(arg_types.len());
        for (index, data_type) in arg_types.iter().enumerate() {
            let target = match index {
                0 => {
                    if !is_geometry_or_box(data_type) && data_type != &DataType::Null {
                        return Err(DataFusionError::Plan(format!(
                            "ST_AsMVTGeom argument 1 must be a geometry, got {data_type}"
                        )));
                    }
                    data_type.clone()
                }
                1 => {
                    if !is_box2d_type(data_type)
                        && !is_geometry_or_box(data_type)
                        && data_type != &DataType::Null
                    {
                        return Err(DataFusionError::Plan(format!(
                            "ST_AsMVTGeom argument 2 must be a box2d or geometry, got {data_type}"
                        )));
                    }
                    data_type.clone()
                }
                2 | 3 => {
                    if data_type != &DataType::Null && !data_type.is_integer() {
                        return Err(DataFusionError::Plan(format!(
                            "ST_AsMVTGeom argument {} must be an integer, got {data_type}",
                            index + 1
                        )));
                    }
                    DataType::Int64
                }
                4 => {
                    if !matches!(data_type, DataType::Null | DataType::Boolean) {
                        return Err(DataFusionError::Plan(format!(
                            "ST_AsMVTGeom clip_geom must be Boolean, got {data_type}"
                        )));
                    }
                    DataType::Boolean
                }
                _ => unreachable!(),
            };
            coerced.push(target);
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_AsMVTGeom return type needs input metadata".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let metadata = field_metadata_or_default(args.arg_fields[0].as_ref())?;
        let output_type = GeometryType::new(metadata).with_coord_type(self.coord_type);
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(transform_geometry(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Transforms a geometry into MVT tile coordinates. The optional extent \
                 (default 4096), buffer (default 256), and clip_geom (default true) \
                 arguments match PostGIS. A box2d or geometry may provide the bounds; \
                 geometry bounds are converted to their 2D envelope. Z and M \
                 ordinates are preserved when the input is not clipped; GEOS clipping may \
                 interpolate Z/M values differently from PostGIS, which MVT encoding ignores. GeometryCollection \
                 inputs are reduced to the highest-dimensional geometry family, matching \
                 the MVT feature model.",
                "ST_AsMVTGeom(geom, bounds[, extent[, buffer[, clip_geom]]])",
            )
            .with_argument("geom", "geometry")
            .with_argument("bounds", "box2d or geometry")
            .with_argument("extent", "integer tile extent")
            .with_argument("buffer", "integer tile buffer")
            .with_argument("clip_geom", "whether to clip the geometry")
            .build()
        }))
    }
}

fn transform_geometry(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geometry_field = &args.arg_fields[0];
    let bounds_field = &args.arg_fields[1];
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    let GeoArrowType::Geometry(_) = output_type else {
        return Err(DataFusionError::Internal(
            "ST_AsMVTGeom return field is not a generic Geometry".to_string(),
        )
        .into());
    };
    let mut builder = StrictGeometryBuilder::new(
        GeometryType::new(field_metadata_or_default(geometry_field.as_ref())?)
            .with_coord_type(coord_type),
    );
    let row_count = args.number_rows;

    if is_null_geometry_input(geometry_field.as_ref(), arrays[0].as_ref()) {
        for _ in 0..row_count {
            builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
        }
        return Ok(ColumnarValue::Array(builder.finish().into_array_ref()));
    }

    let geometry_array = geometry_array_from_arrow(arrays[0].as_ref(), geometry_field.as_ref())?;
    let geometry_wkb = to_wkb::<i32>(geometry_array.as_ref())?;
    let geometry_wkb = geometry_wkb.inner();

    let bounds_array = arrays.get(1);
    let bounds_geometry = match bounds_array {
        Some(bounds_array) if !is_box2d_field(bounds_field.as_ref()) => Some(
            geometry_array_from_arrow(bounds_array.as_ref(), bounds_field.as_ref())?,
        ),
        _ => None,
    };

    let extent_array = arrays.get(2);
    let buffer_array = arrays.get(3);
    let clip_array = arrays.get(4);

    for row in 0..row_count {
        if geometry_wkb.is_null(row) {
            builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            continue;
        }

        let bounds = bounds_at(
            bounds_array,
            bounds_field.as_ref(),
            bounds_geometry.as_deref(),
            row,
        )?;
        let extent = int64_at(extent_array, row)?.unwrap_or(DEFAULT_EXTENT);
        let buffer = int64_at(buffer_array, row)?.unwrap_or(DEFAULT_BUFFER);
        let clip = bool_at(clip_array, row)?.unwrap_or(true);
        validate_bounds(bounds)?;
        if extent <= 0 {
            return Err(DataFusionError::Execution(
                "ST_AsMVTGeom: Extent must be greater than 0".to_string(),
            )
            .into());
        }

        let result = clip_and_transform(geometry_wkb.value(row), bounds, extent, buffer, clip)?;
        builder.push_geometry(result.as_ref())?;
    }

    Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
}

fn clip_and_transform(
    wkb: &[u8],
    bounds: [f64; 4],
    extent: i64,
    buffer: i64,
    clip: bool,
) -> GeoDataFusionResult<Option<OwnedGeometry>> {
    let geometry = Wkb::try_new(wkb).map_err(|error| {
        DataFusionError::Execution(format!("ST_AsMVTGeom: invalid geometry WKB: {error}"))
    })?;
    if is_geometry_topologically_empty(&geometry) || is_empty_nan_point(&geometry) {
        return Ok(None);
    }
    let Some(family) = OwnedGeometry::mvt_flatten(&geometry) else {
        return Ok(None);
    };

    if !clip {
        return Ok(Some(family.mvt_transform(bounds, extent as f64)));
    }

    let width = bounds[2] - bounds[0];
    let height = bounds[3] - bounds[1];
    let buffer = buffer.max(0) as f64;
    let expanded = [
        bounds[0] - buffer * width / extent as f64,
        bounds[1] - buffer * height / extent as f64,
        bounds[2] + buffer * width / extent as f64,
        bounds[3] + buffer * height / extent as f64,
    ];

    if matches!(
        family,
        OwnedGeometry::Point(_) | OwnedGeometry::MultiPoint(_)
    ) {
        return Ok(family
            .mvt_filter_points(expanded)
            .map(|points| points.mvt_transform(bounds, extent as f64)));
    }

    if family.mvt_bounds().is_some_and(|geometry_bounds| {
        geometry_bounds[0] >= expanded[0]
            && geometry_bounds[1] >= expanded[1]
            && geometry_bounds[2] <= expanded[2]
            && geometry_bounds[3] <= expanded[3]
    }) {
        return Ok(Some(family.mvt_transform(bounds, extent as f64)));
    }

    let mut family_wkb = Vec::new();
    wkb::writer::write_geometry(&mut family_wkb, &family, &Default::default()).map_err(
        |error| {
            DataFusionError::Execution(format!("ST_AsMVTGeom: cannot encode geometry: {error}"))
        },
    )?;
    let prepared = prepare_geos_wkb(&family_wkb)?;
    let input = Geometry::new_from_wkb(prepared.as_ref())?;
    let clipped = input.clip_by_rect(expanded[0], expanded[1], expanded[2], expanded[3])?;
    if clipped.is_empty()? {
        return Ok(None);
    }

    let mut writer = GeosWkbWriter::new()?;
    let clipped_wkb = writer.write(&clipped)?;
    let clipped = Wkb::try_new(&clipped_wkb).map_err(|error| {
        DataFusionError::Execution(format!("ST_AsMVTGeom: invalid clipped geometry: {error}"))
    })?;
    Ok(OwnedGeometry::mvt_flatten(&clipped)
        .map(|geometry| geometry.mvt_transform(bounds, extent as f64)))
}

fn is_empty_nan_point(geometry: &impl geo_traits::GeometryTrait<T = f64>) -> bool {
    match geometry.as_type() {
        geo_traits::GeometryType::Point(point) => point
            .coord()
            .is_some_and(|coord| coord.x().is_nan() && coord.y().is_nan()),
        _ => false,
    }
}

fn bounds_at(
    bounds_array: Option<&ArrayRef>,
    bounds_field: &Field,
    bounds_geometry: Option<&dyn GeoArrowArray>,
    row: usize,
) -> GeoDataFusionResult<[f64; 4]> {
    let bounds_array = bounds_array.ok_or_else(|| {
        DataFusionError::Execution("ST_AsMVTGeom: Geometric bounds cannot be null".to_string())
    })?;
    if bounds_field.data_type() == &DataType::Null || bounds_array.is_null(row) {
        return Err(DataFusionError::Execution(
            "ST_AsMVTGeom: Geometric bounds cannot be null".to_string(),
        )
        .into());
    }

    if is_box2d_field(bounds_field) {
        let values = bounds_array.as_struct();
        let DataType::Struct(fields) = bounds_field.data_type() else {
            unreachable!("box2d field is a struct");
        };
        let mut ordinates = [f64::NAN; 4];
        for (index, name) in ["xmin", "ymin", "xmax", "ymax"].iter().enumerate() {
            let field_index = fields
                .iter()
                .position(|field| field.name() == name)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("ST_AsMVTGeom: box2d is missing `{name}`"))
                })?;
            let value = values
                .column(field_index)
                .as_primitive::<Float64Type>()
                .value(row);
            ordinates[index] = value;
        }
        return Ok(ordinates);
    }

    let Some(bounds_geometry) = bounds_geometry else {
        return Err(DataFusionError::Execution(
            "ST_AsMVTGeom: invalid bounds argument".to_string(),
        )
        .into());
    };
    let bounds = bounds_geometry.slice(row, 1);
    let bounds = total_bounds(bounds.as_ref())?;
    Ok([bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()])
}

fn int64_at(array: Option<&ArrayRef>, row: usize) -> Result<Option<i64>> {
    let Some(array) = array else {
        return Ok(None);
    };
    if array.data_type() == &DataType::Null || array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(array.as_primitive::<Int64Type>().value(row)))
}

fn bool_at(array: Option<&ArrayRef>, row: usize) -> Result<Option<bool>> {
    let Some(array) = array else {
        return Ok(None);
    };
    if array.data_type() == &DataType::Null || array.is_null(row) {
        return Ok(None);
    }
    Ok(Some(array.as_boolean().value(row)))
}

fn validate_bounds(bounds: [f64; 4]) -> Result<()> {
    if !bounds.iter().all(|value| value.is_finite())
        || bounds[2] - bounds[0] <= 0.0
        || bounds[3] - bounds[1] <= 0.0
    {
        return Err(DataFusionError::Execution(
            "ST_AsMVTGeom: Geometric bounds are too small".to_string(),
        ));
    }
    Ok(())
}

fn is_box2d_field(field: &Field) -> bool {
    field.extension_type_name() == Some(BoxType::NAME) || is_box2d_type(field.data_type())
}

fn is_box2d_type(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    if fields.len() != 4 {
        return false;
    }
    ["xmin", "ymin", "xmax", "ymax"]
        .iter()
        .zip(fields.iter())
        .all(|(expected, field)| {
            field.name() == expected && field.data_type() == &DataType::Float64
        })
}

fn is_geometry_or_box(data_type: &DataType) -> bool {
    is_box2d_type(data_type) || crate::data_types::any_geometry_type().contains(data_type)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn box_type_shape_is_recognized() {
        let data_type =
            BoxType::new(geoarrow_schema::Dimension::XY, Default::default()).data_type();
        assert!(is_box2d_type(&data_type));
    }
}
