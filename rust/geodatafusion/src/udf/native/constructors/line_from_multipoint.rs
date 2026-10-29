//! MultiPoint to LineString constructor

use std::sync::{Arc, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{CoordTrait, Dimensions, GeometryTrait, MultiPointTrait, PointTrait};
use geoarrow_array::array::{GeometryArray, LineStringArray, MultiPointArray, from_arrow_array};
use geoarrow_array::builder::{GeometryBuilder, LineStringBuilder};
use geoarrow_array::capacity::LineStringCapacity;
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, IntoArrow, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, LineStringType};
use wkt::Wkt;
use wkt::types::{Coord, Dimension as WktDimension, LineString as WktLineString};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineFromMultiPoint {
    coord_type: CoordType,
}

impl LineFromMultiPoint {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for LineFromMultiPoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineFromMultiPoint {
    fn name(&self) -> &str {
        "st_linefrommultipoint"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_from_multi_point_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a LineString from the points of a MultiPoint.",
                "ST_LineFromMultiPoint(ST_GeomFromText('MULTIPOINT((0 0),(1 1),(2 2))'))",
            )
            .with_argument("g1", "geometry")
            .with_related_udf("st_makeline")
            .with_related_udf("st_linefromtext")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    // Untyped NULL may be coerced to geometry storage without extension metadata.
    let Ok(input_type) = GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref()) else {
        let output_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
        return Ok(Arc::new(output_type.to_field("", true)));
    };
    match input_type {
        GeoArrowType::MultiPoint(typ) => {
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

fn line_from_multi_point_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    // Match the generic field emitted for untyped NULL when every row is null.
    if arrays[0].null_count() == arrays[0].len()
        && args
            .return_field
            .try_extension_type::<GeometryType>()
            .is_ok()
    {
        let output = create_null_geometry_array(&args.return_field, args.number_rows)?;
        return Ok(output.into_array_ref().into());
    }
    GeoArrowType::from_arrow_field(args.arg_fields[0].as_ref())?;
    let geo_array = from_arrow_array(&arrays[0], &args.arg_fields[0])?;
    let geo_array_ref = geo_array.as_ref();

    let out: Arc<dyn GeoArrowArray> = match geo_array.data_type() {
        GeoArrowType::MultiPoint(_) => {
            Arc::new(impl_fixed_dim(geo_array.as_multi_point(), coord_type)?)
        }
        _ => Arc::new(downcast_geoarrow_array!(
            geo_array_ref,
            impl_variable_dim,
            coord_type
        )?),
    };

    Ok(out.into_array_ref().into())
}

fn create_null_geometry_array(
    return_field: &FieldRef,
    number_rows: usize,
) -> GeoDataFusionResult<GeometryArray> {
    let output_type = return_field.extension_type::<GeometryType>();
    let mut builder = GeometryBuilder::new(output_type);
    for _ in 0..number_rows {
        builder.push_null();
    }
    Ok(builder.finish())
}

fn impl_fixed_dim(
    array: &MultiPointArray,
    coord_type: CoordType,
) -> GeoDataFusionResult<LineStringArray> {
    let input_type = array.extension_type();
    let output_type = LineStringType::new(input_type.dimension(), input_type.metadata().clone())
        .with_coord_type(coord_type);
    let mut builder = LineStringBuilder::with_capacity(
        output_type,
        LineStringCapacity::new(array.buffer_lengths().coord_capacity(), array.len()),
    );
    let dimension = wkt_dimension(input_type.dimension());

    for value in array.iter() {
        let Some(multi_point) = value else {
            builder.push_line_string(None::<&WktLineString>)?;
            continue;
        };
        let line = multi_point_to_line(&multi_point?, dimension)?;
        builder.push_line_string(Some(&line))?;
    }

    Ok(builder.finish())
}

fn impl_variable_dim<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    coord_type: CoordType,
) -> GeoDataFusionResult<GeometryArray> {
    let output_type =
        GeometryType::new(array.data_type().metadata().clone()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(output_type);

    for value in array.iter() {
        let Some(geometry) = value else {
            builder.push_null();
            continue;
        };
        let line = geometry_to_line(&geometry?)?;
        builder.push_geometry(Some(&line))?;
    }

    Ok(builder.finish())
}

fn geometry_to_line(geometry: &impl GeometryTrait<T = f64>) -> GeoDataFusionResult<Wkt<f64>> {
    match geometry.as_type() {
        geo_traits::GeometryType::MultiPoint(multi_point) => {
            let dimension = dimension_from_traits(multi_point.dim())?;
            let line = multi_point_to_line(multi_point, dimension)?;
            Ok(Wkt::LineString(line))
        }
        geometry_type => {
            let name = match geometry_type {
                geo_traits::GeometryType::Point(_) => "POINT",
                geo_traits::GeometryType::LineString(_) => "LINESTRING",
                geo_traits::GeometryType::Polygon(_) => "POLYGON",
                geo_traits::GeometryType::MultiLineString(_) => "MULTILINESTRING",
                geo_traits::GeometryType::MultiPolygon(_) => "MULTIPOLYGON",
                geo_traits::GeometryType::GeometryCollection(_) => "GEOMETRYCOLLECTION",
                geo_traits::GeometryType::Rect(_) => "RECT",
                geo_traits::GeometryType::Triangle(_) => "TRIANGLE",
                geo_traits::GeometryType::Line(_) => "LINE",
                geo_traits::GeometryType::MultiPoint(_) => unreachable!(),
            };
            Err(DataFusionError::Execution(format!(
                "ST_LineFromMultiPoint requires a MULTIPOINT input, got {name}"
            ))
            .into())
        }
    }
}

fn multi_point_to_line(
    multi_point: &impl MultiPointTrait<T = f64>,
    dimension: WktDimension,
) -> GeoDataFusionResult<WktLineString> {
    let expected_dimensions = Dimensions::from(dimension);
    let mut coords = Vec::with_capacity(multi_point.num_points());

    for point in multi_point.points() {
        let Some(coord) = point.coord() else {
            return Err(DataFusionError::Execution(
                "ST_LineFromMultiPoint does not support MULTIPOINT values containing EMPTY points"
                    .to_string(),
            )
            .into());
        };
        if coord.dim().size() != expected_dimensions.size() {
            return Err(DataFusionError::Execution(
                "ST_LineFromMultiPoint requires all points to have the same coordinate dimension"
                    .to_string(),
            )
            .into());
        }

        let z = match dimension {
            WktDimension::XYZ | WktDimension::XYZM => Some(coord.nth_or_panic(2)),
            WktDimension::XY | WktDimension::XYM => None,
        };
        let m = match dimension {
            WktDimension::XYM => Some(coord.nth_or_panic(2)),
            WktDimension::XYZM => Some(coord.nth_or_panic(3)),
            WktDimension::XY | WktDimension::XYZ => None,
        };
        coords.push(Coord {
            x: coord.x(),
            y: coord.y(),
            z,
            m,
        });
    }

    Ok(WktLineString::new(coords, dimension))
}

fn dimension_from_traits(dimensions: Dimensions) -> GeoDataFusionResult<WktDimension> {
    match dimensions {
        Dimensions::Xy => Ok(WktDimension::XY),
        Dimensions::Xyz => Ok(WktDimension::XYZ),
        Dimensions::Xym => Ok(WktDimension::XYM),
        Dimensions::Xyzm => Ok(WktDimension::XYZM),
        Dimensions::Unknown(size) => Err(DataFusionError::Execution(format!(
            "ST_LineFromMultiPoint does not support {size}-dimensional coordinates"
        ))
        .into()),
    }
}

fn wkt_dimension(dimension: geoarrow_schema::Dimension) -> WktDimension {
    match dimension {
        geoarrow_schema::Dimension::XY => WktDimension::XY,
        geoarrow_schema::Dimension::XYZ => WktDimension::XYZ,
        geoarrow_schema::Dimension::XYM => WktDimension::XYM,
        geoarrow_schema::Dimension::XYZM => WktDimension::XYZM,
    }
}
