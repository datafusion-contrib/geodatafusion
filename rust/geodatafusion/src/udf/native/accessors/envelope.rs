use std::sync::{Arc, LazyLock, OnceLock};

use arrow_schema::{DataType, FieldRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo::{Coord, LineString, Point, Polygon};
use geo_traits::{Dimensions, GeometryTrait};
use geoarrow_array::array::{GeometryArray, from_arrow_array};
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType, Metadata};
use wkt::Wkt;
use wkt::types::{
    Dimension as WktDimension, GeometryCollection as WktGeometryCollection,
    LineString as WktLineString, MultiLineString as WktMultiLineString,
    MultiPoint as WktMultiPoint, MultiPolygon as WktMultiPolygon, Point as WktPoint,
    Polygon as WktPolygon,
};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::{GeometryBounds, geometry_bounds};

static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    let mut valid_types = any_geometry_type();
    valid_types.push(DataType::Null);
    Signature::uniform(1, valid_types, Volatility::Immutable)
});

/// `ST_Envelope` scalar UDF implementation: returns the minimum bounding box of a geometry as a geometry.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Envelope {
    coord_type: CoordType,
}

impl Envelope {
    /// Creates a new `ST_Envelope` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Envelope {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Envelope {
    fn name(&self) -> &str {
        "st_envelope"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from input metadata in return_field_from_args.".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> {
        let metadata = metadata_for_field(args.arg_fields[0].as_ref())?;
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
                "Returns the minimum axis-aligned bounding box of a geometry. The result is a Polygon, or a Point or LineString when the envelope is degenerate. Empty inputs preserve their geometry type.",
                "ST_Envelope(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

fn metadata_for_field(field: &arrow_schema::Field) -> GeoDataFusionResult<Arc<Metadata>> {
    if field.data_type() == &DataType::Null {
        Ok(Arc::new(Metadata::default()))
    } else {
        Ok(Arc::new(Metadata::try_from(field)?))
    }
}

fn envelope_impl(
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
    let output_type =
        GeometryType::new(geo_array.data_type().metadata().clone()).with_coord_type(coord_type);
    let geo_array_ref = geo_array.as_ref();
    let output = downcast_geoarrow_array!(geo_array_ref, envelope_array, &output_type)?;
    Ok(output.into_array_ref().into())
}

fn null_geometry_array(
    number_rows: usize,
    coord_type: CoordType,
) -> GeoDataFusionResult<GeometryArray> {
    let output_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
    let mut builder = GeometryBuilder::new(output_type);
    for _ in 0..number_rows {
        builder.push_null();
    }
    Ok(builder.finish())
}

fn envelope_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeometryType,
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = GeometryBuilder::new(output_type.clone());

    for geometry in array.iter() {
        let Some(geometry) = geometry.transpose()? else {
            builder.push_null();
            continue;
        };

        match geometry_bounds(&geometry) {
            GeometryBounds::Empty => push_empty_or_original(&mut builder, &geometry)?,
            GeometryBounds::NonFinite => {
                return Err(DataFusionError::Execution(
                    "ST_Envelope does not support non-finite coordinates".to_string(),
                )
                .into());
            }
            GeometryBounds::Finite(bounds) => {
                let (xmin, ymin) = (bounds.minx(), bounds.miny());
                let (xmax, ymax) = (bounds.maxx(), bounds.maxy());
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

fn push_empty_or_original(
    builder: &mut GeometryBuilder,
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoDataFusionResult<()> {
    let dimension = wkt_dimension(geometry.dim())?;
    let empty = match geometry.as_type() {
        geo_traits::GeometryType::Point(_) => Wkt::Point(WktPoint::empty(dimension)),
        geo_traits::GeometryType::LineString(_) => Wkt::LineString(WktLineString::empty(dimension)),
        geo_traits::GeometryType::Polygon(_) => Wkt::Polygon(WktPolygon::empty(dimension)),
        geo_traits::GeometryType::MultiPoint(_) => Wkt::MultiPoint(WktMultiPoint::empty(dimension)),
        geo_traits::GeometryType::MultiLineString(_) => {
            Wkt::MultiLineString(WktMultiLineString::empty(dimension))
        }
        geo_traits::GeometryType::MultiPolygon(_) => {
            Wkt::MultiPolygon(WktMultiPolygon::empty(dimension))
        }
        geo_traits::GeometryType::GeometryCollection(_) => {
            Wkt::GeometryCollection(WktGeometryCollection::empty(dimension))
        }
        geo_traits::GeometryType::Rect(_)
        | geo_traits::GeometryType::Triangle(_)
        | geo_traits::GeometryType::Line(_) => {
            builder.push_geometry(Some(geometry))?;
            return Ok(());
        }
    };
    builder.push_geometry(Some(&empty))?;
    Ok(())
}

fn wkt_dimension(dimension: Dimensions) -> GeoDataFusionResult<WktDimension> {
    match dimension {
        Dimensions::Xy => Ok(WktDimension::XY),
        Dimensions::Xyz => Ok(WktDimension::XYZ),
        Dimensions::Xym => Ok(WktDimension::XYM),
        Dimensions::Xyzm => Ok(WktDimension::XYZM),
        Dimensions::Unknown(size) => Err(DataFusionError::NotImplemented(format!(
            "ST_Envelope does not support geometries with {size} physical dimensions"
        ))
        .into()),
    }
}
