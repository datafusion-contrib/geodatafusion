use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, PrimitiveArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::array::{GeometryArray, RectArray, from_arrow_array};
use geoarrow_array::builder::{GeometryBuilder, RectBuilder};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{BoxType, CoordType, Dimension, GeometryType, Metadata};
use wkt::Wkt;
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, GeometryCollection as WktGeometryCollection,
    LineString as WktLineString, MultiLineString as WktMultiLineString,
    MultiPoint as WktMultiPoint, MultiPolygon as WktMultiPolygon, Point as WktPoint,
    Polygon as WktPolygon,
};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

fn box_data_type(dimension: Dimension) -> DataType {
    BoxType::new(dimension, Default::default()).data_type()
}

fn box_dimension(data_type: &DataType) -> Option<Dimension> {
    if data_type == &box_data_type(Dimension::XY) {
        Some(Dimension::XY)
    } else if data_type == &box_data_type(Dimension::XYZ) {
        Some(Dimension::XYZ)
    } else {
        None
    }
}

static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::user_defined(Volatility::Immutable));

/// `ST_Expand` scalar UDF implementation: expands boxes or geometry envelopes by per-axis distances.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Expand {
    coord_type: CoordType,
}

impl Expand {
    /// Creates a new `ST_Expand` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Expand {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Expand {
    fn name(&self) -> &str {
        "st_expand"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from input metadata in return_field_from_args.".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(expand_impl(args, self.coord_type)?)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=5).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(format!(
                "ST_Expand expects between 2 and 5 arguments, got {}",
                arg_types.len()
            )));
        }

        let input_type = &arg_types[0];
        let is_box = box_dimension(input_type).is_some();
        let is_geometry = any_geometry_type()
            .iter()
            .any(|candidate| candidate == input_type);
        if !is_box && !is_geometry && input_type != &DataType::Null {
            return Err(DataFusionError::Plan(format!(
                "ST_Expand does not support input type {input_type}"
            )));
        }

        let max_distances = match box_dimension(input_type) {
            Some(Dimension::XY) => 2,
            Some(Dimension::XYZ) => 3,
            Some(_) => unreachable!("box inputs are restricted to XY and XYZ"),
            None => 4,
        };
        let distance_count = arg_types.len() - 1;
        if distance_count > max_distances {
            return Err(DataFusionError::Plan(format!(
                "ST_Expand accepts at most {max_distances} distances for this input, got {distance_count}"
            )));
        }

        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(input_type.clone());
        for data_type in &arg_types[1..] {
            if data_type.is_numeric() || data_type == &DataType::Null {
                coerced.push(DataType::Float64);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "ST_Expand expects numeric distances, got {data_type}"
                )));
            }
        }
        Ok(coerced)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a box or polygonal geometry expanded by a uniform distance or by per-axis distances. Box bounds are normalized after expansion, while geometry rings preserve the expanded coordinate order.",
                "ST_Expand(geometry, distance[, dy[, dz[, dm]]])",
            )
            .with_argument("geometry", "geometry or box")
            .with_argument("distance", "float")
            .with_argument("dy", "float")
            .with_argument("dz", "float")
            .with_argument("dm", "float")
            .build()
        }))
    }
}

fn metadata_for_field(field: &Field) -> GeoDataFusionResult<Arc<Metadata>> {
    if field.data_type() == &DataType::Null {
        Ok(Arc::new(Metadata::default()))
    } else {
        Ok(Arc::new(Metadata::try_from(field)?))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let input_field = args.arg_fields[0].as_ref();
    if let Ok(box_type) = input_field.try_extension_type::<BoxType>() {
        return Ok(Arc::new(box_type.to_field("", true)));
    }

    let output_type =
        GeometryType::new(metadata_for_field(input_field)?).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn expand_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if let Ok(box_type) = args.arg_fields[0].try_extension_type::<BoxType>() {
        expand_box(&arrays, box_type)
    } else {
        expand_geometry(&arrays, args.number_rows, coord_type, &args.arg_fields[0])
    }
}

fn factor_at(array: &PrimitiveArray<Float64Type>, index: usize) -> Option<f64> {
    let index = if array.len() == 1 { 0 } else { index };
    (index < array.len() && !array.is_null(index)).then(|| array.value(index))
}

fn factor_values(factors: &[&PrimitiveArray<Float64Type>], index: usize) -> Option<Vec<f64>> {
    factors
        .iter()
        .map(|factor| factor_at(factor, index))
        .collect()
}

fn expand_box(arrays: &[ArrayRef], box_type: BoxType) -> GeoDataFusionResult<ColumnarValue> {
    let rects = RectArray::try_from((arrays[0].as_ref(), box_type.clone()))?;
    let factors = arrays[1..]
        .iter()
        .map(|array| array.as_primitive::<Float64Type>())
        .collect::<Vec<_>>();
    let mut builder = RectBuilder::with_capacity(
        BoxType::new(box_type.dimension(), box_type.metadata().clone()),
        rects.len(),
    );

    for (index, rect) in rects.iter().enumerate() {
        let Some(rect) = rect.transpose()? else {
            builder.push_null();
            continue;
        };
        let Some(factors) = factor_values(&factors, index) else {
            builder.push_null();
            continue;
        };
        let (dx, dy, dz) = box_distances(&factors)?;

        let min = rect.min();
        let max = rect.max();
        let x1 = min.x() - dx;
        let x2 = max.x() + dx;
        let y1 = min.y() - dy;
        let y2 = max.y() + dy;
        let (z1, z2) = if box_type.dimension() == Dimension::XYZ {
            let start = min.nth_or_panic(2) - dz;
            let end = max.nth_or_panic(2) + dz;
            (Some(start), Some(end))
        } else {
            (None, None)
        };

        // Box bounds normalize after negative expansion. Geometry rings deliberately do not.
        let lower = WktCoord {
            x: x1.min(x2),
            y: y1.min(y2),
            z: z1.zip(z2).map(|(start, end)| start.min(end)),
            m: None,
        };
        let upper = WktCoord {
            x: x1.max(x2),
            y: y1.max(y2),
            z: z1.zip(z2).map(|(start, end)| start.max(end)),
            m: None,
        };
        builder.push_min_max(&lower, &upper);
    }

    Ok(builder.finish().into_array_ref().into())
}

fn box_distances(factors: &[f64]) -> GeoDataFusionResult<(f64, f64, f64)> {
    match factors {
        [distance] => Ok((*distance, *distance, *distance)),
        [dx, dy] => Ok((*dx, *dy, 0.0)),
        [dx, dy, dz] => Ok((*dx, *dy, *dz)),
        _ => Err(DataFusionError::Internal(format!(
            "ST_Expand received {} distances for a box input",
            factors.len()
        ))
        .into()),
    }
}

fn expand_geometry(
    arrays: &[ArrayRef],
    number_rows: usize,
    coord_type: CoordType,
    input_field: &FieldRef,
) -> GeoDataFusionResult<ColumnarValue> {
    if arrays[0].data_type() == &DataType::Null {
        return Ok(null_geometry_array(number_rows, coord_type)?
            .into_array_ref()
            .into());
    }

    let geo_array = from_arrow_array(&arrays[0], input_field)?;
    let output_type =
        GeometryType::new(geo_array.data_type().metadata().clone()).with_coord_type(coord_type);
    let factors = arrays[1..]
        .iter()
        .map(|array| array.as_primitive::<Float64Type>())
        .collect::<Vec<_>>();
    let geo_array_ref = geo_array.as_ref();
    let output =
        downcast_geoarrow_array!(geo_array_ref, expand_geometry_array, &output_type, &factors)?;
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

fn expand_geometry_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeometryType,
    factors: &[&PrimitiveArray<Float64Type>],
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = GeometryBuilder::new(output_type.clone());

    for (index, geometry) in array.iter().enumerate() {
        let Some(factors) = factor_values(factors, index) else {
            builder.push_null();
            continue;
        };
        let Some(geometry) = geometry.transpose()? else {
            builder.push_null();
            continue;
        };

        match geometry_extent(&geometry) {
            GeometryExtent::Empty => push_empty_or_original(&mut builder, &geometry)?,
            GeometryExtent::NonFinite => {
                return Err(DataFusionError::Execution(
                    "ST_Expand does not support non-finite coordinates".to_string(),
                )
                .into());
            }
            GeometryExtent::Finite(bounds) => {
                let expanded = expanded_polygon(&bounds, &factors)?;
                builder.push_geometry(Some(&expanded))?;
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

#[derive(Debug, Clone, Copy)]
struct ExpandBounds {
    min: [f64; 4],
    max: [f64; 4],
    dimensions: Dimensions,
}

enum GeometryExtent {
    Empty,
    NonFinite,
    Finite(ExpandBounds),
}

fn geometry_extent(geometry: &impl GeometryTrait<T = f64>) -> GeometryExtent {
    let mut accumulator = ExtentAccumulator::new(geometry.dim());
    accumulator.add_geometry(geometry);

    if accumulator.has_non_finite {
        GeometryExtent::NonFinite
    } else if !accumulator.has_coords {
        GeometryExtent::Empty
    } else {
        GeometryExtent::Finite(ExpandBounds {
            min: accumulator.min,
            max: accumulator.max,
            dimensions: accumulator.dimensions,
        })
    }
}

struct ExtentAccumulator {
    min: [f64; 4],
    max: [f64; 4],
    dimensions: Dimensions,
    has_coords: bool,
    has_non_finite: bool,
}

impl ExtentAccumulator {
    fn new(dimensions: Dimensions) -> Self {
        Self {
            min: [f64::INFINITY; 4],
            max: [f64::NEG_INFINITY; 4],
            dimensions,
            has_coords: false,
            has_non_finite: false,
        }
    }

    fn add_coord(&mut self, coord: &impl CoordTrait<T = f64>) {
        self.has_coords = true;
        self.add_value(0, coord.x());
        self.add_value(1, coord.y());
        match coord.dim() {
            Dimensions::Xy => {}
            Dimensions::Xyz => self.add_value(2, coord.nth_or_panic(2)),
            Dimensions::Xym => self.add_value(3, coord.nth_or_panic(2)),
            Dimensions::Xyzm => {
                self.add_value(2, coord.nth_or_panic(2));
                self.add_value(3, coord.nth_or_panic(3));
            }
            Dimensions::Unknown(_) => {}
        }
    }

    fn add_value(&mut self, index: usize, value: f64) {
        if !value.is_finite() {
            self.has_non_finite = true;
        }
        self.min[index] = self.min[index].min(value);
        self.max[index] = self.max[index].max(value);
    }

    fn add_geometry(&mut self, geometry: &impl GeometryTrait<T = f64>) {
        use geo_traits::GeometryType::*;

        match geometry.as_type() {
            Point(point) => {
                if let Some(coord) = point.coord() {
                    self.add_coord(&coord);
                }
            }
            LineString(line_string) => self.add_line_string(line_string),
            Polygon(polygon) => self.add_polygon(polygon),
            MultiPoint(multi_point) => {
                for point in multi_point.points() {
                    if let Some(coord) = point.coord() {
                        self.add_coord(&coord);
                    }
                }
            }
            MultiLineString(multi_line_string) => {
                for line_string in multi_line_string.line_strings() {
                    self.add_line_string(&line_string);
                }
            }
            MultiPolygon(multi_polygon) => {
                for polygon in multi_polygon.polygons() {
                    self.add_polygon(&polygon);
                }
            }
            GeometryCollection(collection) => {
                for geometry in collection.geometries() {
                    self.add_geometry(&geometry);
                }
            }
            Rect(rect) => {
                self.add_coord(&rect.min());
                self.add_coord(&rect.max());
            }
            Triangle(triangle) => {
                for coord in triangle.coords() {
                    self.add_coord(&coord);
                }
            }
            Line(line) => {
                self.add_coord(&line.start());
                self.add_coord(&line.end());
            }
        }
    }

    fn add_line_string(&mut self, line_string: &impl LineStringTrait<T = f64>) {
        for coord in line_string.coords() {
            self.add_coord(&coord);
        }
    }

    fn add_polygon(&mut self, polygon: &impl PolygonTrait<T = f64>) {
        if let Some(exterior) = polygon.exterior() {
            self.add_line_string(&exterior);
        }
        for interior in polygon.interiors() {
            self.add_line_string(&interior);
        }
    }
}

fn expanded_polygon(
    bounds: &ExpandBounds,
    factors: &[f64],
) -> GeoDataFusionResult<WktPolygon<f64>> {
    let (dx, dy, dz, dm) = match factors {
        [distance] => (*distance, *distance, *distance, *distance),
        [dx, dy] => (*dx, *dy, 0.0, 0.0),
        [dx, dy, dz] => (*dx, *dy, *dz, 0.0),
        [dx, dy, dz, dm] => (*dx, *dy, *dz, *dm),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "ST_Expand received {} distances for a geometry input",
                factors.len()
            ))
            .into());
        }
    };

    let output_dimension = wkt_dimension(bounds.dimensions)?;
    let has_z = matches!(output_dimension, WktDimension::XYZ | WktDimension::XYZM);
    let has_m = matches!(output_dimension, WktDimension::XYM | WktDimension::XYZM);
    let x1 = bounds.min[0] - dx;
    let y1 = bounds.min[1] - dy;
    let z1 = bounds.min[2] - dz;
    let m1 = bounds.min[3] - dm;
    let x2 = bounds.max[0] + dx;
    let y2 = bounds.max[1] + dy;
    let z2 = bounds.max[2] + dz;
    let m2 = bounds.max[3] + dm;
    let coord = |x, y, z, m| WktCoord {
        x,
        y,
        z: has_z.then_some(z),
        m: has_m.then_some(m),
    };
    let ring = WktLineString::new(
        vec![
            coord(x1, y1, z1, m1),
            coord(x1, y2, z1, m1),
            coord(x2, y2, z2, m2),
            coord(x2, y1, z2, m2),
            coord(x1, y1, z1, m1),
        ],
        output_dimension,
    );
    Ok(WktPolygon::new(vec![ring], output_dimension))
}

fn wkt_dimension(dimension: Dimensions) -> GeoDataFusionResult<WktDimension> {
    match dimension {
        Dimensions::Xy => Ok(WktDimension::XY),
        Dimensions::Xyz => Ok(WktDimension::XYZ),
        Dimensions::Xym => Ok(WktDimension::XYM),
        Dimensions::Xyzm => Ok(WktDimension::XYZM),
        Dimensions::Unknown(size) => Err(DataFusionError::NotImplemented(format!(
            "ST_Expand does not support geometries with {size} physical dimensions"
        ))
        .into()),
    }
}
