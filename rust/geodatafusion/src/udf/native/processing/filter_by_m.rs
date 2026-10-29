use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{Dimensions, GeometryTrait};
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon,
};

use super::util;
use crate::error::GeoDataFusionResult;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct FilterByM {
    signature: Signature,
}

impl FilterByM {
    pub fn new() -> Self {
        Self {
            signature: util::signature(),
        }
    }
}

impl Default for FilterByM {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for FilterByM {
    fn name(&self) -> &str {
        "st_filterbym"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        util::check_arg_count(arg_types, &[2, 3, 4], self.name())?;
        util::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        coerced[1] = util::coerce_float64(&arg_types[1], 1, self.name())?;
        if arg_types.len() > 2 {
            coerced[2] = util::coerce_float64(&arg_types[2], 2, self.name())?;
        }
        if arg_types.len() > 3 {
            coerced[3] = util::coerce_boolean(&arg_types[3], 3, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_FilterByM requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        util::geometry_return_field(&args)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(filter_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Removes vertices whose M value is outside the inclusive [min, max] range. A NULL min is treated as DBL_MIN and a NULL max as DBL_MAX. Vertices are returned without M unless returnM is true. Geometries without M are returned unchanged.",
                "ST_FilterByM(geometry, min, max, returnM)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("min", "minimum M value")
            .with_argument("max", "maximum M value")
            .with_argument("returnM", "whether to retain M in the result")
            .build()
        }))
    }
}

fn filter_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays[0].len();
    let geometries = util::input_geometry_values(args, "ST_FilterByM")?;
    let mut output = Vec::with_capacity(row_count);

    for (row, geometry) in geometries.iter().enumerate() {
        let Some(geometry) = geometry.clone() else {
            output.push(None);
            continue;
        };

        let min =
            util::float64_value(&arrays, 1, row, "ST_FilterByM")?.unwrap_or(f64::MIN_POSITIVE);
        let max = if arrays.len() > 2 {
            util::float64_value(&arrays, 2, row, "ST_FilterByM")?.unwrap_or(f64::MAX)
        } else {
            f64::MAX
        };
        let return_m = if arrays.len() > 3 {
            util::boolean_value(&arrays, 3, row, "ST_FilterByM")?.unwrap_or(false)
        } else {
            false
        };

        if min > max {
            return Err(DataFusionError::Execution(
                "Min-value cannot be larger than Max value".to_string(),
            )
            .into());
        }

        if !has_m(to_dimension(geometry.dim())) {
            output.push(Some(geometry));
            continue;
        }

        output.push(Some(
            filter_geometry(geometry.clone(), min, max, return_m)
                .unwrap_or_else(|| empty_geometry(&geometry, return_m)),
        ));
    }

    util::geometry_columnar(args, output)
}

fn has_m(dimension: Dimension) -> bool {
    matches!(dimension, Dimension::XYM | Dimension::XYZM)
}

fn to_dimension(dimension: Dimensions) -> Dimension {
    match dimension {
        Dimensions::Xy | Dimensions::Unknown(2) => Dimension::XY,
        Dimensions::Xyz | Dimensions::Unknown(3) => Dimension::XYZ,
        Dimensions::Xym => Dimension::XYM,
        Dimensions::Xyzm | Dimensions::Unknown(4) => Dimension::XYZM,
        Dimensions::Unknown(_) => Dimension::XY,
    }
}

fn without_m(dimension: Dimension) -> Dimension {
    match dimension {
        Dimension::XYM => Dimension::XY,
        Dimension::XYZM => Dimension::XYZ,
        other => other,
    }
}

fn filter_geometry(geometry: Wkt<f64>, min: f64, max: f64, return_m: bool) -> Option<Wkt<f64>> {
    match geometry {
        Wkt::Point(point) => {
            let (coord, dimension) = point.into_inner();
            let filtered = coord
                .filter(|coord| coord_in_range(coord, min, max))
                .map(|coord| output_coord(coord, return_m));
            filtered.map(|coord| {
                Wkt::Point(Point::new(
                    Some(coord),
                    output_dimension(dimension, return_m),
                ))
            })
        }
        Wkt::LineString(line) => {
            let (coords, dimension) = line.into_inner();
            let coords = filter_coords(coords, min, max, return_m);
            (coords.len() >= 2).then(|| {
                Wkt::LineString(LineString::new(
                    coords,
                    output_dimension(dimension, return_m),
                ))
            })
        }
        Wkt::Polygon(polygon) => {
            let (rings, dimension) = polygon.into_inner();
            let mut output = Vec::with_capacity(rings.len());
            for (index, ring) in rings.into_iter().enumerate() {
                let (coords, ring_dimension) = ring.into_inner();
                let coords = filter_coords(coords, min, max, return_m);
                if coords.len() >= 4 {
                    output.push(LineString::new(
                        coords,
                        output_dimension(ring_dimension, return_m),
                    ));
                } else if index == 0 {
                    return None;
                }
            }
            (!output.is_empty())
                .then(|| Wkt::Polygon(Polygon::new(output, output_dimension(dimension, return_m))))
        }
        Wkt::MultiPoint(multi_point) => {
            let (points, dimension) = multi_point.into_inner();
            let points = points
                .into_iter()
                .filter_map(|point| {
                    let (coord, point_dimension) = point.into_inner();
                    coord
                        .filter(|coord| coord_in_range(coord, min, max))
                        .map(|coord| {
                            Point::new(
                                Some(output_coord(coord, return_m)),
                                output_dimension(point_dimension, return_m),
                            )
                        })
                })
                .collect();
            Some(Wkt::MultiPoint(MultiPoint::new(
                points,
                output_dimension(dimension, return_m),
            )))
        }
        Wkt::MultiLineString(multi_line) => {
            let (lines, dimension) = multi_line.into_inner();
            let lines = lines
                .into_iter()
                .filter_map(|line| {
                    let (coords, line_dimension) = line.into_inner();
                    let coords = filter_coords(coords, min, max, return_m);
                    (coords.len() >= 2).then(|| {
                        LineString::new(coords, output_dimension(line_dimension, return_m))
                    })
                })
                .collect();
            Some(Wkt::MultiLineString(MultiLineString::new(
                lines,
                output_dimension(dimension, return_m),
            )))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, dimension) = multi_polygon.into_inner();
            let polygons = polygons
                .into_iter()
                .filter_map(|polygon| {
                    match filter_geometry(Wkt::Polygon(polygon), min, max, return_m) {
                        Some(Wkt::Polygon(polygon)) => Some(polygon),
                        _ => None,
                    }
                })
                .collect();
            Some(Wkt::MultiPolygon(MultiPolygon::new(
                polygons,
                output_dimension(dimension, return_m),
            )))
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, dimension) = collection.into_inner();
            let geometries = geometries
                .into_iter()
                .filter_map(|geometry| filter_geometry(geometry, min, max, return_m))
                .collect();
            Some(Wkt::GeometryCollection(GeometryCollection::new(
                geometries,
                output_dimension(dimension, return_m),
            )))
        }
    }
}

fn empty_geometry(geometry: &Wkt<f64>, return_m: bool) -> Wkt<f64> {
    let dimension = output_dimension(to_dimension(geometry.dim()), return_m);
    match geometry {
        Wkt::Point(_) => Wkt::Point(Point::new(None, dimension)),
        Wkt::LineString(_) => Wkt::LineString(LineString::new(Vec::new(), dimension)),
        Wkt::Polygon(_) => Wkt::Polygon(Polygon::new(Vec::new(), dimension)),
        Wkt::MultiPoint(_) => Wkt::MultiPoint(MultiPoint::new(Vec::new(), dimension)),
        Wkt::MultiLineString(_) => {
            Wkt::MultiLineString(MultiLineString::new(Vec::new(), dimension))
        }
        Wkt::MultiPolygon(_) => Wkt::MultiPolygon(MultiPolygon::new(Vec::new(), dimension)),
        Wkt::GeometryCollection(_) => {
            Wkt::GeometryCollection(GeometryCollection::new(Vec::new(), dimension))
        }
    }
}

fn filter_coords(coords: Vec<Coord<f64>>, min: f64, max: f64, return_m: bool) -> Vec<Coord<f64>> {
    coords
        .into_iter()
        .filter(|coord| coord_in_range(coord, min, max))
        .map(|coord| output_coord(coord, return_m))
        .collect()
}

fn coord_in_range(coord: &Coord<f64>, min: f64, max: f64) -> bool {
    coord.m.is_none_or(|value| value >= min && value <= max)
}

fn output_coord(mut coord: Coord<f64>, return_m: bool) -> Coord<f64> {
    if !return_m {
        coord.m = None;
    }
    coord
}

fn output_dimension(dimension: Dimension, return_m: bool) -> Dimension {
    if return_m {
        dimension
    } else {
        without_m(dimension)
    }
}
