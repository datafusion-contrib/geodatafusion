use std::sync::{Arc, OnceLock};

use arrow_array::builder::StringBuilder;
use arrow_schema::{DataType, Field};
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{
    GeometryCollectionTrait, GeometryTrait, LineStringTrait, MultiLineStringTrait, MultiPointTrait,
    MultiPolygonTrait, PolygonTrait,
};
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::Metadata;

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

/// Returns a text summary of geometry type, dimensions, and element counts.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Summary;

impl Summary {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for Summary {
    fn default() -> Self {
        Self::new()
    }
}

static SUMMARY_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Summary {
    fn name(&self) -> &str {
        "st_summary"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(summary_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SUMMARY_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a text summary of the geometry type, coordinate dimensions, and \
                 element/ring/point counts, matching the PostGIS ST_Summary layout. GeoArrow has \
                 no cached bounding-box flag and no geodetic flag, so the PostGIS B and G flags \
                 are not emitted. The S flag is emitted when the geometry column carries CRS \
                 metadata.",
                "ST_Summary(geometry)",
            )
            .with_argument("geometry", "input geometry")
            .build()
        }))
    }
}

fn summary_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let has_srid = has_crs_metadata(&args.arg_fields[0]);
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, summary_array, has_srid)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn summary_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    has_srid: bool,
) -> GeoDataFusionResult<arrow_array::StringArray> {
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 64);

    for item in array.iter() {
        if let Some(geometry) = item.transpose()? {
            builder.append_value(summary_geometry(&geometry, has_srid, 0));
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

fn has_crs_metadata(field: &Field) -> bool {
    let Ok(metadata) = Metadata::try_from(field) else {
        return false;
    };
    metadata.crs().crs_value().is_some()
}

fn summary_geometry(
    geometry: &impl GeometryTrait<T = f64>,
    has_srid: bool,
    indent: usize,
) -> String {
    use geo_traits::GeometryType::*;

    let flags = summary_flags(geometry, has_srid);
    let pad = " ".repeat(indent);
    match geometry.as_type() {
        Point(_) => format!("{pad}Point[{flags}]"),
        LineString(line_string) => format!(
            "{pad}LineString[{flags}] with {} points",
            line_string.num_coords()
        ),
        Polygon(polygon) => polygon_summary("Polygon", &flags, polygon, pad),
        MultiPoint(multi_point) => {
            let children = multi_point
                .points()
                .map(|point| summary_geometry(&point, has_srid, indent + 2))
                .collect();
            collection_summary(
                "MultiPoint",
                &flags,
                multi_point.num_points(),
                children,
                pad,
            )
        }
        MultiLineString(multi_line_string) => {
            let children = multi_line_string
                .line_strings()
                .map(|line_string| summary_geometry(&line_string, has_srid, indent + 2))
                .collect();
            collection_summary(
                "MultiLineString",
                &flags,
                multi_line_string.num_line_strings(),
                children,
                pad,
            )
        }
        MultiPolygon(multi_polygon) => {
            let children = multi_polygon
                .polygons()
                .map(|polygon| polygon_summary("Polygon", &flags, &polygon, " ".repeat(indent + 2)))
                .collect();
            collection_summary(
                "MultiPolygon",
                &flags,
                multi_polygon.num_polygons(),
                children,
                pad,
            )
        }
        GeometryCollection(collection) => {
            let children = collection
                .geometries()
                .map(|child| summary_geometry(&child, has_srid, indent + 2))
                .collect();
            collection_summary(
                "GeometryCollection",
                &flags,
                collection.num_geometries(),
                children,
                pad,
            )
        }
        // PostGIS stores a Triangle as a closed 4-point line.
        Triangle(_) => format!("{pad}Triangle[{flags}] with 4 points"),
        // GeoArrow Rect is a rectangle; report it like the equivalent closed Polygon.
        Rect(_) => format!("{pad}Polygon[{flags}] with 1 ring:\n   ring 0 has 5 points"),
        Line(_) => format!("{pad}LineString[{flags}] with 2 points"),
    }
}

fn polygon_summary(
    name: &str,
    flags: &str,
    polygon: &impl PolygonTrait<T = f64>,
    pad: String,
) -> String {
    let mut ring_point_counts = Vec::new();
    if let Some(ring) = polygon.exterior() {
        ring_point_counts.push(ring.num_coords());
    }
    for ring in polygon.interiors() {
        ring_point_counts.push(ring.num_coords());
    }

    let ring_count = ring_point_counts.len();
    let mut result = format!(
        "{pad}{name}[{flags}] with {ring_count} ring{}",
        if ring_count == 1 { "" } else { "s" }
    );
    if ring_count > 0 {
        result.push_str(":\n");
        let rings = ring_point_counts
            .iter()
            .enumerate()
            .map(|(index, point_count)| format!("   ring {index} has {point_count} points"))
            .collect::<Vec<_>>();
        result.push_str(&rings.join("\n"));
    }
    result
}

fn collection_summary(
    name: &str,
    flags: &str,
    count: usize,
    children: Vec<String>,
    pad: String,
) -> String {
    let mut result = format!(
        "{pad}{name}[{flags}] with {count} element{}",
        if count == 1 { "" } else { "s" }
    );
    if count > 0 {
        result.push_str(":\n");
        result.push_str(&children.join("\n"));
    }
    result
}

fn summary_flags(geometry: &impl GeometryTrait<T = f64>, has_srid: bool) -> String {
    let mut flags = String::new();
    if matches!(
        geometry.dim(),
        geo_traits::Dimensions::Xyz
            | geo_traits::Dimensions::Xyzm
            | geo_traits::Dimensions::Unknown(3 | 4)
    ) {
        flags.push('Z');
    }
    if matches!(
        geometry.dim(),
        geo_traits::Dimensions::Xym
            | geo_traits::Dimensions::Xyzm
            | geo_traits::Dimensions::Unknown(4)
    ) {
        flags.push('M');
    }
    if has_srid {
        flags.push('S');
    }
    flags
}

#[cfg(test)]
mod tests {
    use wkt::Wkt;
    use wkt::types::{Coord, Dimension, GeometryCollection, Point};

    use super::*;

    fn point(x: f64, y: f64) -> Point {
        Point::from_coord(Coord {
            x,
            y,
            z: None,
            m: None,
        })
    }

    #[test]
    fn nested_geometry_collections_render_postgis_layout() {
        let nested = GeometryCollection::new(
            vec![
                Wkt::GeometryCollection(GeometryCollection::new(
                    vec![Wkt::Point(point(1., 2.))],
                    Dimension::XY,
                )),
                Wkt::Point(point(3., 4.)),
            ],
            Dimension::XY,
        );
        assert_eq!(
            summary_geometry(&Wkt::GeometryCollection(nested), false, 0),
            "GeometryCollection[] with 2 elements:\n  \
             GeometryCollection[] with 1 element:\n    Point[]\n  Point[]"
        );
    }

    #[test]
    fn single_level_geometry_collections_render_postgis_layout() {
        let flat = GeometryCollection::new(vec![Wkt::Point(point(1., 2.))], Dimension::XY);
        assert_eq!(
            summary_geometry(&Wkt::GeometryCollection(flat), false, 0),
            "GeometryCollection[] with 1 element:\n  Point[]"
        );
    }
}
