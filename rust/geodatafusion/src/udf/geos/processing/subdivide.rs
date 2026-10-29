use std::sync::{Arc, OnceLock};

use arrow_array::{Array, StructArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::GeoArrowType;
use geos::{Geom, Geometry, GeometryTypes, sys};
use wkb::reader::Wkb;

use super::super::adapter::GeosWkbWriter;
use super::super::{adapter, raw};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

const FP_TOLERANCE: f64 = 1e-12;
const MAX_DEPTH: u32 = 50;
const MIN_MAX_VERTICES: i64 = 5;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Subdivide {
    signature: Signature,
}

impl Subdivide {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for Subdivide {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Subdivide {
    fn name(&self) -> &str {
        "st_subdivide"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1, 2, 3], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() >= 2 {
            coerced[1] = adapter::coerce_int64(&arg_types[1], 1, self.name())?;
        }
        if arg_types.len() == 3 {
            coerced[2] = adapter::coerce_float64(&arg_types[2], 2, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let geometry = adapter::geometry_return_field(&args, &[0], self.name())?;
        let geom = Arc::new(geometry.as_ref().clone().with_name("geom"));
        let item = Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![geom])),
            false,
        ));
        Ok(Arc::new(Field::new("", DataType::List(item), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(subdivide_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns, per input row, a list of `geom` structs for pieces of a geometry subdivided until each piece has at most `maxvertices` vertices. Use `SELECT s.comp.geom FROM (SELECT unnest(ST_Subdivide(...)) AS comp) AS s` to expand the list into one row per piece. NULL input or NULL maxvertices produces a NULL list, which `unnest` expands to zero rows like the PostGIS set-returning function. Z is preserved and M is dropped before GEOS.",
                "ST_Subdivide(geom, maxvertices, gridsize)",
            )
            .with_argument("geom", "geometry to subdivide")
            .with_argument("maxvertices", "maximum vertices per output piece")
            .with_argument("gridsize", "optional precision grid size for clipping")
            .build()
        }))
    }
}

fn subdivide_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let output_type = {
        let DataType::List(item) = args.return_field.data_type() else {
            return Err(DataFusionError::Internal(
                "ST_Subdivide result field is not a List".to_string(),
            )
            .into());
        };
        let DataType::Struct(fields) = item.data_type() else {
            return Err(DataFusionError::Internal(
                "ST_Subdivide list item is not a Struct".to_string(),
            )
            .into());
        };
        let geometry_field = fields
            .iter()
            .find(|field| field.name() == "geom")
            .ok_or_else(|| {
                DataFusionError::Internal("ST_Subdivide list item has no geom field".to_string())
            })?;
        GeoArrowType::from_arrow_field(geometry_field)?
    };
    let GeoArrowType::Geometry(geometry_type) = output_type else {
        return Err(DataFusionError::Internal(
            "ST_Subdivide result field is not a generic Geometry".to_string(),
        )
        .into());
    };
    let row_count = arrays.first().map_or(0, |array| array.len());

    let input_nulls = matches!(args.arg_fields[0].data_type(), DataType::Null);
    let geo_array = if input_nulls {
        None
    } else {
        Some(geometry_array_from_arrow(
            arrays[0].as_ref(),
            args.arg_fields[0].as_ref(),
        )?)
    };
    let wkb_array = match &geo_array {
        Some(array) => Some(to_wkb::<i32>(array.as_ref())?),
        None => None,
    };

    let mut values = OutputBuilder {
        builder: GeometryBuilder::new(geometry_type.clone()),
        count: 0,
    };
    let mut row_lengths = Vec::with_capacity(row_count);
    let mut validity = NullBufferBuilder::new(row_count);
    let mut writer = GeosWkbWriter::new()?;

    for row in 0..row_count {
        let maxvertices = if arrays.len() >= 2 {
            adapter::int64_value(&arrays, 1, row, "ST_Subdivide")?
        } else {
            Some(256)
        };
        let gridsize = if arrays.len() == 3 {
            adapter::float64_value(&arrays, 2, row, "ST_Subdivide")?
        } else {
            Some(-1.0)
        };

        let (Some(wkb_array), Some(maxvertices), Some(gridsize)) =
            (&wkb_array, maxvertices, gridsize)
        else {
            row_lengths.push(0);
            validity.append_null();
            continue;
        };
        if wkb_array.inner().is_null(row) {
            row_lengths.push(0);
            validity.append_null();
            continue;
        }
        let prepared = adapter::prepare_geos_wkb(wkb_array.inner().value(row))?;
        let geometry = Geometry::new_from_wkb(prepared.as_ref())?;
        let start = values.count;
        if !geometry.is_empty()? {
            if maxvertices < MIN_MAX_VERTICES {
                return Err(DataFusionError::Execution(
                    "ST_Subdivide: cannot subdivide to fewer than 5 vertices per output"
                        .to_string(),
                )
                .into());
            }
            let dimension = geometry_dimension(&geometry)?;
            let options = SubdivideOptions {
                maxvertices: maxvertices as usize,
                grid_size: gridsize,
            };
            subdivide_recursive(&geometry, dimension, 0, &options, &mut writer, &mut values)?;
        }
        row_lengths.push(values.count - start);
        validity.append_non_null();
    }

    let geom_field = Arc::new(geometry_type.to_field("geom", true));
    let struct_array = StructArray::new(
        Fields::from(vec![geom_field.clone()]),
        vec![values.builder.finish().into_array_ref()],
        None,
    );
    let item = Arc::new(Field::new(
        "item",
        DataType::Struct(Fields::from(vec![geom_field])),
        false,
    ));
    Ok(ColumnarValue::Array(Arc::new(arrow_array::ListArray::new(
        item,
        OffsetBuffer::<i32>::from_lengths(row_lengths),
        Arc::new(struct_array),
        validity.finish(),
    ))))
}

struct SubdivideOptions {
    maxvertices: usize,
    grid_size: f64,
}

struct OutputBuilder {
    builder: GeometryBuilder,
    count: usize,
}

impl OutputBuilder {
    fn push(
        &mut self,
        geometry: &impl Geom,
        writer: &mut GeosWkbWriter,
    ) -> GeoDataFusionResult<()> {
        let wkb = writer.write(geometry)?;
        let parsed = Wkb::try_new(&wkb).map_err(|error| {
            DataFusionError::Execution(format!("ST_Subdivide result WKB is invalid: {error}"))
        })?;
        self.builder.push_geometry(Some(&parsed))?;
        self.count += 1;
        Ok(())
    }
}

fn subdivide_recursive(
    geometry: &impl Geom,
    dimension: u8,
    depth: u32,
    options: &SubdivideOptions,
    writer: &mut GeosWkbWriter,
    output: &mut OutputBuilder,
) -> GeoDataFusionResult<()> {
    if geometry.is_empty()? {
        return Ok(());
    }

    let mut bbox = BBox {
        xmin: geometry.get_x_min()?,
        ymin: geometry.get_y_min()?,
        xmax: geometry.get_x_max()?,
        ymax: geometry.get_y_max()?,
    };
    let mut width = bbox.xmax - bbox.xmin;
    let mut height = bbox.ymax - bbox.ymin;

    if width == 0.0 && height == 0.0 {
        if geometry.geometry_type()? == GeometryTypes::Point && dimension == 0 {
            output.push(geometry, writer)?;
        }
        return Ok(());
    }
    if width == 0.0 {
        bbox.xmin -= FP_TOLERANCE;
        bbox.xmax += FP_TOLERANCE;
        width = 2.0 * FP_TOLERANCE;
    }
    if height == 0.0 {
        bbox.ymin -= FP_TOLERANCE;
        bbox.ymax += FP_TOLERANCE;
        height = 2.0 * FP_TOLERANCE;
    }

    let geometry_type = geometry.geometry_type()?;
    if geometry_type != GeometryTypes::MultiPoint
        && matches!(
            geometry_type,
            GeometryTypes::MultiLineString
                | GeometryTypes::MultiPolygon
                | GeometryTypes::GeometryCollection
        )
    {
        for index in 0..geometry.get_num_geometries()? {
            let child = geometry.get_geometry_n(index)?;
            subdivide_recursive(&child, dimension, depth, options, writer, output)?;
        }
        return Ok(());
    }

    if geometry_dimension(geometry)? < dimension {
        return Ok(());
    }
    if depth > MAX_DEPTH {
        output.push(geometry, writer)?;
        return Ok(());
    }

    let vertices = geometry.get_num_coordinates()?;
    if vertices == 0 {
        return Ok(());
    }
    if vertices <= options.maxvertices {
        output.push(geometry, writer)?;
        return Ok(());
    }

    let split_x = width > height;
    let center = if split_x {
        (bbox.xmin + bbox.xmax) / 2.0
    } else {
        (bbox.ymin + bbox.ymax) / 2.0
    };
    let mut pivot = f64::MAX;
    if geometry_type == GeometryTypes::Polygon {
        pivot = polygon_split_pivot(geometry, split_x, center, vertices)?;
    }
    if pivot == f64::MAX {
        pivot = center;
    }

    let mut first = bbox;
    let mut second = bbox;
    if split_x {
        if fp_nequals(first.xmax, pivot) && fp_nequals(first.xmin, pivot) {
            first.xmax = pivot;
            second.xmin = pivot;
        } else {
            first.xmax = center;
            second.xmin = center;
        }
    } else {
        if fp_nequals(first.ymax, pivot) && fp_nequals(first.ymin, pivot) {
            first.ymax = pivot;
            second.ymin = pivot;
        } else {
            first.ymax = center;
            second.ymin = center;
        }
    }

    for subbox in [first, second] {
        let envelope = envelope_geometry(&subbox)?;
        let mut clipped = intersect_with_precision(geometry, &envelope, options.grid_size, writer)?;
        clipped = clipped.simplify(0.0)?;
        if !clipped.is_empty()? {
            subdivide_recursive(&clipped, dimension, depth + 1, options, writer, output)?;
        }
    }
    Ok(())
}

fn polygon_split_pivot(
    geometry: &impl Geom,
    split_x: bool,
    center: f64,
    vertices: usize,
) -> GeoDataFusionResult<f64> {
    let exterior_ring = geometry.get_exterior_ring()?;
    let exterior = exterior_ring.get_coord_seq()?;
    let mut ring: Option<geos::CoordSeq> = None;
    if vertices >= 2 * exterior.size()? {
        let mut best_area = 0.0;
        for index in 0..geometry.get_num_interior_rings()? {
            let hole = geometry.get_interior_ring_n(index)?;
            let sequence = hole.get_coord_seq()?;
            let area = signed_area(&sequence)?.abs();
            if area >= best_area {
                best_area = area;
                ring = Some(sequence);
            }
        }
    }
    let sequence = ring.as_ref().unwrap_or(&exterior);

    let mut pivot = f64::MAX;
    let mut pivot_eps = f64::MAX;
    for index in 0..sequence.size()? {
        let coordinate = if split_x {
            sequence.get_x(index)?
        } else {
            sequence.get_y(index)?
        };
        let eps = (coordinate - center).abs();
        if pivot_eps > eps {
            pivot = coordinate;
            pivot_eps = eps;
        }
    }
    Ok(pivot)
}

fn signed_area(sequence: &geos::CoordSeq) -> GeoDataFusionResult<f64> {
    let size = sequence.size()?;
    let mut area = 0.0;
    for index in 0..size {
        let next = (index + 1) % size;
        area += sequence.get_x(index)? * sequence.get_y(next)?;
        area -= sequence.get_x(next)? * sequence.get_y(index)?;
    }
    Ok(area / 2.0)
}

fn intersect_with_precision(
    geometry: &impl Geom,
    envelope: &Geometry,
    grid_size: f64,
    writer: &mut GeosWkbWriter,
) -> GeoDataFusionResult<Geometry> {
    if grid_size < 0.0 {
        return Ok(geometry.intersection(envelope)?);
    }

    let left = writer.write(geometry)?;
    let right = writer.write(envelope)?;
    let result = raw::with_context("ST_Subdivide", |handle| unsafe {
        let left_geometry = raw::read_wkb(handle, &left, "ST_Subdivide")?;
        let right_geometry = raw::read_wkb(handle, &right, "ST_Subdivide")?;
        let output = sys::GEOSIntersectionPrec_r(handle, left_geometry, right_geometry, grid_size);
        raw::destroy_geometry(handle, left_geometry);
        raw::destroy_geometry(handle, right_geometry);
        if output.is_null() {
            return Err(DataFusionError::Execution(
                "ST_Subdivide: GEOS precision intersection failed".to_string(),
            )
            .into());
        }
        let wkb = raw::write_wkb(handle, output, "ST_Subdivide")?;
        raw::destroy_geometry(handle, output);
        Ok(wkb)
    })?;
    Ok(Geometry::new_from_wkb(&result)?)
}

fn envelope_geometry(bbox: &BBox) -> GeoDataFusionResult<Geometry> {
    let wkt = format!(
        "POLYGON(({} {},{} {},{} {},{} {},{} {}))",
        bbox.xmin,
        bbox.ymin,
        bbox.xmax,
        bbox.ymin,
        bbox.xmax,
        bbox.ymax,
        bbox.xmin,
        bbox.ymax,
        bbox.xmin,
        bbox.ymin
    );
    Ok(Geometry::new_from_wkt(&wkt)?)
}

fn geometry_dimension(geometry: &impl Geom) -> GeoDataFusionResult<u8> {
    Ok(match geometry.geometry_type()? {
        GeometryTypes::Point | GeometryTypes::MultiPoint => 0,
        GeometryTypes::LineString | GeometryTypes::LinearRing | GeometryTypes::MultiLineString => 1,
        GeometryTypes::Polygon | GeometryTypes::MultiPolygon => 2,
        GeometryTypes::GeometryCollection => {
            let mut dimension = 0;
            for index in 0..geometry.get_num_geometries()? {
                let child = geometry.get_geometry_n(index)?;
                dimension = dimension.max(geometry_dimension(&child)?);
            }
            dimension
        }
    })
}

fn fp_nequals(left: f64, right: f64) -> bool {
    (left - right).abs() > FP_TOLERANCE
}

#[derive(Debug, Clone, Copy)]
struct BBox {
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
}
