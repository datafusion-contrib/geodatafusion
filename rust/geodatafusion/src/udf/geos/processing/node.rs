use std::collections::HashSet;
use std::sync::OnceLock;

use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geos::{CoordDimensions, CoordSeq, CoordType, DimensionType, Geom, Geometry, GeometryTypes};

use super::super::adapter;
use crate::error::GeoDataFusionResult;

// Little-endian WKB for typed empty linework. PostGIS drops Z and M when it
// nodes empty input.
const EMPTY_LINESTRING_WKB: &[u8] = &[1, 2, 0, 0, 0, 0, 0, 0, 0];
const EMPTY_MULTILINESTRING_WKB: &[u8] = &[1, 5, 0, 0, 0, 0, 0, 0, 0];

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Node {
    signature: Signature,
}

impl Node {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Node {
    fn name(&self) -> &str {
        "st_node"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        adapter::geometry_return_field(&args, &[0], self.name())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(node_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns fully-noded linework, dissolving duplicate segments. The input must have topological dimension 1. GEOS preserves Z where available. M ordinates are dropped before GEOS, and empty linework is returned as typed 2D empty output.",
                "ST_Node(geometry)",
            )
            .with_argument("geometry", "linear geometry")
            .build()
        }))
    }
}

fn node_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    adapter::unary_geos(args, |_row, _arrays, _original, geometry, writer| {
        if geometry.is_empty()?
            && matches!(
                geometry.geometry_type()?,
                GeometryTypes::LineString | GeometryTypes::MultiLineString
            )
        {
            let empty = match geometry.geometry_type()? {
                GeometryTypes::LineString => EMPTY_LINESTRING_WKB,
                GeometryTypes::MultiLineString => EMPTY_MULTILINESTRING_WKB,
                _ => unreachable!("empty linework was checked above"),
            };
            return Ok(Some(empty.to_vec()));
        }

        if geometry.get_dimension()? != DimensionType::Curve {
            return Err(DataFusionError::Execution(
                "ST_Node supports only geometries with topological dimension 1".to_string(),
            )
            .into());
        }

        let noded = geometry.node()?;
        let count = noded.get_num_geometries()?;
        if count == 0 {
            return Ok(Some(writer.write(&noded)?));
        }
        let linework = if count == 1 {
            let child = noded.get_geometry_n(0)?;
            Geom::clone(&child)?
        } else {
            noded.line_merge()?
        };
        let split = split_at_original_endpoints(&linework, geometry)?;
        Ok(Some(writer.write(&split)?))
    })
}

fn split_at_original_endpoints(
    linework: &Geometry,
    original: &Geometry,
) -> GeoDataFusionResult<Geometry> {
    // LineMerge joins across degree-2 input endpoints. PostGIS re-splits the
    // merged linework at the original endpoints to keep them as stable nodes.
    let mut endpoints = HashSet::new();
    collect_endpoints(original, &mut endpoints)?;
    if endpoints.is_empty() {
        return Ok(Geom::clone(linework)?);
    }

    let mut lines = Vec::new();
    split_linework(linework, &endpoints, &mut lines)?;
    match lines.len() {
        0 => Ok(Geom::clone(linework)?),
        1 => lines.pop().ok_or_else(|| {
            DataFusionError::Internal("ST_Node produced an empty split result".to_string()).into()
        }),
        _ => Ok(Geometry::create_multiline_string(lines)?),
    }
}

fn collect_endpoints<G: Geom>(
    geometry: &G,
    endpoints: &mut HashSet<(u64, u64)>,
) -> GeoDataFusionResult<()> {
    match geometry.geometry_type()? {
        GeometryTypes::LineString | GeometryTypes::LinearRing => {
            let coords = geometry.get_coord_seq()?;
            let size = coords.size()?;
            if size == 0 {
                return Ok(());
            }
            for index in [0, size - 1] {
                let x = coords.get_x(index)?;
                let y = coords.get_y(index)?;
                endpoints.insert((x.to_bits(), y.to_bits()));
            }
        }
        GeometryTypes::MultiLineString | GeometryTypes::GeometryCollection => {
            for index in 0..geometry.get_num_geometries()? {
                collect_endpoints(&geometry.get_geometry_n(index)?, endpoints)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn split_linework<G: Geom>(
    geometry: &G,
    endpoints: &HashSet<(u64, u64)>,
    output: &mut Vec<Geometry>,
) -> GeoDataFusionResult<()> {
    match geometry.geometry_type()? {
        GeometryTypes::LineString | GeometryTypes::LinearRing => {
            split_linestring(geometry, endpoints, output)
        }
        GeometryTypes::MultiLineString | GeometryTypes::GeometryCollection => {
            for index in 0..geometry.get_num_geometries()? {
                split_linework(&geometry.get_geometry_n(index)?, endpoints, output)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn split_linestring<G: Geom>(
    geometry: &G,
    endpoints: &HashSet<(u64, u64)>,
    output: &mut Vec<Geometry>,
) -> GeoDataFusionResult<()> {
    let source = geometry.get_coord_seq()?;
    let size = source.size()?;
    if size < 2 {
        return Ok(());
    }
    let has_z = matches!(
        geometry.get_coordinate_dimension()?,
        CoordDimensions::ThreeD
    );

    let mut split_indices = vec![0];
    for index in 1..size - 1 {
        let key = (
            source.get_x(index)?.to_bits(),
            source.get_y(index)?.to_bits(),
        );
        if endpoints.contains(&key) {
            split_indices.push(index);
        }
    }
    split_indices.push(size - 1);

    for pair in split_indices.windows(2) {
        let start = pair[0];
        let end = pair[1];
        if end <= start {
            continue;
        }

        let mut coords = CoordSeq::new(
            (end - start + 1) as u32,
            if has_z { CoordType::XYZ } else { CoordType::XY },
        )?;
        for (target, source_index) in (start..=end).enumerate() {
            coords.set_x(target, source.get_x(source_index)?)?;
            coords.set_y(target, source.get_y(source_index)?)?;
            if has_z {
                coords.set_z(target, source.get_z(source_index)?)?;
            }
        }
        output.push(Geometry::create_line_string(coords)?);
    }
    Ok(())
}
