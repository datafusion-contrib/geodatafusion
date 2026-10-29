use std::sync::{Arc, OnceLock};

use arrow_array::builder::{ArrayBuilder, Int32Builder, ListBuilder};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geo_traits::{GeometryTrait, LineStringTrait, PolygonTrait};
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::{GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{CoordType, GeometryType};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;
use crate::udf::native::accessors::common::{coord_to_wkt, output_geometry_type, wkt_dimension};
use crate::udf::native::accessors::dump::{
    finish_dump_list, output_field, path_values_field, push_dump_item,
};
use crate::udf::native::geometry::geometry_array_from_arrow;

/// Returns the exterior and interior rings of a Polygon as single-ring Polygons.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct DumpRings {
    coord_type: CoordType,
}

impl DumpRings {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for DumpRings {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DUMP_RINGS_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for DumpRings {
    fn name(&self) -> &str {
        "st_dumprings"
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "Return field is computed from geometry metadata in return_field_from_args."
                .to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<arrow_schema::Field>> {
        let geometry_type = output_geometry_type(&args.arg_fields[0], self.coord_type)?;
        Ok(output_field(&geometry_type))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(dump_rings_impl(args, self.coord_type)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DUMP_RINGS_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a list of (path, geom) records for the rings of a POLYGON. \
                 The exterior ring has path index 0 and interior rings follow with 1-based \
                 hole indexes. Non-polygon inputs raise an error.",
                "ST_DumpRings(polygon)",
            )
            .with_argument("polygon", "input polygon")
            .build()
        }))
    }
}

fn dump_rings_impl(
    args: ScalarFunctionArgs,
    fallback_coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let geo_array = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let geometry_type = output_geometry_type(&args.arg_fields[0], fallback_coord_type)?;
    let geo_array_ref = geo_array.as_ref();
    let result = downcast_geoarrow_array!(geo_array_ref, dump_rings_array, &geometry_type)?;
    Ok(ColumnarValue::Array(Arc::new(result)))
}

fn dump_rings_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    geometry_type: &GeometryType,
) -> GeoDataFusionResult<arrow_array::ListArray> {
    let mut geom_builder = GeometryBuilder::new(geometry_type.clone());
    let mut path_builder = ListBuilder::new(Int32Builder::new()).with_field(path_values_field());
    let mut path = Vec::new();
    let mut row_lengths = Vec::with_capacity(array.len());
    let mut row_validity = NullBufferBuilder::new(array.len());

    for item in array.iter() {
        let start = path_builder.len();
        match item.transpose()? {
            Some(geometry) => {
                row_validity.append_non_null();
                let geo_traits::GeometryType::Polygon(polygon) = geometry.as_type() else {
                    return Err(DataFusionError::Execution(
                        "ST_DumpRings input must be a POLYGON".to_string(),
                    )
                    .into());
                };
                let mut ring_number = 0;
                if let Some(ring) = polygon.exterior() {
                    path.push(ring_number);
                    emit_ring(&ring, &path, &mut geom_builder, &mut path_builder)?;
                    path.pop();
                    ring_number += 1;
                }
                for ring in polygon.interiors() {
                    path.push(ring_number);
                    emit_ring(&ring, &path, &mut geom_builder, &mut path_builder)?;
                    path.pop();
                    ring_number += 1;
                }
            }
            None => row_validity.append_null(),
        }
        row_lengths.push(path_builder.len() - start);
    }

    Ok(finish_dump_list(
        geom_builder,
        path_builder,
        row_lengths,
        row_validity,
        geometry_type,
    ))
}

fn emit_ring<L: LineStringTrait<T = f64>>(
    ring: &L,
    path: &[i32],
    geom_builder: &mut GeometryBuilder,
    path_builder: &mut ListBuilder<Int32Builder>,
) -> GeoDataFusionResult<()> {
    let line_string = wkt::types::LineString::new(
        ring.coords().map(|coord| coord_to_wkt(&coord)).collect(),
        wkt_dimension(ring.dim()),
    );
    let polygon = wkt::types::Polygon::new(vec![line_string], wkt_dimension(ring.dim()));
    push_dump_item(geom_builder, path_builder, path, &polygon)
}
