use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::builder::Int32Builder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{ScalarValue, plan_err};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TableType, Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use geo_traits::{CoordTrait, GeometryTrait, GeometryType as GeoTraitsGeometryType, PointTrait};
use geoarrow_array::builder::PolygonBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowError;
use geoarrow_schema::{
    BoxType, CoordType, Dimension, GeoArrowType, GeometryCollectionType, GeometryType,
    LineStringType, Metadata, MultiLineStringType, MultiPointType, MultiPolygonType, PointType,
    PolygonType, WkbType,
};
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString,
    Polygon as WktPolygon,
};

use crate::data_types::any_geometry_type;
use crate::udf::native::bounding_box::util::total_bounds;
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata, geometry_array_from_arrow,
};

const HEX_HEIGHT_FACTOR: f64 = 0.8660254037844387;
const HEX_X: [f64; 7] = [-1.0, -0.5, 0.5, 1.0, 0.5, -0.5, -1.0];
const HEX_Y: [f64; 7] = [0.0, -0.5, -0.5, 0.0, 0.5, 0.5, 0.0];

static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::user_defined(Volatility::Immutable));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GridShape {
    Hexagon,
    Square,
}

#[derive(Debug, Clone, Copy)]
struct Bounds2D {
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
}

#[derive(Debug, Clone)]
struct GridPlan {
    shape: GridShape,
    size: f64,
    i_min: i32,
    i_max: i32,
    j_min_even: i32,
    j_max_even: i32,
    j_min_odd: i32,
    j_max_odd: i32,
}

#[derive(Debug)]
struct GridTable {
    schema: SchemaRef,
    geometry_type: GeometryType,
    plan: Option<GridPlan>,
}

impl GridTable {
    fn generate_batches(&self, state: &dyn Session) -> Result<Vec<RecordBatch>> {
        let Some(plan) = self.plan.as_ref() else {
            return Ok(Vec::new());
        };
        let batch_size = state.config_options().execution.batch_size.get().max(1);

        let mut batches = Vec::new();
        let mut geometry = StrictGeometryBuilder::new(self.geometry_type.clone());
        let mut i_builder = Int32Builder::new();
        let mut j_builder = Int32Builder::new();
        let mut batch_rows = 0usize;

        let mut i = i64::from(plan.i_min);
        while i <= i64::from(plan.i_max) {
            let cell_i = i as i32;
            let (j_min, j_max) = if plan.shape == GridShape::Hexagon && cell_i.rem_euclid(2) == 1 {
                (plan.j_min_odd, plan.j_max_odd)
            } else {
                (plan.j_min_even, plan.j_max_even)
            };

            let mut j = i64::from(j_min);
            while j <= i64::from(j_max) {
                let cell_j = j as i32;
                let polygon = match plan.shape {
                    GridShape::Hexagon => hexagon_polygon(0.0, 0.0, plan.size, cell_i, cell_j),
                    GridShape::Square => square_polygon(0.0, 0.0, plan.size, cell_i, cell_j),
                };
                geometry
                    .push_geometry(Some(&polygon))
                    .map_err(|error| geoarrow_error("grid cell construction failed", error))?;
                i_builder.append_value(cell_i);
                j_builder.append_value(cell_j);
                batch_rows += 1;

                if batch_rows == batch_size {
                    batches.push(finish_batch(
                        &self.schema,
                        geometry,
                        &mut i_builder,
                        &mut j_builder,
                    )?);
                    geometry = StrictGeometryBuilder::new(self.geometry_type.clone());
                    i_builder = Int32Builder::new();
                    j_builder = Int32Builder::new();
                    batch_rows = 0;
                }
                j += 1;
            }
            i += 1;
        }

        if batch_rows > 0 {
            batches.push(finish_batch(
                &self.schema,
                geometry,
                &mut i_builder,
                &mut j_builder,
            )?);
        }
        Ok(batches)
    }
}

fn finish_batch(
    schema: &SchemaRef,
    geometry: StrictGeometryBuilder,
    i_builder: &mut Int32Builder,
    j_builder: &mut Int32Builder,
) -> Result<RecordBatch> {
    let geometry = Arc::new(geometry.finish()).to_array_ref();
    let i = Arc::new(i_builder.finish());
    let j = Arc::new(j_builder.finish());
    let options = RecordBatchOptions::new().with_row_count(Some(i.len()));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        vec![geometry, i, j],
        &options,
    )?)
}

impl TableProvider for GridTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 Vec<usize>>,
        _filters: &'life3 [Expr],
        _limit: Option<usize>,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<dyn ExecutionPlan>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let batches = self.generate_batches(state)?;
            let partitions = vec![batches];
            let exec: Arc<dyn ExecutionPlan> =
                MemorySourceConfig::try_new_exec(&partitions, self.schema(), projection.cloned())?;
            Ok(exec)
        })
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Hexagon;

impl Default for Hexagon {
    fn default() -> Self {
        Self
    }
}

impl ScalarUDFImpl for Hexagon {
    fn name(&self) -> &str {
        "st_hexagon"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        scalar_return_field(&args, "ST_Hexagon")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        grid_scalar_impl(args, GridShape::Hexagon, "ST_Hexagon")
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_scalar_types(arg_types, "ST_Hexagon")
    }

    fn documentation(&self) -> Option<&Documentation> {
        static DOC: OnceLock<Documentation> = OnceLock::new();
        Some(DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a single hexagon using the provided edge size and grid cell coordinate.",
                "ST_Hexagon(size, cell_i, cell_j[, origin])",
            )
            .with_argument("size", "hexagon edge size")
            .with_argument("cell_i", "integer cell column")
            .with_argument("cell_j", "integer cell row")
            .with_argument("origin", "optional origin point, default POINT(0 0)")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Square;

impl Default for Square {
    fn default() -> Self {
        Self
    }
}

impl ScalarUDFImpl for Square {
    fn name(&self) -> &str {
        "st_square"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        scalar_return_field(&args, "ST_Square")
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        grid_scalar_impl(args, GridShape::Square, "ST_Square")
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_scalar_types(arg_types, "ST_Square")
    }

    fn documentation(&self) -> Option<&Documentation> {
        static DOC: OnceLock<Documentation> = OnceLock::new();
        Some(DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a single square using the provided edge size and grid cell coordinate.",
                "ST_Square(size, cell_i, cell_j[, origin])",
            )
            .with_argument("size", "square edge size")
            .with_argument("cell_i", "integer cell column")
            .with_argument("cell_j", "integer cell row")
            .with_argument("origin", "optional origin point, default POINT(0 0)")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct HexagonGrid;

impl Default for HexagonGrid {
    fn default() -> Self {
        Self
    }
}

impl TableFunctionImpl for HexagonGrid {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        call_grid_table(args, GridShape::Hexagon, "ST_HexagonGrid")
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SquareGrid;

impl Default for SquareGrid {
    fn default() -> Self {
        Self
    }
}

impl TableFunctionImpl for SquareGrid {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        call_grid_table(args, GridShape::Square, "ST_SquareGrid")
    }
}

fn call_grid_table(
    args: TableFunctionArgs,
    shape: GridShape,
    name: &'static str,
) -> Result<Arc<dyn TableProvider>> {
    let exprs = args.exprs();
    if exprs.len() != 2 {
        return plan_err!("{name} expects exactly 2 arguments");
    }

    let size = literal_f64(&exprs[0], name, "size")?;
    let (bounds, metadata) = literal_bounds(&exprs[1], name)?;
    let geometry_type = GeometryType::new(metadata);
    let schema = Arc::new(Schema::new(vec![
        Arc::new(geometry_type.to_field("geom", false)),
        Arc::new(Field::new("i", DataType::Int32, false)),
        Arc::new(Field::new("j", DataType::Int32, false)),
    ]));
    let plan = match (size, bounds) {
        (Some(size), _) if !size.is_finite() => {
            return plan_err!("{name} requires a finite size, got {size}");
        }
        (Some(size), Some(bounds)) if size > 0.0 => Some(grid_plan(shape, size, bounds, name)?),
        _ => None,
    };

    Ok(Arc::new(GridTable {
        schema,
        geometry_type,
        plan,
    }))
}

fn scalar_return_field(args: &ReturnFieldArgs, name: &str) -> Result<FieldRef> {
    let metadata = match args.arg_fields.get(3) {
        Some(field) => planar_metadata(field_metadata(field.as_ref()).map_err(|error| {
            DataFusionError::Plan(format!("{name}: invalid origin metadata: {error}"))
        })?),
        None => Arc::new(Metadata::default()),
    };
    Ok(Arc::new(
        PolygonType::new(Dimension::XY, metadata).to_field("", true),
    ))
}

fn coerce_scalar_types(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    if !matches!(arg_types.len(), 3 | 4) {
        return plan_err!("{name} expects 3 or 4 arguments, got {}", arg_types.len());
    }

    let mut coerced = Vec::with_capacity(arg_types.len());
    coerced.push(coerce_f64(&arg_types[0], name, 1)?);
    coerced.push(coerce_i64(&arg_types[1], name, 2)?);
    coerced.push(coerce_i64(&arg_types[2], name, 3)?);
    if let Some(origin_type) = arg_types.get(3) {
        if origin_type != &DataType::Null
            && !any_geometry_type()
                .iter()
                .any(|data_type| data_type == origin_type)
        {
            return plan_err!("{name} argument 4 must be a geometry, got {origin_type}");
        }
        coerced.push(origin_type.clone());
    }
    Ok(coerced)
}

fn coerce_f64(data_type: &DataType, name: &str, position: usize) -> Result<DataType> {
    if data_type == &DataType::Null || data_type.is_numeric() {
        Ok(DataType::Float64)
    } else {
        plan_err!("{name} argument {position} must be numeric, got {data_type}")
    }
}

fn coerce_i64(data_type: &DataType, name: &str, position: usize) -> Result<DataType> {
    if data_type == &DataType::Null || data_type.is_integer() {
        Ok(DataType::Int64)
    } else {
        plan_err!("{name} argument {position} must be an integer, got {data_type}")
    }
}

fn grid_scalar_impl(
    args: ScalarFunctionArgs,
    shape: GridShape,
    name: &'static str,
) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let return_type = args.return_field.extension_type::<PolygonType>();
    let mut builder = PolygonBuilder::new(return_type);
    let rows = arrays
        .iter()
        .map(|array| array.len())
        .max()
        .unwrap_or(args.number_rows);

    if arrays
        .iter()
        .take(3)
        .any(|array| array.data_type() == &DataType::Null)
    {
        for _ in 0..rows {
            builder
                .push_polygon(None::<&WktPolygon<f64>>)
                .map_err(|error| geoarrow_error(name, error))?;
        }
        return Ok(ColumnarValue::Array(builder.finish().into_array_ref()));
    }

    let size = arrays[0].as_primitive::<Float64Type>();
    let cell_i = arrays[1].as_primitive::<Int64Type>();
    let cell_j = arrays[2].as_primitive::<Int64Type>();
    let origin_coords = match arrays.get(3) {
        None => None,
        Some(array) if array.data_type() == &DataType::Null => Some(Vec::new()),
        Some(array) => {
            let geo = geometry_array_from_arrow(array, &args.arg_fields[3])
                .map_err(|error| DataFusionError::Execution(format!("{name}: {error}")))?;
            Some(collect_origin_coords(geo.as_ref(), name)?)
        }
    };

    for row in 0..rows {
        let row_is_null = size.is_null(row)
            || cell_i.is_null(row)
            || cell_j.is_null(row)
            || origin_coords
                .as_ref()
                .is_some_and(|coords| coords.get(row).is_none_or(Option::is_none));
        if row_is_null {
            builder
                .push_polygon(None::<&WktPolygon<f64>>)
                .map_err(|error| geoarrow_error(name, error))?;
            continue;
        }

        let (origin_x, origin_y) = origin_coords
            .as_ref()
            .and_then(|coords| coords[row])
            .unwrap_or((0.0, 0.0));
        let cell_i = checked_index(cell_i.value(row), name, "cell_i")?;
        let cell_j = checked_index(cell_j.value(row), name, "cell_j")?;
        let polygon = match shape {
            GridShape::Hexagon => {
                hexagon_polygon(origin_x, origin_y, size.value(row), cell_i, cell_j)
            }
            GridShape::Square => {
                square_polygon(origin_x, origin_y, size.value(row), cell_i, cell_j)
            }
        };
        builder
            .push_polygon(Some(&polygon))
            .map_err(|error| geoarrow_error(name, error))?;
    }

    Ok(ColumnarValue::Array(builder.finish().into_array_ref()))
}

fn collect_origin_coords(
    array: &dyn GeoArrowArray,
    name: &'static str,
) -> Result<Vec<Option<(f64, f64)>>> {
    downcast_geoarrow_array!(array, collect_origin_coords_impl, name)
}

fn collect_origin_coords_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    name: &'static str,
) -> Result<Vec<Option<(f64, f64)>>> {
    let mut coords = Vec::with_capacity(array.len());
    for geometry in array.iter() {
        let Some(geometry) = geometry
            .transpose()
            .map_err(|error| geoarrow_error(name, error))?
        else {
            coords.push(None);
            continue;
        };
        let point = match geometry.as_type() {
            GeoTraitsGeometryType::Point(point) => point,
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "{name}: origin argument is not a point"
                )));
            }
        };
        let Some(coord) = point.coord() else {
            return Err(DataFusionError::Execution(format!(
                "{name}: origin point is empty"
            )));
        };
        coords.push(Some((coord.x(), coord.y())));
    }
    Ok(coords)
}

fn literal_f64(expr: &Expr, name: &str, argument: &str) -> Result<Option<f64>> {
    let Expr::Literal(scalar, _) = expr else {
        return plan_err!("{name} requires a constant {argument} argument");
    };
    if scalar.is_null() {
        return Ok(None);
    }
    if !scalar.data_type().is_numeric() {
        return plan_err!(
            "{name} {argument} must be numeric, got {}",
            scalar.data_type()
        );
    }
    let scalar = scalar.cast_to(&DataType::Float64).map_err(|error| {
        DataFusionError::Plan(format!("{name} {argument} must be numeric: {error}"))
    })?;
    match scalar {
        ScalarValue::Float64(value) => Ok(value),
        other => plan_err!(
            "{name} {argument} must be numeric, got {}",
            other.data_type()
        ),
    }
}

fn literal_bounds(expr: &Expr, name: &str) -> Result<(Option<Bounds2D>, Arc<Metadata>)> {
    let Expr::Literal(scalar, literal_metadata) = expr else {
        return plan_err!("{name} requires a constant bounds geometry argument");
    };
    let literal_field = literal_metadata
        .clone()
        .unwrap_or_default()
        .add_to_field(Field::new("bounds", scalar.data_type(), true));
    // Table-function arguments reach this crate as protobuf-serialized
    // expressions, and `datafusion-proto` drops `Expr::Literal`'s field
    // metadata. A bounds literal that crossed the FFI therefore arrives without
    // a GeoArrow extension name; recover the geometry type from the Arrow data
    // type instead of rejecting a call that is valid in Rust SQL. A NULL
    // literal has no geometry type to recover, and `field_metadata` already
    // reads `DataType::Null` as the default metadata.
    let field = if literal_field.extension_type_name().is_some() || scalar.is_null() {
        literal_field
    } else {
        let data_type = scalar.data_type();
        untyped_geometry_field(&data_type).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{name}: unsupported bounds geometry data type: {data_type}"
            ))
        })?
    };
    let metadata = planar_metadata(field_metadata(&field).map_err(|error| {
        DataFusionError::Plan(format!("{name}: invalid bounds metadata: {error}"))
    })?);
    if scalar.is_null() {
        return Ok((None, metadata));
    }

    let array = scalar.to_array_of_size(1)?;
    let geometry = geometry_array_from_arrow(array.as_ref(), &field).map_err(|error| {
        DataFusionError::Plan(format!("{name}: unsupported bounds geometry: {error}"))
    })?;
    let bounds = total_bounds(geometry.as_ref()).map_err(|error| {
        DataFusionError::Execution(format!("{name}: could not compute bounds: {error}"))
    })?;
    let bounds = Bounds2D {
        xmin: bounds.minx(),
        ymin: bounds.miny(),
        xmax: bounds.maxx(),
        ymax: bounds.maxy(),
    };
    let finite = [bounds.xmin, bounds.ymin, bounds.xmax, bounds.ymax]
        .into_iter()
        .all(f64::is_finite);
    if finite && bounds.xmin <= bounds.xmax && bounds.ymin <= bounds.ymax {
        Ok((Some(bounds), metadata))
    } else {
        Ok((None, metadata))
    }
}

fn planar_metadata(metadata: Arc<Metadata>) -> Arc<Metadata> {
    Arc::new(Metadata::new(metadata.crs().clone(), None))
}

/// Rebuild a GeoArrow field from an Arrow data type alone.
///
/// Arrow shapes do not identify a geometry uniquely: a polygon and a
/// multi-line string share one shape, a line string and a multi-point share
/// another, and the separated and interleaved coordinate layouts of the same
/// dimension share sizes. The caller only computes a bounding box, so the first
/// shape match is as good as the extension name that was lost. Note that the
/// generic GeoArrow geometry type is a union over every kind *and* dimension,
/// so the geometry literals built by the string constructors match here too.
fn untyped_geometry_field(data_type: &DataType) -> Option<Field> {
    let metadata = Arc::new(Metadata::default());
    let mut candidates = Vec::new();
    for coord_type in [CoordType::Separated, CoordType::Interleaved] {
        for dimension in [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ] {
            candidates.extend([
                GeoArrowType::Point(
                    PointType::new(dimension, Arc::clone(&metadata)).with_coord_type(coord_type),
                ),
                GeoArrowType::LineString(
                    LineStringType::new(dimension, Arc::clone(&metadata))
                        .with_coord_type(coord_type),
                ),
                GeoArrowType::Polygon(
                    PolygonType::new(dimension, Arc::clone(&metadata)).with_coord_type(coord_type),
                ),
                GeoArrowType::MultiPoint(
                    MultiPointType::new(dimension, Arc::clone(&metadata))
                        .with_coord_type(coord_type),
                ),
                GeoArrowType::MultiLineString(
                    MultiLineStringType::new(dimension, Arc::clone(&metadata))
                        .with_coord_type(coord_type),
                ),
                GeoArrowType::MultiPolygon(
                    MultiPolygonType::new(dimension, Arc::clone(&metadata))
                        .with_coord_type(coord_type),
                ),
                GeoArrowType::GeometryCollection(
                    GeometryCollectionType::new(dimension, Arc::clone(&metadata))
                        .with_coord_type(coord_type),
                ),
                GeoArrowType::Rect(BoxType::new(dimension, Arc::clone(&metadata))),
            ]);
        }
        candidates.push(GeoArrowType::Geometry(
            GeometryType::new(Arc::clone(&metadata)).with_coord_type(coord_type),
        ));
    }
    candidates.push(GeoArrowType::Wkb(WkbType::new(Arc::clone(&metadata))));

    candidates.into_iter().find_map(|candidate| {
        let field = candidate.to_field("bounds", true);
        (field.data_type() == data_type).then_some(field)
    })
}

fn grid_plan(
    shape: GridShape,
    size: f64,
    bounds: Bounds2D,
    name: &'static str,
) -> Result<GridPlan> {
    match shape {
        GridShape::Square => {
            let i_min = checked_cell((bounds.xmin / size).floor(), name, "column")?;
            let i_max = checked_cell((bounds.xmax / size).floor(), name, "column")?;
            let j_min = checked_cell((bounds.ymin / size).floor(), name, "row")?;
            let j_max = checked_cell((bounds.ymax / size).floor(), name, "row")?;
            Ok(GridPlan {
                shape,
                size,
                i_min,
                i_max,
                j_min_even: j_min,
                j_max_even: j_max,
                j_min_odd: j_min,
                j_max_odd: j_max,
            })
        }
        GridShape::Hexagon => {
            let col_width = 1.5 * size;
            let row_height = size * 2.0 * HEX_HEIGHT_FACTOR;

            let mut column_min = (bounds.xmin / col_width).floor();
            if bounds.xmin - column_min * col_width > size {
                column_min += 1.0;
            }
            let mut column_max = (bounds.xmax / col_width).ceil();
            if column_max * col_width - bounds.xmax > size {
                column_max -= 1.0;
            }

            let j_min_even = checked_cell((bounds.ymin / row_height + 0.5).floor(), name, "row")?;
            let j_max_even = checked_cell((bounds.ymax / row_height + 0.5).floor(), name, "row")?;
            let j_min_odd = checked_cell((bounds.ymin / row_height).floor(), name, "row")?;
            let j_max_odd = checked_cell((bounds.ymax / row_height).floor(), name, "row")?;
            Ok(GridPlan {
                shape,
                size,
                i_min: checked_cell(column_min, name, "column")?,
                i_max: checked_cell(column_max, name, "column")?,
                j_min_even,
                j_max_even,
                j_min_odd,
                j_max_odd,
            })
        }
    }
}

fn checked_cell(value: f64, name: &str, kind: &str) -> Result<i32> {
    if !value.is_finite() {
        return plan_err!("{name}: {kind} index is not finite");
    }
    if value < f64::from(i32::MIN) || value > f64::from(i32::MAX) {
        return plan_err!("{name}: {kind} index {value} is outside the Int32 range");
    }
    Ok(value as i32)
}

fn checked_index(value: i64, name: &str, kind: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| {
        DataFusionError::Execution(format!(
            "{name}: {kind} index {value} is outside the Int32 range"
        ))
    })
}

fn hexagon_polygon(
    origin_x: f64,
    origin_y: f64,
    size: f64,
    cell_i: i32,
    cell_j: i32,
) -> WktPolygon<f64> {
    let height = size * 2.0 * HEX_HEIGHT_FACTOR;
    let row_offset = f64::from(cell_j) + 0.5 * f64::from(cell_i.rem_euclid(2));
    let coords = (0..7)
        .map(|index| WktCoord {
            x: origin_x + size * (1.5 * f64::from(cell_i) + HEX_X[index]),
            y: origin_y + height * (row_offset + HEX_Y[index]),
            z: None,
            m: None,
        })
        .collect::<Vec<_>>();
    WktPolygon::new(
        vec![WktLineString::new(coords, WktDimension::XY)],
        WktDimension::XY,
    )
}

fn square_polygon(
    origin_x: f64,
    origin_y: f64,
    size: f64,
    cell_i: i32,
    cell_j: i32,
) -> WktPolygon<f64> {
    let lower_x = origin_x + size * f64::from(cell_i);
    let lower_y = origin_y + size * f64::from(cell_j);
    let upper_x = origin_x + size * f64::from(cell_i + 1);
    let upper_y = origin_y + size * f64::from(cell_j + 1);
    let coord = |x, y| WktCoord {
        x,
        y,
        z: None,
        m: None,
    };
    WktPolygon::new(
        vec![WktLineString::new(
            vec![
                coord(lower_x, lower_y),
                coord(lower_x, upper_y),
                coord(upper_x, upper_y),
                coord(upper_x, lower_y),
                coord(lower_x, lower_y),
            ],
            WktDimension::XY,
        )],
        WktDimension::XY,
    )
}

fn geoarrow_error(context: &str, error: GeoArrowError) -> DataFusionError {
    DataFusionError::Execution(format!("{context}: {error}"))
}
