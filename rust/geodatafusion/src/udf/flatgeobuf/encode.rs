use std::io::Cursor;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::arrow::compute::{cast, filter_record_batch};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use flatgeobuf::packed_r_tree::{NodeItem, PackedRTree, calc_extent, hilbert_sort};
use flatgeobuf::{
    Column, ColumnArgs, ColumnType, Crs, CrsArgs, Feature, FeatureArgs, Geometry as FgbGeometry,
    GeometryArgs, GeometryType as FgbGeometryType, Header, HeaderArgs,
};
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, LineStringTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
};
use geoarrow_array::array::{
    LargeWkbArray, LargeWktArray, WkbArray, WkbViewArray, WktArray, WktViewArray,
};
use geoarrow_array::cast::AsGeoArrowArray;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::{CrsType, GeoArrowType, GeometryType, Metadata, WkbType, WktType};

use crate::udf::flatgeobuf::schema::{flattened_schema, geometry_type, row_schema};
use crate::udf::native::geometry::geometry_array_from_arrow;
use crate::udf::native::io::decode_geometry_array;

const FGB_MAGIC_BYTES: [u8; 8] = [b'f', b'g', b'b', 3, b'f', b'g', b'b', 1];

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct AsFlatGeobuf {
    signature: Signature,
}

impl AsFlatGeobuf {
    pub(super) fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for AsFlatGeobuf {
    fn name(&self) -> &str {
        "st_asflatgeobuf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_AsFlatGeobuf return_type must use fields".to_string(),
        ))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        validate_schema(arg_fields)?;
        Ok(Arc::new(Field::new("", DataType::Binary, true)))
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let row_field = args
            .expr_fields
            .first()
            .ok_or_else(|| DataFusionError::Internal("missing row argument".to_string()))?;
        let (row_schema, input, write_index, flattened_value_count) = if !is_geometry_field(
            row_field,
        ) && matches!(
            row_field.data_type(),
            DataType::Struct(_)
        ) {
            let fields = struct_fields(row_field)?;
            let write_index = if args.expr_fields.len() > 1 {
                constant_bool(&args.exprs[1], args.schema)?.unwrap_or(false)
            } else {
                false
            };
            let geom_name = if args.expr_fields.len() > 2 {
                constant_text(&args.exprs[2], args.schema)?
            } else {
                None
            };
            (
                row_schema(fields, geom_name.as_deref())?,
                AccumulatorInput::Row,
                write_index,
                0,
            )
        } else {
            let (fields, write_index, flattened_value_count) = if flattened_index_options(&args) {
                if args.expr_fields.len() == 3 {
                    return Err(DataFusionError::Execution(
                        "ST_AsFlatGeobuf: the geom_name option is not supported by the \
                         flattened form because its first argument is always the geometry \
                         column; drop the third argument, or pass a struct row that carries \
                         GeoArrow metadata"
                            .to_string(),
                    ));
                }
                (
                    &args.expr_fields[..1],
                    constant_bool(&args.exprs[1], args.schema)?.unwrap_or(false),
                    1,
                )
            } else {
                (args.expr_fields, false, args.expr_fields.len())
            };
            (
                flattened_schema(fields)?,
                AccumulatorInput::Flattened,
                write_index,
                flattened_value_count,
            )
        };

        Ok(Box::new(AsFlatGeobufAccumulator::new(
            row_schema,
            input,
            write_index,
            flattened_value_count,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        validate_schema(args.input_fields)?;
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "state"),
            DataType::Binary,
            true,
        ))])
    }
}

/// PostGIS accepts `(row, index)` and `(row, index, geom_name)` variants. The
/// flattened DataFusion form uses the same trailing constant arguments when
/// the first argument is already a geometry column.
fn flattened_index_options(args: &AccumulatorArgs) -> bool {
    if !matches!(args.expr_fields.len(), 2 | 3) || !is_geometry_field(&args.expr_fields[0]) {
        return false;
    }
    if constant_bool(&args.exprs[1], args.schema)
        .ok()
        .flatten()
        .is_none()
    {
        return false;
    }
    args.expr_fields.len() == 2
        || constant_text(&args.exprs[2], args.schema)
            .ok()
            .flatten()
            .is_some()
}

fn validate_schema(fields: &[FieldRef]) -> Result<()> {
    if fields.is_empty() {
        return Err(DataFusionError::Execution(
            "ST_AsFlatGeobuf expects at least one argument".to_string(),
        ));
    }
    if !is_geometry_field(&fields[0]) && matches!(fields[0].data_type(), DataType::Struct(_)) {
        if fields.len() > 3 {
            return Err(DataFusionError::Execution(format!(
                "ST_AsFlatGeobuf expects one to three arguments for a row, got {}",
                fields.len()
            )));
        }
        if fields.len() > 1 && !matches!(fields[1].data_type(), DataType::Boolean | DataType::Null)
        {
            return Err(DataFusionError::Execution(
                "ST_AsFlatGeobuf: second argument must be boolean".to_string(),
            ));
        }
        if fields.len() > 2
            && !matches!(
                fields[2].data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            )
        {
            return Err(DataFusionError::Execution(
                "ST_AsFlatGeobuf: third argument must be text".to_string(),
            ));
        }
        let fields = struct_fields(&fields[0])?;
        row_schema(fields, None)?;
        return Ok(());
    }

    if flattened_option_shape(fields) {
        flattened_schema(&fields[..1])?;
    } else {
        flattened_schema(fields)?;
    }
    Ok(())
}

fn flattened_option_shape(fields: &[FieldRef]) -> bool {
    matches!(fields.len(), 2 | 3)
        && is_geometry_field(&fields[0])
        && matches!(fields[1].data_type(), DataType::Boolean | DataType::Null)
        && (fields.len() == 2
            || matches!(
                fields[2].data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
            ))
}

fn is_geometry_field(field: &Field) -> bool {
    GeoArrowType::from_extension_field(field)
        .map(|geometry| geometry.is_some())
        .unwrap_or(false)
}

fn struct_fields(field: &Field) -> Result<&[FieldRef]> {
    match field.data_type() {
        DataType::Struct(fields) => Ok(fields),
        other => Err(DataFusionError::Execution(format!(
            "ST_AsFlatGeobuf: first argument must be a struct row, got {other}"
        ))),
    }
}

fn empty_batch() -> RecordBatch {
    RecordBatch::new_empty(Arc::new(Schema::empty()))
}

fn constant_bool(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Option<bool>> {
    let _ = schema;
    let value = expr.evaluate(&empty_batch())?;
    let arrays = ColumnarValue::values_to_arrays(&[value])?;
    let array = arrays[0].as_boolean_opt().ok_or_else(|| {
        DataFusionError::Execution(
            "ST_AsFlatGeobuf: index argument must be a constant boolean".to_string(),
        )
    })?;
    if array.is_null(0) {
        Ok(None)
    } else {
        Ok(Some(array.value(0)))
    }
}

fn constant_text(expr: &Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Option<String>> {
    let _ = schema;
    let value = expr.evaluate(&empty_batch())?;
    let arrays = ColumnarValue::values_to_arrays(&[value])?;
    let array = &arrays[0];
    if array.is_null(0) {
        return Ok(None);
    }
    match array.data_type() {
        DataType::Utf8 => Ok(Some(array.as_string::<i32>().value(0).to_string())),
        DataType::LargeUtf8 => Ok(Some(array.as_string::<i64>().value(0).to_string())),
        DataType::Utf8View => Ok(Some(array.as_string_view().value(0).to_string())),
        other => Err(DataFusionError::Execution(format!(
            "ST_AsFlatGeobuf: geom_name must be constant text, got {other}"
        ))),
    }
}

#[derive(Debug)]
struct AsFlatGeobufAccumulator {
    schema: SchemaRef,
    input: AccumulatorInput,
    write_index: bool,
    flattened_value_count: usize,
    batches: Vec<RecordBatch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AccumulatorInput {
    Row,
    Flattened,
}

impl AsFlatGeobufAccumulator {
    fn new(
        row_schema: super::schema::FlatGeobufRowSchema,
        input: AccumulatorInput,
        write_index: bool,
        flattened_value_count: usize,
    ) -> Self {
        Self {
            schema: row_schema.schema,
            input,
            write_index,
            flattened_value_count,
            batches: Vec::new(),
        }
    }

    fn encode_flatgeobuf(&self) -> Result<Option<Vec<u8>>> {
        if self.batches.is_empty() {
            return Ok(None);
        }
        encode_batches(&self.schema, &self.batches, self.write_index).map(Some)
    }

    fn encode_state(&self) -> Result<Option<Vec<u8>>> {
        if self.batches.is_empty() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, self.schema.as_ref())
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            for batch in &self.batches {
                writer
                    .write(batch)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            }
            writer
                .finish()
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        }
        Ok(Some(bytes))
    }

    fn append_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() > 0 {
            self.batches.push(batch);
        }
        Ok(())
    }
}

impl Accumulator for AsFlatGeobufAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let batch = match self.input {
            AccumulatorInput::Row => {
                let value = values
                    .first()
                    .ok_or_else(|| DataFusionError::Internal("missing row input".to_string()))?;
                let struct_array = value.as_struct_opt().ok_or_else(|| {
                    DataFusionError::Execution(
                        "ST_AsFlatGeobuf: first argument must be a struct row".to_string(),
                    )
                })?;
                struct_array_to_batch(struct_array, &self.schema)?
            }
            AccumulatorInput::Flattened => {
                let values = values.get(..self.flattened_value_count).ok_or_else(|| {
                    DataFusionError::Execution(
                        "ST_AsFlatGeobuf: missing flattened arguments".to_string(),
                    )
                })?;
                arrays_to_batch(values, &self.schema)?
            }
        };
        self.append_batch(batch)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(self.encode_state()?)])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(self.encode_flatgeobuf()?))
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let state = states
            .first()
            .ok_or_else(|| DataFusionError::Internal("missing aggregate state".to_string()))?;
        let state = state.as_binary_opt::<i32>().ok_or_else(|| {
            DataFusionError::Execution("ST_AsFlatGeobuf: partial state must be binary".to_string())
        })?;

        for row in 0..state.len() {
            if state.is_null(row) {
                continue;
            }
            let reader = StreamReader::try_new(Cursor::new(state.value(row)), None)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            for batch in reader {
                let batch = batch.map_err(|error| DataFusionError::Execution(error.to_string()))?;
                self.append_batch(batch)?;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .batches
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>()
    }
}

fn struct_array_to_batch(struct_array: &StructArray, schema: &SchemaRef) -> Result<RecordBatch> {
    if struct_array.num_columns() != schema.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "ST_AsFlatGeobuf: row has {} columns, expected {}",
            struct_array.num_columns(),
            schema.fields().len()
        )));
    }

    let offset = struct_array.offset();
    let length = struct_array.len();
    let columns = schema
        .fields()
        .iter()
        .zip(struct_array.columns())
        .map(|(field, array)| {
            let array = array.slice(offset, length);
            convert_column(&array, field)
        })
        .collect::<Result<Vec<_>>>()?;
    let batch = RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| {
        DataFusionError::Execution(format!("ST_AsFlatGeobuf: invalid row: {error}"))
    })?;

    if struct_array.null_count() == 0 {
        return Ok(batch);
    }
    let mask = BooleanArray::from_iter((0..length).map(|row| !struct_array.is_null(row)));
    filter_record_batch(&batch, &mask).map_err(|error| {
        DataFusionError::Execution(format!("ST_AsFlatGeobuf: cannot filter null rows: {error}"))
    })
}

fn arrays_to_batch(values: &[ArrayRef], schema: &SchemaRef) -> Result<RecordBatch> {
    if values.len() != schema.fields().len() {
        return Err(DataFusionError::Execution(format!(
            "ST_AsFlatGeobuf: received {} arguments, expected {}",
            values.len(),
            schema.fields().len()
        )));
    }
    let columns = schema
        .fields()
        .iter()
        .zip(values)
        .map(|(field, array)| convert_column(array, field))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|error| {
        DataFusionError::Execution(format!("ST_AsFlatGeobuf: invalid input: {error}"))
    })
}

fn convert_column(array: &ArrayRef, field: &FieldRef) -> Result<ArrayRef> {
    if array.data_type() == field.data_type() {
        return Ok(Arc::clone(array));
    }

    if let Some(GeoArrowType::Geometry(geometry_type)) = geometry_type(field)? {
        return decode_serialized_geometry(array, &geometry_type);
    }

    cast(array, field.data_type()).map_err(|error| {
        DataFusionError::Execution(format!(
            "ST_AsFlatGeobuf: cannot cast `{}` from {} to {}: {error}",
            field.name(),
            array.data_type(),
            field.data_type()
        ))
    })
}

fn decode_serialized_geometry(array: &ArrayRef, geometry_type: &GeometryType) -> Result<ArrayRef> {
    // This decode path parses text storage, so it applies the shared WKT bound.
    crate::udf::native::io::wkt::ensure_wkt_array_depth(array.as_ref())?;
    let metadata = Arc::clone(geometry_type.metadata());
    let decoded = match array.data_type() {
        DataType::Binary => {
            let source = GeoArrowType::Wkb(WkbType::new(metadata)).to_field("geometry", true);
            let source = WkbArray::try_from((array.as_ref(), &source))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            decode_geometry_array(&source, geometry_type.clone(), None)?
        }
        DataType::LargeBinary => {
            let source = GeoArrowType::Wkb(WkbType::new(metadata)).to_field("geometry", true);
            let source = LargeWkbArray::try_from((array.as_ref(), &source))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            decode_geometry_array(&source, geometry_type.clone(), None)?
        }
        DataType::BinaryView => {
            let source = GeoArrowType::Wkb(WkbType::new(metadata)).to_field("geometry", true);
            let source = WkbViewArray::try_from((array.as_ref(), &source))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            decode_geometry_array(&source, geometry_type.clone(), None)?
        }
        DataType::Utf8 => {
            let source = GeoArrowType::Wkt(WktType::new(metadata)).to_field("geometry", true);
            let source = WktArray::try_from((array.as_ref(), &source))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            decode_geometry_array(&source, geometry_type.clone(), None)?
        }
        DataType::LargeUtf8 => {
            let source = GeoArrowType::Wkt(WktType::new(metadata)).to_field("geometry", true);
            let source = LargeWktArray::try_from((array.as_ref(), &source))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            decode_geometry_array(&source, geometry_type.clone(), None)?
        }
        DataType::Utf8View => {
            let source = GeoArrowType::Wkt(WktType::new(metadata)).to_field("geometry", true);
            let source = WktViewArray::try_from((array.as_ref(), &source))
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            decode_geometry_array(&source, geometry_type.clone(), None)?
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "ST_AsFlatGeobuf: cannot decode geometry column from {other} to a native GeoArrow type"
            )));
        }
    };
    Ok(decoded)
}

struct FeatureBuffer {
    bytes: Vec<u8>,
    bounds: NodeItem,
}

fn encode_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    write_index: bool,
) -> Result<Vec<u8>> {
    let geometry_index = geometry_index(schema)?;
    let mut features = Vec::new();
    let mut dataset_type = None;
    let mut dimensions = None;

    for batch in batches {
        if batch.schema_ref().as_ref() != schema.as_ref() {
            return Err(DataFusionError::Internal(
                "ST_AsFlatGeobuf: accumulator batch schema changed".to_string(),
            ));
        }
        let geometry_field = schema.field(geometry_index);
        let geometry = geometry_array_from_arrow(batch.column(geometry_index), geometry_field)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        for row in 0..batch.num_rows() {
            let properties = encode_properties(schema, geometry_index, batch, row)?;
            let mut bounds = NodeItem::create(0);
            let mut fbb = FlatBufferBuilder::new();
            let geometry = build_geometry_value(
                &mut fbb,
                geometry.as_ref(),
                row,
                &mut dataset_type,
                &mut dimensions,
                &mut bounds,
            )?;
            let properties = (!properties.is_empty()).then(|| fbb.create_vector(&properties));
            let feature = Feature::create(
                &mut fbb,
                &FeatureArgs {
                    geometry,
                    properties,
                    ..Default::default()
                },
            );
            fbb.finish_size_prefixed(feature, None);
            let bytes = fbb.finished_data().to_vec();
            if !bounds.min_x.is_finite() || !bounds.min_y.is_finite() {
                bounds = NodeItem::bounds(0.0, 0.0, 0.0, 0.0);
            }
            features.push(FeatureBuffer { bytes, bounds });
        }
    }

    let dimensions = dimensions.unwrap_or(DimensionsInfo::xy());
    let mut output = Vec::new();
    output.extend_from_slice(&FGB_MAGIC_BYTES);

    let (order, extent) = if write_index && !features.is_empty() {
        let mut nodes = features
            .iter()
            .map(|feature| feature.bounds.clone())
            .collect::<Vec<_>>();
        let extent = calc_extent(&nodes);
        for (index, node) in nodes.iter_mut().enumerate() {
            node.offset = index as u64;
        }
        hilbert_sort(&mut nodes, &extent);
        let mut order = Vec::with_capacity(nodes.len());
        let mut offset = 0u64;
        let mut index_nodes = Vec::with_capacity(nodes.len());
        for node in nodes {
            let index = node.offset as usize;
            let mut node = node;
            node.offset = offset;
            offset += features[index].bytes.len() as u64;
            order.push(index);
            index_nodes.push(node);
        }
        (order, Some(extent))
    } else {
        ((0..features.len()).collect(), None)
    };

    output.extend_from_slice(&build_header(
        schema,
        geometry_index,
        dataset_type,
        dimensions,
        features.len(),
        write_index,
        extent.as_ref(),
    )?);

    if write_index && !features.is_empty() {
        let tree = PackedRTree::build(
            &order
                .iter()
                .scan(0u64, |offset, &index| {
                    let node = NodeItem {
                        offset: *offset,
                        ..features[index].bounds.clone()
                    };
                    *offset += features[index].bytes.len() as u64;
                    Some(node)
                })
                .collect::<Vec<_>>(),
            extent.as_ref().expect("index extent exists"),
            16,
        )
        .map_err(|error| {
            DataFusionError::Execution(format!("FlatGeobuf index creation failed: {error}"))
        })?;
        let mut index_bytes = Vec::new();
        tree.stream_write(&mut index_bytes).map_err(|error| {
            DataFusionError::Execution(format!("FlatGeobuf index write failed: {error}"))
        })?;
        output.extend_from_slice(&index_bytes);
    }

    for index in order {
        output.extend_from_slice(&features[index].bytes);
    }
    Ok(output)
}

fn geometry_index(schema: &Schema) -> Result<usize> {
    let mut geometry_indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(
            |(index, field)| match GeoArrowType::from_extension_field(field) {
                Ok(Some(_)) => Some(index),
                _ => None,
            },
        );
    let index = geometry_indices.next().ok_or_else(|| {
        DataFusionError::Internal("ST_AsFlatGeobuf: missing geometry field".to_string())
    })?;
    if geometry_indices.next().is_some() {
        return Err(DataFusionError::Internal(
            "ST_AsFlatGeobuf: multiple geometry fields".to_string(),
        ));
    }
    Ok(index)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DimensionsInfo {
    has_z: bool,
    has_m: bool,
}

impl DimensionsInfo {
    const fn xy() -> Self {
        Self {
            has_z: false,
            has_m: false,
        }
    }

    fn from_geo_traits(dimensions: Dimensions) -> Result<Self> {
        match dimensions {
            Dimensions::Xy | Dimensions::Unknown(2) => Ok(Self::xy()),
            Dimensions::Xyz | Dimensions::Unknown(3) => Ok(Self {
                has_z: true,
                has_m: false,
            }),
            Dimensions::Xym => Ok(Self {
                has_z: false,
                has_m: true,
            }),
            Dimensions::Xyzm | Dimensions::Unknown(4) => Ok(Self {
                has_z: true,
                has_m: true,
            }),
            other => Err(DataFusionError::Execution(format!(
                "ST_AsFlatGeobuf: unsupported coordinate dimensions {other:?}"
            ))),
        }
    }
}

fn update_dimensions(current: &mut Option<DimensionsInfo>, dimensions: Dimensions) -> Result<()> {
    let dimensions = DimensionsInfo::from_geo_traits(dimensions)?;
    match current {
        Some(existing) if *existing != dimensions => Err(DataFusionError::Execution(
            "ST_AsFlatGeobuf: mixed coordinate dimensions are not supported".to_string(),
        )),
        Some(_) => Ok(()),
        None => {
            *current = Some(dimensions);
            Ok(())
        }
    }
}

fn append_coord<C: CoordTrait<T = f64>>(
    coord: &C,
    xy: &mut Vec<f64>,
    z: &mut Vec<f64>,
    m: &mut Vec<f64>,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<()> {
    update_dimensions(dimensions, coord.dim())?;
    let dimensions = dimensions.expect("dimensions set above");
    xy.push(coord.x());
    xy.push(coord.y());
    if dimensions.has_z {
        z.push(coord.nth(2).ok_or_else(|| {
            DataFusionError::Execution("ST_AsFlatGeobuf: coordinate is missing Z".to_string())
        })?);
    }
    if dimensions.has_m {
        // GeoArrow places M at index 2 for XYM, but at index 3 for XYZM.
        let index = if dimensions.has_z { 3 } else { 2 };
        m.push(coord.nth(index).ok_or_else(|| {
            DataFusionError::Execution("ST_AsFlatGeobuf: coordinate is missing M".to_string())
        })?);
    }
    bounds.expand_xy(coord.x(), coord.y());
    Ok(())
}

fn new_geometry<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    xy: Vec<f64>,
    z: Vec<f64>,
    m: Vec<f64>,
    ends: Vec<u32>,
    parts: Vec<WIPOffset<FgbGeometry<'a>>>,
    geometry_type: FgbGeometryType,
) -> WIPOffset<FgbGeometry<'a>> {
    let xy = fbb.create_vector(&xy);
    let ends = (ends.len() > 1).then(|| fbb.create_vector(&ends));
    let z = (!z.is_empty()).then(|| fbb.create_vector(&z));
    let m = (!m.is_empty()).then(|| fbb.create_vector(&m));
    let parts = (!parts.is_empty()).then(|| fbb.create_vector(&parts));
    FgbGeometry::create(
        fbb,
        &GeometryArgs {
            xy: Some(xy),
            ends,
            z,
            m,
            type_: geometry_type,
            parts,
            ..Default::default()
        },
    )
}

fn build_point<'a, G: PointTrait<T = f64>>(
    fbb: &mut FlatBufferBuilder<'a>,
    point: &G,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<WIPOffset<FgbGeometry<'a>>> {
    let mut xy = Vec::new();
    let mut z: Vec<f64> = Vec::new();
    let mut m: Vec<f64> = Vec::new();
    if let Some(coord) = point.coord() {
        append_coord(&coord, &mut xy, &mut z, &mut m, dimensions, bounds)?;
    }
    Ok(new_geometry(
        fbb,
        xy,
        z,
        m,
        Vec::new(),
        Vec::new(),
        FgbGeometryType::Point,
    ))
}

fn build_linestring<'a, G: LineStringTrait<T = f64>>(
    fbb: &mut FlatBufferBuilder<'a>,
    line_string: &G,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<WIPOffset<FgbGeometry<'a>>> {
    let mut xy = Vec::new();
    let mut z = Vec::new();
    let mut m = Vec::new();
    append_line_string(
        line_string,
        dimensions,
        bounds,
        &mut xy,
        &mut z,
        &mut m,
        None,
    )?;
    Ok(new_geometry(
        fbb,
        xy,
        z,
        m,
        Vec::new(),
        Vec::new(),
        FgbGeometryType::LineString,
    ))
}

fn build_polygon_part<'a, G: PolygonTrait<T = f64>>(
    fbb: &mut FlatBufferBuilder<'a>,
    polygon: &G,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<WIPOffset<FgbGeometry<'a>>> {
    let mut xy = Vec::new();
    let mut z = Vec::new();
    let mut m = Vec::new();
    let mut ends = Vec::new();
    if let Some(exterior) = polygon.exterior() {
        append_ring(
            &exterior, &mut xy, &mut z, &mut m, &mut ends, dimensions, bounds,
        )?;
    }
    for interior in polygon.interiors() {
        append_ring(
            &interior, &mut xy, &mut z, &mut m, &mut ends, dimensions, bounds,
        )?;
    }
    Ok(new_geometry(
        fbb,
        xy,
        z,
        m,
        ends,
        Vec::new(),
        FgbGeometryType::Polygon,
    ))
}

fn build_multipoint<'a, G: MultiPointTrait<T = f64>>(
    fbb: &mut FlatBufferBuilder<'a>,
    multi_point: &G,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<WIPOffset<FgbGeometry<'a>>> {
    let mut xy = Vec::new();
    let mut z = Vec::new();
    let mut m = Vec::new();
    for point in multi_point.points() {
        if let Some(coord) = point.coord() {
            append_coord(&coord, &mut xy, &mut z, &mut m, dimensions, bounds)?;
        }
    }
    Ok(new_geometry(
        fbb,
        xy,
        z,
        m,
        Vec::new(),
        Vec::new(),
        FgbGeometryType::MultiPoint,
    ))
}

fn build_multilinestring<'a, G: MultiLineStringTrait<T = f64>>(
    fbb: &mut FlatBufferBuilder<'a>,
    multi_line_string: &G,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<WIPOffset<FgbGeometry<'a>>> {
    let mut xy = Vec::new();
    let mut z = Vec::new();
    let mut m = Vec::new();
    let mut ends = Vec::new();
    for line_string in multi_line_string.line_strings() {
        append_line_string(
            &line_string,
            dimensions,
            bounds,
            &mut xy,
            &mut z,
            &mut m,
            Some(&mut ends),
        )?;
    }
    Ok(new_geometry(
        fbb,
        xy,
        z,
        m,
        ends,
        Vec::new(),
        FgbGeometryType::MultiLineString,
    ))
}

fn build_multipolygon<'a, G: MultiPolygonTrait<T = f64>>(
    fbb: &mut FlatBufferBuilder<'a>,
    multi_polygon: &G,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<WIPOffset<FgbGeometry<'a>>> {
    let mut parts = Vec::new();
    for polygon in multi_polygon.polygons() {
        parts.push(build_polygon_part(fbb, &polygon, dimensions, bounds)?);
    }
    Ok(new_geometry(
        fbb,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        parts,
        FgbGeometryType::MultiPolygon,
    ))
}

fn build_geometry_collection<'a, G: GeometryCollectionTrait<T = f64>>(
    fbb: &mut FlatBufferBuilder<'a>,
    collection: &G,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<WIPOffset<FgbGeometry<'a>>> {
    let mut parts = Vec::new();
    for geometry in collection.geometries() {
        parts.push(build_geometry_from_trait(
            fbb, &geometry, dimensions, bounds,
        )?);
    }
    Ok(new_geometry(
        fbb,
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        parts,
        FgbGeometryType::GeometryCollection,
    ))
}

fn build_rect<'a, G: RectTrait<T = f64>>(
    fbb: &mut FlatBufferBuilder<'a>,
    rect: &G,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<WIPOffset<FgbGeometry<'a>>> {
    let min = rect.min();
    let max = rect.max();
    let mut xy = Vec::new();
    let mut z = Vec::new();
    let mut m = Vec::new();
    for coord in [
        (min.x(), min.y()),
        (max.x(), min.y()),
        (max.x(), max.y()),
        (min.x(), max.y()),
        (min.x(), min.y()),
    ] {
        append_coord(&coord, &mut xy, &mut z, &mut m, dimensions, bounds)?;
    }
    Ok(new_geometry(
        fbb,
        xy,
        z,
        m,
        Vec::new(),
        Vec::new(),
        FgbGeometryType::Polygon,
    ))
}

fn build_geometry_from_trait<'a, G: GeometryTrait<T = f64>>(
    fbb: &mut FlatBufferBuilder<'a>,
    geometry: &G,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<WIPOffset<FgbGeometry<'a>>> {
    match geometry.as_type() {
        geo_traits::GeometryType::Point(point) => build_point(fbb, point, dimensions, bounds),
        geo_traits::GeometryType::LineString(line_string) => {
            build_linestring(fbb, line_string, dimensions, bounds)
        }
        geo_traits::GeometryType::Polygon(polygon) => {
            build_polygon_part(fbb, polygon, dimensions, bounds)
        }
        geo_traits::GeometryType::MultiPoint(multi_point) => {
            build_multipoint(fbb, multi_point, dimensions, bounds)
        }
        geo_traits::GeometryType::MultiLineString(multi_line_string) => {
            build_multilinestring(fbb, multi_line_string, dimensions, bounds)
        }
        geo_traits::GeometryType::MultiPolygon(multi_polygon) => {
            build_multipolygon(fbb, multi_polygon, dimensions, bounds)
        }
        geo_traits::GeometryType::GeometryCollection(collection) => {
            build_geometry_collection(fbb, collection, dimensions, bounds)
        }
        geo_traits::GeometryType::Rect(rect) => build_rect(fbb, rect, dimensions, bounds),
        geo_traits::GeometryType::Triangle(_) | geo_traits::GeometryType::Line(_) => {
            Err(DataFusionError::Execution(
                "ST_AsFlatGeobuf: Triangle and Line geometries are not supported".to_string(),
            ))
        }
    }
}

fn build_geometry_value<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    array: &dyn GeoArrowArray,
    row: usize,
    dataset_type: &mut Option<FgbGeometryType>,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<Option<WIPOffset<FgbGeometry<'a>>>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let (geometry, geometry_type) = match array.data_type() {
        GeoArrowType::Point(_) => {
            let geometry = array
                .as_point()
                .value(row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if geometry.coord().is_none() {
                return Ok(None);
            }
            (
                build_point(fbb, &geometry, dimensions, bounds)?,
                FgbGeometryType::Point,
            )
        }
        GeoArrowType::LineString(_) => {
            let geometry = array
                .as_line_string()
                .value(row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if geometry.num_coords() == 0 {
                return Ok(None);
            }
            (
                build_linestring(fbb, &geometry, dimensions, bounds)?,
                FgbGeometryType::LineString,
            )
        }
        GeoArrowType::Polygon(_) => {
            let geometry = array
                .as_polygon()
                .value(row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if geometry.exterior().is_none() {
                return Ok(None);
            }
            (
                build_polygon_part(fbb, &geometry, dimensions, bounds)?,
                FgbGeometryType::Polygon,
            )
        }
        GeoArrowType::MultiPoint(_) => {
            let geometry = array
                .as_multi_point()
                .value(row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if geometry.num_points() == 0 {
                return Ok(None);
            }
            (
                build_multipoint(fbb, &geometry, dimensions, bounds)?,
                FgbGeometryType::MultiPoint,
            )
        }
        GeoArrowType::MultiLineString(_) => {
            let geometry = array
                .as_multi_line_string()
                .value(row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if geometry.num_line_strings() == 0 {
                return Ok(None);
            }
            (
                build_multilinestring(fbb, &geometry, dimensions, bounds)?,
                FgbGeometryType::MultiLineString,
            )
        }
        GeoArrowType::MultiPolygon(_) => {
            let geometry = array
                .as_multi_polygon()
                .value(row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if geometry.num_polygons() == 0 {
                return Ok(None);
            }
            (
                build_multipolygon(fbb, &geometry, dimensions, bounds)?,
                FgbGeometryType::MultiPolygon,
            )
        }
        GeoArrowType::GeometryCollection(_) => {
            let geometry = array
                .as_geometry_collection()
                .value(row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if geometry_collection_is_empty(&geometry) {
                return Ok(None);
            }
            (
                build_geometry_collection(fbb, &geometry, dimensions, bounds)?,
                FgbGeometryType::GeometryCollection,
            )
        }
        GeoArrowType::Rect(_) => {
            let geometry = array
                .as_rect()
                .value(row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            (
                build_rect(fbb, &geometry, dimensions, bounds)?,
                FgbGeometryType::Polygon,
            )
        }
        GeoArrowType::Geometry(_) => {
            let geometry = array
                .as_geometry()
                .value(row)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            if geometry_is_empty(&geometry)? {
                return Ok(None);
            }
            let geometry_type = top_level_geometry_type(&geometry)?;
            (
                build_geometry_from_trait(fbb, &geometry, dimensions, bounds)?,
                geometry_type,
            )
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "ST_AsFlatGeobuf: unsupported geometry array type {other:?}"
            )));
        }
    };

    match dataset_type {
        Some(expected) if *expected != geometry_type => {
            return Err(DataFusionError::Execution(
                "ST_AsFlatGeobuf: mixed geometry types are not supported".to_string(),
            ));
        }
        Some(_) => {}
        None => *dataset_type = Some(geometry_type),
    }
    Ok(Some(geometry))
}

fn geometry_is_empty<G: GeometryTrait<T = f64>>(geometry: &G) -> Result<bool> {
    Ok(match geometry.as_type() {
        geo_traits::GeometryType::Point(point) => point.coord().is_none(),
        geo_traits::GeometryType::LineString(line_string) => line_string.num_coords() == 0,
        geo_traits::GeometryType::Polygon(polygon) => polygon.exterior().is_none(),
        geo_traits::GeometryType::MultiPoint(multi_point) => multi_point.num_points() == 0,
        geo_traits::GeometryType::MultiLineString(multi_line_string) => {
            multi_line_string.num_line_strings() == 0
        }
        geo_traits::GeometryType::MultiPolygon(multi_polygon) => multi_polygon.num_polygons() == 0,
        geo_traits::GeometryType::GeometryCollection(collection) => {
            geometry_collection_is_empty(collection)
        }
        geo_traits::GeometryType::Rect(_) => false,
        geo_traits::GeometryType::Triangle(_) | geo_traits::GeometryType::Line(_) => {
            return Err(DataFusionError::Execution(
                "ST_AsFlatGeobuf: Triangle and Line geometries are not supported".to_string(),
            ));
        }
    })
}

fn geometry_collection_is_empty<G: GeometryCollectionTrait<T = f64>>(collection: &G) -> bool {
    collection
        .geometries()
        .all(|geometry| geometry_is_empty(&geometry).unwrap_or(false))
}

fn top_level_geometry_type<G: GeometryTrait<T = f64>>(geometry: &G) -> Result<FgbGeometryType> {
    Ok(match geometry.as_type() {
        geo_traits::GeometryType::Point(_) => FgbGeometryType::Point,
        geo_traits::GeometryType::LineString(_) => FgbGeometryType::LineString,
        geo_traits::GeometryType::Polygon(_) | geo_traits::GeometryType::Rect(_) => {
            FgbGeometryType::Polygon
        }
        geo_traits::GeometryType::MultiPoint(_) => FgbGeometryType::MultiPoint,
        geo_traits::GeometryType::MultiLineString(_) => FgbGeometryType::MultiLineString,
        geo_traits::GeometryType::MultiPolygon(_) => FgbGeometryType::MultiPolygon,
        geo_traits::GeometryType::GeometryCollection(_) => FgbGeometryType::GeometryCollection,
        geo_traits::GeometryType::Triangle(_) | geo_traits::GeometryType::Line(_) => {
            return Err(DataFusionError::Execution(
                "ST_AsFlatGeobuf: Triangle and Line geometries are not supported".to_string(),
            ));
        }
    })
}

fn append_line_string(
    line_string: &impl LineStringTrait<T = f64>,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
    xy: &mut Vec<f64>,
    z: &mut Vec<f64>,
    m: &mut Vec<f64>,
    ends: Option<&mut Vec<u32>>,
) -> Result<()> {
    for coord in line_string.coords() {
        append_coord(&coord, xy, z, m, dimensions, bounds)?;
    }
    if let Some(ends) = ends {
        ends.push((xy.len() / 2) as u32);
    }
    Ok(())
}

fn append_ring(
    ring: &impl LineStringTrait<T = f64>,
    xy: &mut Vec<f64>,
    z: &mut Vec<f64>,
    m: &mut Vec<f64>,
    ends: &mut Vec<u32>,
    dimensions: &mut Option<DimensionsInfo>,
    bounds: &mut NodeItem,
) -> Result<()> {
    for coord in ring.coords() {
        append_coord(&coord, xy, z, m, dimensions, bounds)?;
    }
    ends.push((xy.len() / 2) as u32);
    Ok(())
}

fn encode_properties(
    schema: &Schema,
    geometry_index: usize,
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<u8>> {
    let mut properties = Vec::new();
    let mut property_index = 0u16;
    for (column_index, (field, array)) in schema.fields().iter().zip(batch.columns()).enumerate() {
        if column_index == geometry_index {
            continue;
        }
        if !array.is_null(row) {
            append_property(
                &mut properties,
                property_index,
                field.data_type(),
                array,
                row,
            )?;
        }
        property_index = property_index.checked_add(1).ok_or_else(|| {
            DataFusionError::Execution(
                "ST_AsFlatGeobuf: too many property columns for FlatGeobuf".to_string(),
            )
        })?;
    }
    Ok(properties)
}

fn append_property(
    properties: &mut Vec<u8>,
    property_index: u16,
    data_type: &DataType,
    array: &ArrayRef,
    row: usize,
) -> Result<()> {
    properties.extend_from_slice(&property_index.to_le_bytes());
    match data_type {
        DataType::Boolean => properties.push(array.as_boolean().value(row) as u8),
        DataType::Int8 => properties.push(array.as_primitive::<Int8Type>().value(row) as u8),
        DataType::UInt8 => properties.push(array.as_primitive::<UInt8Type>().value(row)),
        DataType::Int16 => properties
            .extend_from_slice(&array.as_primitive::<Int16Type>().value(row).to_le_bytes()),
        DataType::UInt16 => properties
            .extend_from_slice(&array.as_primitive::<UInt16Type>().value(row).to_le_bytes()),
        DataType::Int32 => properties
            .extend_from_slice(&array.as_primitive::<Int32Type>().value(row).to_le_bytes()),
        DataType::UInt32 => properties
            .extend_from_slice(&array.as_primitive::<UInt32Type>().value(row).to_le_bytes()),
        DataType::Int64 => properties
            .extend_from_slice(&array.as_primitive::<Int64Type>().value(row).to_le_bytes()),
        DataType::UInt64 => properties
            .extend_from_slice(&array.as_primitive::<UInt64Type>().value(row).to_le_bytes()),
        DataType::Float32 => properties
            .extend_from_slice(&array.as_primitive::<Float32Type>().value(row).to_le_bytes()),
        DataType::Float64 => properties
            .extend_from_slice(&array.as_primitive::<Float64Type>().value(row).to_le_bytes()),
        DataType::Utf8 => append_string(properties, array.as_string::<i32>().value(row)),
        DataType::Binary => append_binary(properties, array.as_binary::<i32>().value(row)),
        DataType::Timestamp(_, _) => {
            let value = array
                .as_primitive::<TimestampMicrosecondType>()
                .value_as_datetime(row)
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "ST_AsFlatGeobuf: invalid timestamp property".to_string(),
                    )
                })?;
            append_string(properties, &value.and_utc().to_rfc3339())
        }
        other => {
            return Err(DataFusionError::Execution(format!(
                "ST_AsFlatGeobuf: unsupported property type {other}"
            )));
        }
    }
    Ok(())
}

fn append_string(properties: &mut Vec<u8>, value: &str) {
    properties.extend_from_slice(&(value.len() as u32).to_le_bytes());
    properties.extend_from_slice(value.as_bytes());
}

fn append_binary(properties: &mut Vec<u8>, value: &[u8]) {
    properties.extend_from_slice(&(value.len() as u32).to_le_bytes());
    properties.extend_from_slice(value);
}

fn build_header(
    schema: &Schema,
    geometry_index: usize,
    geometry_type: Option<FgbGeometryType>,
    dimensions: DimensionsInfo,
    features_count: usize,
    write_index: bool,
    extent: Option<&NodeItem>,
) -> Result<Vec<u8>> {
    let mut fbb = FlatBufferBuilder::new();

    let mut columns = Vec::new();
    for (index, field) in schema.fields().iter().enumerate() {
        if index == geometry_index {
            continue;
        }
        let name = fbb.create_string(field.name());
        columns.push(Column::create(
            &mut fbb,
            &ColumnArgs {
                name: Some(name),
                type_: column_type(field.data_type())?,
                nullable: field.is_nullable(),
                ..Default::default()
            },
        ));
    }
    let columns = (!columns.is_empty()).then(|| fbb.create_vector(&columns));
    let envelope = extent
        .map(|extent| fbb.create_vector(&[extent.min_x, extent.min_y, extent.max_x, extent.max_y]));
    let crs = build_crs(
        &mut fbb,
        geometry_metadata(schema, geometry_index)?.as_ref(),
    )?;
    let header = Header::create(
        &mut fbb,
        &HeaderArgs {
            envelope,
            geometry_type: geometry_type.unwrap_or(FgbGeometryType::Unknown),
            has_z: dimensions.has_z,
            has_m: dimensions.has_m,
            columns,
            features_count: features_count as u64,
            index_node_size: if write_index && features_count > 0 {
                16
            } else {
                0
            },
            crs,
            ..Default::default()
        },
    );
    fbb.finish_size_prefixed(header, None);
    Ok(fbb.finished_data().to_vec())
}

fn geometry_metadata(schema: &Schema, geometry_index: usize) -> Result<Arc<Metadata>> {
    let geometry_type = GeoArrowType::try_from(schema.field(geometry_index))
        .map_err(|error| DataFusionError::Execution(format!("invalid geometry field: {error}")))?;
    Ok(Arc::clone(geometry_type.metadata()))
}

fn build_crs<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    metadata: &Metadata,
) -> Result<Option<WIPOffset<Crs<'a>>>> {
    let Some(crs) = metadata.crs().crs_value() else {
        return Ok(None);
    };
    let Some(crs_type) = metadata.crs().crs_type() else {
        let wkt = crs.as_str().ok_or_else(|| {
            DataFusionError::Execution(
                "ST_AsFlatGeobuf: CRS must be text, WKT, or an authority code".to_string(),
            )
        })?;
        let wkt = fbb.create_string(wkt);
        return Ok(Some(Crs::create(
            fbb,
            &CrsArgs {
                wkt: Some(wkt),
                ..Default::default()
            },
        )));
    };

    match crs_type {
        CrsType::AuthorityCode | CrsType::Srid => {
            let value = crs.as_str().ok_or_else(|| {
                DataFusionError::Execution(
                    "ST_AsFlatGeobuf: invalid authority CRS metadata".to_string(),
                )
            })?;
            let (org, code) = value
                .split_once(':')
                .map_or(("EPSG", value), |(org, code)| (org, code));
            let org = fbb.create_string(org);
            if let Ok(code) = code.parse::<i32>() {
                Ok(Some(Crs::create(
                    fbb,
                    &CrsArgs {
                        org: Some(org),
                        code,
                        ..Default::default()
                    },
                )))
            } else {
                let code_string = fbb.create_string(code);
                Ok(Some(Crs::create(
                    fbb,
                    &CrsArgs {
                        org: Some(org),
                        code_string: Some(code_string),
                        ..Default::default()
                    },
                )))
            }
        }
        CrsType::Wkt2_2019 => {
            let wkt = crs.as_str().ok_or_else(|| {
                DataFusionError::Execution("ST_AsFlatGeobuf: invalid WKT CRS metadata".to_string())
            })?;
            let wkt = fbb.create_string(wkt);
            Ok(Some(Crs::create(
                fbb,
                &CrsArgs {
                    wkt: Some(wkt),
                    ..Default::default()
                },
            )))
        }
        CrsType::Projjson => Err(DataFusionError::Execution(
            "ST_AsFlatGeobuf: PROJJSON CRS cannot be encoded without a WKT transform".to_string(),
        )),
    }
}

fn column_type(data_type: &DataType) -> Result<ColumnType> {
    match data_type {
        DataType::Boolean => Ok(ColumnType::Bool),
        DataType::Int8 => Ok(ColumnType::Byte),
        DataType::UInt8 => Ok(ColumnType::UByte),
        DataType::Int16 => Ok(ColumnType::Short),
        DataType::UInt16 => Ok(ColumnType::UShort),
        DataType::Int32 => Ok(ColumnType::Int),
        DataType::UInt32 => Ok(ColumnType::UInt),
        DataType::Int64 => Ok(ColumnType::Long),
        DataType::UInt64 => Ok(ColumnType::ULong),
        DataType::Float32 => Ok(ColumnType::Float),
        DataType::Float64 => Ok(ColumnType::Double),
        DataType::Utf8 => Ok(ColumnType::String),
        DataType::Binary => Ok(ColumnType::Binary),
        DataType::Timestamp(_, _) => Ok(ColumnType::DateTime),
        other => Err(DataFusionError::Execution(format!(
            "ST_AsFlatGeobuf: FlatGeobuf cannot represent property type {other}"
        ))),
    }
}
