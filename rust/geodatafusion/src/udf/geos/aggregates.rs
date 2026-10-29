//! GEOS-backed geometry aggregates and their PostGIS scalar overloads.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BinaryArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::utils::get_row_at_idx;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use geo_traits::GeometryTrait;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::{CoordType, GeoArrowType, GeometryType, Metadata};
use geos::{Geom, Geometry as GeosGeometry, GeometryTypes};
use wkt::Wkt;
use wkt::types::{GeometryCollection, MultiLineString, MultiPoint, MultiPolygon};

use super::adapter;
use crate::error::GeoDataFusionResult;
use crate::udf::aggregate_state::{GeometryAggregateState, ordering_fields_from_sort};
use crate::udf::native::geometry::{
    StrictGeometryBuilder, geometry_array_from_arrow, is_null_geometry_input, merge_metadata,
};

/// PostGIS overloads that DataFusion cannot express as overloads of the
/// aggregate name. They keep the PostGIS arguments but get an explicit name.
pub(super) const COLLECT_PAIR_NAME: &str = "st_collectpair";
pub(super) const COLLECT_ARRAY_NAME: &str = "st_collectarray";
pub(super) const UNION_PAIR_NAME: &str = "st_unionpair";
pub(super) const UNION_ARRAY_NAME: &str = "st_unionarray";
pub(super) const POLYGONIZE_ARRAY_NAME: &str = "st_polygonizearray";

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum AggregateKind {
    Collect,
    MemUnion,
    Union,
    Polygonize,
}

macro_rules! geometry_aggregate {
    ($type:ident, $name:literal, $kind:ident, $coerce:ident) => {
        #[derive(Debug, Eq, PartialEq, Hash)]
        pub struct $type {
            signature: Signature,
        }

        impl $type {
            pub fn new() -> Self {
                Self {
                    signature: Signature::user_defined(Volatility::Immutable),
                }
            }
        }

        impl Default for $type {
            fn default() -> Self {
                Self::new()
            }
        }

        impl AggregateUDFImpl for $type {
            fn name(&self) -> &str {
                $name
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
                $coerce(arg_types, $name)
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Err(DataFusionError::Internal("return_type".to_string()))
            }

            fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
                aggregate_return_field(arg_fields, $name)
            }

            fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
                GeometryAggregateAccumulator::try_new(AggregateKind::$kind, $name, acc_args)
            }

            fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
                let mut fields = vec![GeometryAggregateState::payload_state_field(args.name)];
                if !args.ordering_fields.is_empty() {
                    fields.push(GeometryAggregateState::ordering_state_field(
                        args.name,
                        args.ordering_fields,
                    ));
                }
                Ok(fields)
            }
        }
    };
}

geometry_aggregate!(CollectAggregate, "st_collect", Collect, coerce_collect);
geometry_aggregate!(MemUnionAggregate, "st_memunion", MemUnion, coerce_memunion);
geometry_aggregate!(UnionAggregate, "st_union", Union, coerce_union);
geometry_aggregate!(
    PolygonizeAggregate,
    "st_polygonize",
    Polygonize,
    coerce_polygonize
);

fn coerce_collect(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    match arg_types {
        [argument] if is_geometry_like(argument) => Ok(arg_types.to_vec()),
        _ => Err(DataFusionError::Plan(format!(
            "{name} expects one geometry argument per row; use {COLLECT_PAIR_NAME} or {COLLECT_ARRAY_NAME} for the PostGIS pair and array overloads"
        ))),
    }
}

fn coerce_memunion(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    match arg_types {
        [argument] if is_geometry_like(argument) => Ok(arg_types.to_vec()),
        _ => Err(DataFusionError::Plan(format!(
            "{name} expects one geometry argument"
        ))),
    }
}

fn coerce_union(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    match arg_types {
        [argument] if is_geometry_like(argument) => Ok(arg_types.to_vec()),
        [first, second] if is_geometry_like(first) && is_numeric(second) => {
            let mut coerced = arg_types.to_vec();
            coerced[1] = DataType::Float64;
            Ok(coerced)
        }
        _ => Err(DataFusionError::Plan(format!(
            "{name} expects one geometry argument with an optional numeric gridSize; use {UNION_PAIR_NAME} or {UNION_ARRAY_NAME} for the PostGIS pair and array overloads"
        ))),
    }
}

fn coerce_polygonize(arg_types: &[DataType], name: &str) -> Result<Vec<DataType>> {
    match arg_types {
        [argument] if is_geometry_like(argument) => Ok(arg_types.to_vec()),
        _ => Err(DataFusionError::Plan(format!(
            "{name} expects one geometry argument per row; use {POLYGONIZE_ARRAY_NAME} for the PostGIS array overload"
        ))),
    }
}

fn is_geometry_like(data_type: &DataType) -> bool {
    crate::data_types::any_geometry_type().contains(data_type) || data_type == &DataType::Null
}

fn is_numeric(data_type: &DataType) -> bool {
    data_type.is_numeric() || data_type == &DataType::Null
}

fn is_list(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    )
}

fn aggregate_return_field(arg_fields: &[FieldRef], name: &str) -> Result<FieldRef> {
    let geometry_field = arg_fields.iter().find(|field| {
        is_geometry_like(field.data_type()) && !is_null_geometry_field(field.as_ref())
    });
    let (metadata, coord_type) = match geometry_field {
        Some(field) => (
            merge_metadata([field.as_ref()], name)?,
            coord_type_of(field.as_ref()),
        ),
        None => (Arc::new(Metadata::default()), CoordType::Separated),
    };
    let output = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output.to_field("", true)))
}

fn is_null_geometry_field(field: &Field) -> bool {
    field.data_type() == &DataType::Null || field.extension_type_name().is_none()
}

fn list_element_field(field: &Field) -> Result<FieldRef> {
    match field.data_type() {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Ok(element.clone()),
        other => Err(DataFusionError::Plan(format!(
            "expected a geometry array, got {other}"
        ))),
    }
}

#[derive(Debug)]
struct GeometryAggregateAccumulator {
    kind: AggregateKind,
    name: &'static str,
    input_field: FieldRef,
    return_field: FieldRef,
    state: GeometryAggregateState,
    has_grid_argument: bool,
}

impl GeometryAggregateAccumulator {
    fn try_new(
        kind: AggregateKind,
        name: &'static str,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        let ordering_fields = ordering_fields_from_sort(acc_args.order_bys, acc_args.schema)?;
        let input_field = acc_args.exprs[0].return_field(acc_args.schema)?;
        Ok(Box::new(Self {
            kind,
            name,
            input_field,
            return_field: acc_args.return_field,
            state: GeometryAggregateState::new(
                acc_args.order_bys,
                &ordering_fields,
                acc_args.is_distinct,
            ),
            has_grid_argument: acc_args
                .expr_fields
                .get(1)
                .is_some_and(|field| field.data_type().is_numeric()),
        }))
    }

    fn evaluate_geos(&mut self) -> GeoDataFusionResult<ColumnarValue> {
        let grid_size = self.state.grid_size();
        let entries = self.state.sorted_entries()?;
        if entries.is_empty() {
            return Ok(ColumnarValue::Scalar(null_scalar(&self.return_field)?));
        }

        match self.kind {
            AggregateKind::Collect => Ok(ColumnarValue::Array(collect_wkb_array(
                entries,
                &self.return_field,
            )?)),
            AggregateKind::MemUnion | AggregateKind::Union => {
                let result = unary_union_wkb(entries, grid_size)?;
                Ok(ColumnarValue::Scalar(scalar_from_wkb(
                    result,
                    &self.return_field,
                )?))
            }
            AggregateKind::Polygonize => {
                let result = polygonize_wkb(entries)?;
                Ok(ColumnarValue::Scalar(scalar_from_wkb(
                    result,
                    &self.return_field,
                )?))
            }
        }
    }
}

impl Accumulator for GeometryAggregateAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let value = values.first().ok_or_else(|| {
            DataFusionError::Internal(format!("{} received no input batch", self.name))
        })?;
        if self.kind == AggregateKind::Union && self.has_grid_argument {
            let grid = values[1]
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "{} gridSize was not coerced to Float64",
                        self.name
                    ))
                })?;
            for grid in grid.iter().flatten() {
                self.state.set_grid_size(grid)?;
            }
        }
        if is_null_geometry_input(&self.input_field, value.as_ref()) {
            return Ok(());
        }

        let geometry = geometry_array_from_arrow(value, &self.input_field)?;
        let wkb =
            to_wkb::<i32>(geometry.as_ref()).map_err(crate::error::GeoDataFusionError::GeoArrow)?;
        for row in 0..wkb.inner().len() {
            if wkb.inner().is_null(row) {
                continue;
            }
            let ordering_start = if self.kind == AggregateKind::Union && self.has_grid_argument {
                2
            } else {
                1
            };
            let ordering = if values.len() > ordering_start {
                get_row_at_idx(&values[ordering_start..], row)?
            } else {
                Vec::new()
            };
            self.state.push(wkb.inner().value(row).to_vec(), ordering);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.state.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.state.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let result = self.evaluate_geos()?;
        match result {
            ColumnarValue::Scalar(value) => Ok(value),
            ColumnarValue::Array(array) => ScalarValue::try_from_array(array.as_ref(), 0),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.state.size()
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
enum ScalarMode {
    CollectBinary,
    CollectArray,
    UnionBinary,
    UnionArray,
    PolygonizeArray,
}

#[derive(Debug, Eq, PartialEq, Hash)]
struct ScalarOverloadUdf {
    name: &'static str,
    mode: ScalarMode,
}

impl ScalarOverloadUdf {
    fn new(name: &'static str, mode: ScalarMode) -> Self {
        Self { name, mode }
    }
}

impl ScalarUDFImpl for ScalarOverloadUdf {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::OnceLock<Signature> = std::sync::OnceLock::new();
        SIGNATURE.get_or_init(adapter::signature)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match self.mode {
            ScalarMode::CollectBinary => {
                if matches!(arg_types, [first, second] if is_geometry_like(first) && is_geometry_like(second))
                {
                    Ok(arg_types.to_vec())
                } else {
                    Err(DataFusionError::Plan(format!(
                        "{} expects two geometry arguments",
                        self.name
                    )))
                }
            }
            ScalarMode::CollectArray | ScalarMode::UnionArray | ScalarMode::PolygonizeArray => {
                if matches!(arg_types, [argument] if is_list(argument)) {
                    Ok(arg_types.to_vec())
                } else {
                    Err(DataFusionError::Plan(format!(
                        "{} expects one geometry array argument",
                        self.name
                    )))
                }
            }
            ScalarMode::UnionBinary => match arg_types {
                [first, second] if is_geometry_like(first) && is_geometry_like(second) => {
                    Ok(arg_types.to_vec())
                }
                [first, second, third]
                    if is_geometry_like(first) && is_geometry_like(second) && is_numeric(third) =>
                {
                    let mut coerced = arg_types.to_vec();
                    coerced[2] = DataType::Float64;
                    Ok(coerced)
                }
                _ => Err(DataFusionError::Plan(format!(
                    "{} expects two geometries with an optional numeric gridSize",
                    self.name
                ))),
            },
        }
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let geometry_positions: Vec<usize> = match self.mode {
            ScalarMode::CollectBinary | ScalarMode::UnionBinary => vec![0, 1],
            ScalarMode::CollectArray | ScalarMode::UnionArray | ScalarMode::PolygonizeArray => {
                vec![0]
            }
        };
        if matches!(
            self.mode,
            ScalarMode::CollectArray | ScalarMode::UnionArray | ScalarMode::PolygonizeArray
        ) {
            let field = &args.arg_fields[0];
            if is_null_field(field.as_ref()) || is_list(field.data_type()) {
                let element = list_element_field(field.as_ref())?;
                let metadata = merge_metadata([element.as_ref()], self.name)?;
                let coord_type = coord_type_of(element.as_ref());
                return Ok(Arc::new(
                    GeometryType::new(metadata)
                        .with_coord_type(coord_type)
                        .to_field("", true),
                ));
            }
        }
        adapter::geometry_return_field(&args, &geometry_positions, self.name)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match self.mode {
            ScalarMode::CollectBinary => collect_binary_impl(&args),
            ScalarMode::CollectArray => collect_array_impl(&args),
            ScalarMode::UnionBinary => union_binary_impl(&args),
            ScalarMode::UnionArray => union_array_impl(&args),
            ScalarMode::PolygonizeArray => polygonize_array_impl(&args),
        }
    }
}

fn is_null_field(field: &Field) -> bool {
    field.data_type() == &DataType::Null
}

fn coord_type_of(field: &Field) -> CoordType {
    GeoArrowType::from_arrow_field(field)
        .ok()
        .and_then(|typ| typ.coord_type())
        .unwrap_or(CoordType::Separated)
}

fn collect_binary_impl(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    Ok(adapter::binary_geos(
        args,
        |_row, _arrays, _left_wkb, left, _right_wkb, right, _writer| {
            Ok(Some(collect_geos(&[left, right])?))
        },
    )?)
}

fn collect_array_impl(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())
        .map_err(crate::error::GeoDataFusionError::GeoArrow)?;
    let rows = list_wkb_rows(&arrays[0], &args.arg_fields[0])?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        output.push(match row {
            Some(entries) if !entries.is_empty() => Some(collect_geos_wkb(&entries)?),
            _ => None,
        });
    }
    let output = BinaryArray::from_iter(output);
    Ok(adapter::finish_geos_result(output, output_type)?)
}

fn union_binary_impl(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    Ok(adapter::binary_geos(
        args,
        |row, arrays, left_wkb, left, right_wkb, right, writer| {
            let grid_size = if arrays.len() > 2 {
                match adapter::float64_value(arrays, 2, row, "ST_Union")? {
                    Some(grid_size) => grid_size,
                    None => return Ok(None),
                }
            } else {
                -1.0
            };
            if !grid_size.is_finite() {
                return Err(DataFusionError::Execution(
                    "ST_Union gridSize must be finite".to_string(),
                )
                .into());
            }
            if left.is_empty()? || right.is_empty()? {
                return Ok(Some(if left.is_empty()? {
                    right_wkb.to_vec()
                } else {
                    left_wkb.to_vec()
                }));
            }
            let result = if grid_size >= 0.0 {
                left.union_prec(right, grid_size)?
            } else {
                left.union(right)?
            };
            let mut result = result;
            result.normalize()?;
            Ok(Some(writer.write(&result)?))
        },
    )?)
}

fn union_array_impl(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())
        .map_err(crate::error::GeoDataFusionError::GeoArrow)?;
    let rows = list_wkb_rows(&arrays[0], &args.arg_fields[0])?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        output.push(match row {
            Some(entries) if !entries.is_empty() => Some(unary_union_wkb(&entries, None)?),
            _ => None,
        });
    }
    let output = BinaryArray::from_iter(output);
    Ok(adapter::finish_geos_result(output, output_type)?)
}

fn polygonize_array_impl(args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())
        .map_err(crate::error::GeoDataFusionError::GeoArrow)?;
    let rows = list_wkb_rows(&arrays[0], &args.arg_fields[0])?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        output.push(match row {
            Some(entries) if !entries.is_empty() => Some(polygonize_wkb(&entries)?),
            _ => None,
        });
    }
    let output = BinaryArray::from_iter(output);
    Ok(adapter::finish_geos_result(output, output_type)?)
}

fn collect_geos(geometries: &[&GeosGeometry]) -> GeoDataFusionResult<Vec<u8>> {
    collect_geos_inner(
        geometries
            .iter()
            .map(|geometry| Clone::clone(*geometry))
            .collect(),
    )
}

fn collect_geos_wkb(entries: &[Vec<u8>]) -> GeoDataFusionResult<Vec<u8>> {
    let geometries = entries
        .iter()
        .map(|entry| geos_from_wkb(entry))
        .collect::<GeoDataFusionResult<Vec<_>>>()?;
    collect_geos_inner(geometries)
}

fn collect_geos_inner(geometries: Vec<GeosGeometry>) -> GeoDataFusionResult<Vec<u8>> {
    let mut writer = adapter::GeosWkbWriter::new()?;
    let result = if geometries.is_empty() {
        GeosGeometry::create_geometry_collection(Vec::new())?
    } else if geometries.iter().all(|geometry| {
        geometry
            .geometry_type()
            .is_ok_and(|typ| typ == geos::GeometryTypes::Point)
    }) {
        GeosGeometry::create_multipoint(geometries)?
    } else if geometries.iter().all(|geometry| {
        geometry
            .geometry_type()
            .is_ok_and(|typ| typ == geos::GeometryTypes::LineString)
    }) {
        GeosGeometry::create_multiline_string(geometries)?
    } else if geometries.iter().all(|geometry| {
        geometry
            .geometry_type()
            .is_ok_and(|typ| typ == geos::GeometryTypes::Polygon)
    }) {
        GeosGeometry::create_multipolygon(geometries)?
    } else {
        GeosGeometry::create_geometry_collection(geometries)?
    };
    writer.write(&result)
}

fn unary_union_wkb(entries: &[Vec<u8>], grid_size: Option<f64>) -> GeoDataFusionResult<Vec<u8>> {
    let geometries = entries
        .iter()
        .map(|entry| geos_from_wkb(entry))
        .collect::<GeoDataFusionResult<Vec<_>>>()?;
    let non_empty = geometries
        .iter()
        .filter(|geometry| !geometry.is_empty().unwrap_or(false))
        .map(Clone::clone)
        .collect::<Vec<_>>();
    if non_empty.is_empty() {
        return Ok(entries.last().expect("non-empty entries").clone());
    }

    let collection = GeosGeometry::create_geometry_collection(non_empty)?;
    let mut result = match grid_size {
        Some(grid_size) if grid_size >= 0.0 => collection.unary_union_prec(grid_size)?,
        _ => collection.unary_union()?,
    };
    result.normalize()?;
    result = normalize_union_result(result)?;
    let mut writer = adapter::GeosWkbWriter::new()?;
    writer.write(&result)
}

/// PostGIS canonicalizes member order for MultiPoint, MultiPolygon, and
/// GeometryCollection results. GEOS `normalize()` does not reorder members.
fn normalize_union_result(result: GeosGeometry) -> GeoDataFusionResult<GeosGeometry> {
    let geometry_type = result.geometry_type()?;
    if !matches!(
        geometry_type,
        GeometryTypes::MultiPoint | GeometryTypes::MultiPolygon | GeometryTypes::GeometryCollection
    ) {
        return Ok(result);
    }

    let mut children = Vec::with_capacity(result.get_num_geometries()?);
    for index in 0..result.get_num_geometries()? {
        let child = result.get_geometry_n(index)?;
        let key = (
            child.get_x_min()?,
            child.get_y_min()?,
            child.get_x_max()?,
            child.get_y_max()?,
        );
        children.push((key, Geom::clone(&child)?));
    }
    children.sort_by(|left, right| {
        left.0
            .0
            .total_cmp(&right.0.0)
            .then_with(|| left.0.1.total_cmp(&right.0.1))
            .then_with(|| left.0.2.total_cmp(&right.0.2))
            .then_with(|| left.0.3.total_cmp(&right.0.3))
    });
    let children = children
        .into_iter()
        .map(|(_, geometry)| geometry)
        .collect::<Vec<_>>();
    Ok(match geometry_type {
        GeometryTypes::MultiPoint => GeosGeometry::create_multipoint(children)?,
        GeometryTypes::MultiPolygon => GeosGeometry::create_multipolygon(children)?,
        GeometryTypes::GeometryCollection => GeosGeometry::create_geometry_collection(children)?,
        _ => unreachable!("filtered above"),
    })
}

fn polygonize_wkb(entries: &[Vec<u8>]) -> GeoDataFusionResult<Vec<u8>> {
    let geometries = entries
        .iter()
        .map(|entry| geos_from_wkb(entry))
        .collect::<GeoDataFusionResult<Vec<_>>>()?;
    let mut result = GeosGeometry::polygonize(&geometries)?;
    result.normalize()?;
    result = normalize_union_result(result)?;
    let mut writer = adapter::GeosWkbWriter::new()?;
    writer.write(&result)
}

fn geos_from_wkb(bytes: &[u8]) -> GeoDataFusionResult<GeosGeometry> {
    let prepared = adapter::prepare_geos_wkb(bytes)?;
    Ok(GeosGeometry::new_from_wkb(prepared.as_ref())?)
}

fn scalar_from_wkb(wkb: Vec<u8>, return_field: &FieldRef) -> GeoDataFusionResult<ScalarValue> {
    let output_type = GeoArrowType::from_arrow_field(return_field)?;
    let output =
        adapter::finish_geos_result(BinaryArray::from(vec![Some(wkb.as_slice())]), output_type)?;
    match output {
        ColumnarValue::Scalar(value) => Ok(value),
        ColumnarValue::Array(array) => Ok(ScalarValue::try_from_array(array.as_ref(), 0)?),
    }
}

fn null_scalar(return_field: &FieldRef) -> Result<ScalarValue> {
    let output_type = GeoArrowType::from_arrow_field(return_field.as_ref())
        .map_err(crate::error::GeoDataFusionError::GeoArrow)?;
    let output = adapter::finish_geos_result(BinaryArray::from(vec![None::<&[u8]>]), output_type)?;
    match output {
        ColumnarValue::Scalar(value) => Ok(value),
        ColumnarValue::Array(array) => ScalarValue::try_from_array(array.as_ref(), 0),
    }
}

fn collect_wkb_array(
    entries: &[Vec<u8>],
    return_field: &FieldRef,
) -> GeoDataFusionResult<ArrayRef> {
    let geometries = entries
        .iter()
        .map(|entry| crate::udf::wkb_wkt::wkb_to_wkt(entry))
        .collect::<GeoDataFusionResult<Vec<_>>>()?;
    let dimension = geometries
        .first()
        .map(|geometry| geometry.dim())
        .ok_or_else(|| DataFusionError::Internal("ST_Collect received no geometry".to_string()))?;
    if geometries
        .iter()
        .any(|geometry| geometry.dim() != dimension)
    {
        return Err(DataFusionError::Execution(
            "ST_Collect cannot mix geometry dimensions".to_string(),
        )
        .into());
    }
    let dimension = crate::udf::wkb_wkt::wkt_dimension(dimension)?;

    let collection = if geometries
        .iter()
        .all(|geometry| matches!(geometry, Wkt::Point(_)))
    {
        Wkt::MultiPoint(MultiPoint::new(
            geometries
                .iter()
                .map(|geometry| match geometry {
                    Wkt::Point(point) => point.clone(),
                    _ => unreachable!(),
                })
                .collect(),
            dimension,
        ))
    } else if geometries
        .iter()
        .all(|geometry| matches!(geometry, Wkt::LineString(_)))
    {
        Wkt::MultiLineString(MultiLineString::new(
            geometries
                .iter()
                .map(|geometry| match geometry {
                    Wkt::LineString(line_string) => line_string.clone(),
                    _ => unreachable!(),
                })
                .collect(),
            dimension,
        ))
    } else if geometries
        .iter()
        .all(|geometry| matches!(geometry, Wkt::Polygon(_)))
    {
        Wkt::MultiPolygon(MultiPolygon::new(
            geometries
                .iter()
                .map(|geometry| match geometry {
                    Wkt::Polygon(polygon) => polygon.clone(),
                    _ => unreachable!(),
                })
                .collect(),
            dimension,
        ))
    } else {
        Wkt::GeometryCollection(GeometryCollection::new(geometries, dimension))
    };

    let GeoArrowType::Geometry(typ) = GeoArrowType::from_arrow_field(return_field.as_ref())? else {
        return Err(DataFusionError::Internal(
            "ST_Collect output field is not generic Geometry".to_string(),
        )
        .into());
    };
    let mut builder = StrictGeometryBuilder::new(typ);
    builder.push_geometry(Some(&collection))?;
    Ok(builder.finish().into_array_ref())
}

fn list_wkb_rows(
    array: &ArrayRef,
    field: &Field,
) -> GeoDataFusionResult<Vec<Option<Vec<Vec<u8>>>>> {
    let element_field = list_element_field(field)?;
    match array.data_type() {
        DataType::List(_) => list_wkb_rows_impl(array.as_list::<i32>(), element_field.as_ref()),
        DataType::LargeList(_) => {
            list_wkb_rows_impl(array.as_list::<i64>(), element_field.as_ref())
        }
        DataType::FixedSizeList(_, _) => {
            let list = array.as_fixed_size_list();
            let mut rows = Vec::with_capacity(list.len());
            for row in 0..list.len() {
                if list.is_null(row) {
                    rows.push(None);
                    continue;
                }
                rows.push(Some(wkb_values(list.value(row), element_field.as_ref())?));
            }
            Ok(rows)
        }
        other => {
            Err(DataFusionError::Plan(format!("expected a geometry array, got {other}")).into())
        }
    }
}

fn list_wkb_rows_impl<O: arrow_array::OffsetSizeTrait>(
    list: &arrow_array::GenericListArray<O>,
    element_field: &Field,
) -> GeoDataFusionResult<Vec<Option<Vec<Vec<u8>>>>> {
    let mut rows = Vec::with_capacity(list.len());
    for row in 0..list.len() {
        if list.is_null(row) {
            rows.push(None);
        } else {
            rows.push(Some(wkb_values(list.value(row), element_field)?));
        }
    }
    Ok(rows)
}

fn wkb_values(values: ArrayRef, element_field: &Field) -> GeoDataFusionResult<Vec<Vec<u8>>> {
    if values.data_type() == &DataType::Null {
        return Ok(Vec::new());
    }
    let geometry = geometry_array_from_arrow(values.as_ref(), element_field)?;
    let wkb =
        to_wkb::<i32>(geometry.as_ref()).map_err(crate::error::GeoDataFusionError::GeoArrow)?;
    Ok((0..wkb.inner().len())
        .filter(|row| !wkb.inner().is_null(*row))
        .map(|row| wkb.inner().value(row).to_vec())
        .collect())
}

pub(super) fn register(session_context: &SessionContext) {
    session_context.register_udaf(CollectAggregate::default().into());
    session_context.register_udaf(MemUnionAggregate::default().into());
    session_context.register_udaf(UnionAggregate::default().into());
    session_context.register_udaf(PolygonizeAggregate::default().into());

    for (name, mode) in [
        (COLLECT_PAIR_NAME, ScalarMode::CollectBinary),
        (COLLECT_ARRAY_NAME, ScalarMode::CollectArray),
        (UNION_PAIR_NAME, ScalarMode::UnionBinary),
        (UNION_ARRAY_NAME, ScalarMode::UnionArray),
        (POLYGONIZE_ARRAY_NAME, ScalarMode::PolygonizeArray),
    ] {
        session_context.register_udf(ScalarUDF::from(ScalarOverloadUdf::new(name, mode)));
    }
}
