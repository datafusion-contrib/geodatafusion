//! Aggregate implementations of `ST_ClusterIntersecting` and `ST_ClusterWithin`.
//!
//! Both aggregates return one `geom` field per cluster, mirroring `ST_Dump`: the list element
//! is a struct whose `geom` field is the cluster's `GeometryCollection` encoded as GeoArrow WKB
//! with the input's CRS metadata. The accumulator state keeps the input geometries as
//! length-prefixed WKB records so that partial aggregation and `merge_batch` preserve every
//! geometry, including Z/M coordinates and empty geometries.
//!
//! PostGIS reads the aggregate's direct float8 argument from the first row it sees, so the
//! state also records that first tolerance value and whether any row was seen. A missing
//! tolerance or a zero tolerance is reported as `Tolerance not defined`, matching the
//! reference behavior; `ST_ClusterWithinWin` does not have that limitation.

use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, ArrayRef, BinaryArray, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::scalar::ScalarValue;
use geo_traits::GeometryTrait;
use geoarrow_array::array::WkbArray;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::{GeoArrowType, GeometryType, Metadata, WkbType};
use wkb::writer::{WriteOptions, geometry_wkb_size, write_geometry};

use super::algorithms::{
    ClusteringRow, clustering_rows, intersecting_clusters, owned_wkt_geometry, wkt_dimension,
};
use super::common::{arrow_error, coerce_float, coerce_geometry, float_at, user_defined_signature};
use crate::udf::native::geometry::geometry_array_from_arrow;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AggregateKind {
    Intersecting,
    Within,
}

impl AggregateKind {
    fn name(self) -> &'static str {
        match self {
            Self::Intersecting => "st_clusterintersecting",
            Self::Within => "st_clusterwithin",
        }
    }
}

/// `ST_ClusterIntersecting(geometry)` aggregate.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ClusterIntersecting {
    signature: Signature,
}

impl ClusterIntersecting {
    pub fn new() -> Self {
        Self {
            signature: user_defined_signature(Volatility::Immutable),
        }
    }
}

impl Default for ClusterIntersecting {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for ClusterIntersecting {
    fn name(&self) -> &str {
        AggregateKind::Intersecting.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_aggregate_types(AggregateKind::Intersecting, arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_ClusterIntersecting return field is computed from the input field".to_string(),
        ))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        output_list_field(arg_fields)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ClusterAccumulator::new(
            AggregateKind::Intersecting,
            acc_args,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(state_fields(AggregateKind::Intersecting, args))
    }
}

/// `ST_ClusterWithin(geometry, float8)` aggregate.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ClusterWithin {
    signature: Signature,
}

impl ClusterWithin {
    pub fn new() -> Self {
        Self {
            signature: user_defined_signature(Volatility::Immutable),
        }
    }
}

impl Default for ClusterWithin {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for ClusterWithin {
    fn name(&self) -> &str {
        AggregateKind::Within.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_aggregate_types(AggregateKind::Within, arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_ClusterWithin return field is computed from the input field".to_string(),
        ))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        output_list_field(arg_fields)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ClusterAccumulator::new(
            AggregateKind::Within,
            acc_args,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(state_fields(AggregateKind::Within, args))
    }
}

fn coerce_aggregate_types(kind: AggregateKind, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let function = kind.name();
    match kind {
        AggregateKind::Intersecting => {
            if arg_types.len() != 1 {
                return Err(DataFusionError::Plan(format!(
                    "Function '{function}' expects 1 argument, got {}",
                    arg_types.len()
                )));
            }
            Ok(vec![coerce_geometry(function, 0, &arg_types[0])?])
        }
        AggregateKind::Within => {
            if arg_types.len() != 2 {
                return Err(DataFusionError::Plan(format!(
                    "Function '{function}' expects 2 arguments, got {}",
                    arg_types.len()
                )));
            }
            Ok(vec![
                coerce_geometry(function, 0, &arg_types[0])?,
                coerce_float(function, 1, &arg_types[1])?,
            ])
        }
    }
}

/// The result is one list element per cluster. Each element is a `geom` struct whose field is
/// GeoArrow WKB with the input's CRS metadata, mirroring `ST_Dump`'s `geom` field so that
/// `UNNEST` keeps the field metadata through DataFusion's struct expansion.
fn output_list_field(arg_fields: &[FieldRef]) -> Result<FieldRef> {
    let input_field = arg_fields.first().ok_or_else(|| {
        DataFusionError::Internal("clustering aggregate has no input".to_string())
    })?;
    let metadata = Arc::new(Metadata::try_from(input_field.as_ref()).unwrap_or_default());
    let geom_field =
        Field::new("geom", DataType::Binary, true).with_extension_type(WkbType::new(metadata));
    let item = Field::new(
        "item",
        DataType::Struct(Fields::from(vec![geom_field])),
        false,
    );
    Ok(Arc::new(Field::new(
        "",
        DataType::List(Arc::new(item)),
        true,
    )))
}

fn output_geometry_type(input_field: &FieldRef) -> GeometryType {
    let metadata = Metadata::try_from(input_field.as_ref()).unwrap_or_default();
    let coord_type = GeoArrowType::from_arrow_field(input_field.as_ref())
        .ok()
        .and_then(|geometry| geometry.coord_type())
        .unwrap_or_default();
    GeometryType::new(Arc::new(metadata)).with_coord_type(coord_type)
}

fn state_fields(kind: AggregateKind, args: StateFieldsArgs) -> Vec<FieldRef> {
    let mut fields = vec![Arc::new(Field::new(
        format_state_name(args.name, "geometries"),
        DataType::Binary,
        true,
    )) as FieldRef];
    if kind == AggregateKind::Within {
        fields.push(Arc::new(Field::new(
            format_state_name(args.name, "seen"),
            DataType::Boolean,
            true,
        )));
        fields.push(Arc::new(Field::new(
            format_state_name(args.name, "tolerance"),
            DataType::Float64,
            true,
        )));
    }
    fields
}

#[derive(Debug)]
struct ClusterAccumulator {
    kind: AggregateKind,
    geometry_field: FieldRef,
    return_field: FieldRef,
    geometries: Vec<u8>,
    saw_rows: bool,
    tolerance_seen: bool,
    tolerance: Option<f64>,
}

impl ClusterAccumulator {
    fn new(kind: AggregateKind, acc_args: AccumulatorArgs) -> Self {
        Self {
            kind,
            geometry_field: acc_args.expr_fields[0].clone(),
            return_field: acc_args.return_field.clone(),
            geometries: Vec::new(),
            saw_rows: false,
            tolerance_seen: false,
            tolerance: None,
        }
    }

    fn append_geometry(&mut self, wkb: &[u8]) {
        let length = u32::try_from(wkb.len())
            .expect("a single WKB geometry cannot exceed the state record length");
        self.geometries.extend_from_slice(&length.to_le_bytes());
        self.geometries.extend_from_slice(wkb);
    }

    fn append_state_blob(&mut self, blob: &[u8]) -> Result<()> {
        let mut cursor = 0usize;
        while cursor < blob.len() {
            if cursor + 4 > blob.len() {
                return Err(DataFusionError::Internal(
                    "clustering aggregate state is truncated".to_string(),
                ));
            }
            let length = u32::from_le_bytes(
                blob[cursor..cursor + 4]
                    .try_into()
                    .expect("slice has exactly four bytes"),
            ) as usize;
            cursor += 4;
            let end = cursor.checked_add(length).ok_or_else(|| {
                DataFusionError::Internal("clustering aggregate state length overflow".to_string())
            })?;
            if end > blob.len() {
                return Err(DataFusionError::Internal(
                    "clustering aggregate state is truncated".to_string(),
                ));
            }
            self.geometries.extend_from_slice(&blob[cursor - 4..end]);
            cursor = end;
        }
        Ok(())
    }

    fn decoded_geometries(&self) -> Result<WkbArray> {
        let mut builder = BinaryBuilder::new();
        let mut cursor = 0usize;
        while cursor < self.geometries.len() {
            if cursor + 4 > self.geometries.len() {
                return Err(DataFusionError::Internal(
                    "clustering aggregate state is truncated".to_string(),
                ));
            }
            let length = u32::from_le_bytes(
                self.geometries[cursor..cursor + 4]
                    .try_into()
                    .expect("slice has exactly four bytes"),
            ) as usize;
            cursor += 4;
            let end = cursor.checked_add(length).ok_or_else(|| {
                DataFusionError::Internal("clustering aggregate state length overflow".to_string())
            })?;
            if end > self.geometries.len() {
                return Err(DataFusionError::Internal(
                    "clustering aggregate state is truncated".to_string(),
                ));
            }
            builder.append_value(&self.geometries[cursor..end]);
            cursor = end;
        }
        let binary = builder.finish();
        let geometry_type = output_geometry_type(&self.geometry_field);
        // Keep the parsed WKB array instead of rebuilding the native geometry union:
        // WKB can represent nested collections, which the native union cannot.
        Ok(WkbArray::new(binary, geometry_type.metadata().clone()))
    }

    fn cluster_ids(&self, rows: &[Option<ClusteringRow>]) -> Result<Vec<Option<u32>>> {
        match self.kind {
            AggregateKind::Intersecting => Ok(intersecting_clusters(rows)),
            AggregateKind::Within => Ok(super::algorithms::dbscan_minpoints_one(
                rows,
                self.tolerance
                    .expect("ST_ClusterWithin checks the tolerance before clustering"),
            )),
        }
    }

    fn build_output(&self, ids: &[Option<u32>]) -> Result<ScalarValue> {
        let cluster_count = ids
            .iter()
            .flatten()
            .copied()
            .max()
            .map_or(0, |max| max as usize + 1);

        let decoded = self.decoded_geometries()?;
        let (collections, _member_lengths) = build_collections(&decoded, ids, cluster_count)?;

        if collections.is_empty() {
            return ScalarValue::try_from(self.return_field.data_type());
        }

        let mut wkbs = Vec::with_capacity(collections.len());
        for collection in &collections {
            let mut wkb = Vec::with_capacity(geometry_wkb_size(collection));
            write_geometry(&mut wkb, collection, &WriteOptions::default()).map_err(|error| {
                DataFusionError::Execution(format!(
                    "could not encode clustering aggregate geometry collection: {error}"
                ))
            })?;
            wkbs.push(wkb);
        }

        let (item, item_fields) = match self.return_field.data_type() {
            DataType::List(item) => match item.data_type() {
                DataType::Struct(fields) => (item.clone(), fields.clone()),
                other => {
                    return Err(DataFusionError::Internal(format!(
                        "clustering aggregate expected a struct list item, got {other}"
                    )));
                }
            },
            other => {
                return Err(DataFusionError::Internal(format!(
                    "clustering aggregate expected a list return type, got {other}"
                )));
            }
        };

        let mut builder = BinaryBuilder::new();
        for wkb in &wkbs {
            builder.append_value(wkb);
        }
        let geom_array = builder.finish();
        let struct_array =
            StructArray::new(item_fields, vec![Arc::new(geom_array) as ArrayRef], None);
        // `ScalarValue::List` must contain a single list row; the row holds one element
        // per output cluster.
        let list = arrow_array::ListArray::new(
            item,
            OffsetBuffer::<i32>::from_lengths(vec![wkbs.len()]),
            Arc::new(struct_array),
            None,
        );
        Ok(ScalarValue::List(Arc::new(list)))
    }
}

fn build_collections<'a>(
    array: &'a impl geoarrow_array::GeoArrowArrayAccessor<'a>,
    ids: &[Option<u32>],
    cluster_count: usize,
) -> Result<(Vec<wkt::types::GeometryCollection<f64>>, Vec<usize>)> {
    let mut owned = Vec::with_capacity(array.len());
    let mut dimensions = Vec::with_capacity(array.len());
    for geometry in array.iter() {
        let geometry = match geometry {
            Some(geometry) => geometry.map_err(arrow_error)?,
            None => {
                return Err(DataFusionError::Internal(
                    "clustering aggregate state contains a null geometry".to_string(),
                ));
            }
        };
        dimensions.push(geometry.dim());
        owned.push(owned_wkt_geometry(&geometry)?);
    }
    let mut collections = Vec::new();
    let mut lengths = Vec::new();
    for cluster in 0..cluster_count {
        let members = ids
            .iter()
            .enumerate()
            .filter_map(|(index, id)| (*id == Some(cluster as u32)).then_some(index))
            .collect::<Vec<_>>();
        if members.is_empty() {
            continue;
        }

        let first_dimension = dimensions[members[0]];
        for &member in &members[1..] {
            if dimensions[member] != first_dimension {
                return Err(DataFusionError::Execution(format!(
                    "lwcollection_construct: mixed dimension geometries: {}/{}",
                    zm_code(first_dimension),
                    zm_code(dimensions[member])
                )));
            }
        }
        collections.push(wkt::types::GeometryCollection::new(
            members
                .iter()
                .map(|&member| owned[member].clone())
                .collect(),
            wkt_dimension(first_dimension)?,
        ));
        lengths.push(members.len());
    }
    Ok((collections, lengths))
}

fn zm_code(dimensions: geo_traits::Dimensions) -> u8 {
    match dimensions {
        geo_traits::Dimensions::Xy => 0,
        geo_traits::Dimensions::Xym => 1,
        geo_traits::Dimensions::Xyz => 2,
        geo_traits::Dimensions::Xyzm => 3,
        geo_traits::Dimensions::Unknown(value) => value as u8,
    }
}

impl Accumulator for ClusterAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = values.first().ok_or_else(|| {
            DataFusionError::Internal("clustering aggregate received no geometry input".to_string())
        })?;
        let num_rows = array.len();
        if num_rows > 0 {
            self.saw_rows = true;
        }

        if self.kind == AggregateKind::Within && !self.tolerance_seen && num_rows > 0 {
            let tolerance = values.get(1).ok_or_else(|| {
                DataFusionError::Internal(
                    "ST_ClusterWithin received no tolerance input".to_string(),
                )
            })?;
            if tolerance.len() != num_rows && tolerance.len() != 1 {
                return Err(DataFusionError::Internal(format!(
                    "ST_ClusterWithin tolerance input has {} rows, expected {num_rows}",
                    tolerance.len()
                )));
            }
            self.tolerance_seen = true;
            self.tolerance = float_at("st_clusterwithin", tolerance, 0)?;
        }

        if matches!(array.data_type(), DataType::Null) {
            return Ok(());
        }
        let array = geometry_array_from_arrow(array, &self.geometry_field)?;
        let wkb = to_wkb::<i32>(array.as_ref()).map_err(arrow_error)?;
        let inner = wkb.inner();
        for row in 0..inner.len() {
            if !inner.is_null(row) {
                self.append_geometry(inner.value(row));
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let geometries = states.first().ok_or_else(|| {
            DataFusionError::Internal("clustering aggregate received no state".to_string())
        })?;
        let geometries = geometries
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "clustering aggregate geometry state has type {}, expected Binary",
                    geometries.data_type()
                ))
            })?;

        if self.kind == AggregateKind::Within && !self.tolerance_seen {
            let seen = states.get(1).ok_or_else(|| {
                DataFusionError::Internal(
                    "ST_ClusterWithin received no tolerance state".to_string(),
                )
            })?;
            let tolerance = states.get(2).ok_or_else(|| {
                DataFusionError::Internal(
                    "ST_ClusterWithin received no tolerance state".to_string(),
                )
            })?;
            let seen = seen
                .as_any()
                .downcast_ref::<arrow_array::BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "ST_ClusterWithin seen state has type {}, expected Boolean",
                        seen.data_type()
                    ))
                })?;
            for row in 0..seen.len() {
                if !seen.is_null(row) && seen.value(row) {
                    self.tolerance_seen = true;
                    self.saw_rows = true;
                    self.tolerance = float_at("st_clusterwithin", tolerance, row)?;
                    break;
                }
            }
        }

        for row in 0..geometries.len() {
            if !geometries.is_null(row) {
                self.append_state_blob(geometries.value(row))?;
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut state = vec![ScalarValue::Binary(if self.geometries.is_empty() {
            None
        } else {
            Some(self.geometries.clone())
        })];
        if self.kind == AggregateKind::Within {
            state.push(ScalarValue::Boolean(Some(self.saw_rows)));
            state.push(ScalarValue::Float64(if self.tolerance_seen {
                self.tolerance
            } else {
                None
            }));
        }
        Ok(state)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.kind == AggregateKind::Within {
            if !self.saw_rows {
                return ScalarValue::try_from(self.return_field.data_type());
            }
            match self.tolerance {
                None | Some(0.0) => {
                    return Err(DataFusionError::Execution(
                        "Tolerance not defined".to_string(),
                    ));
                }
                Some(tolerance) if tolerance < 0.0 => {
                    return Err(DataFusionError::Execution(
                        "Tolerance must be a positive number.".to_string(),
                    ));
                }
                Some(_) => {}
            }
        }

        if self.geometries.is_empty() {
            return ScalarValue::try_from(self.return_field.data_type());
        }

        let decoded = self.decoded_geometries()?;
        let rows = clustering_rows(&decoded)?;
        let ids = self.cluster_ids(&rows)?;
        self.build_output(&ids)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.geometries.capacity()
    }
}
