//! Window implementations of the PostGIS clustering functions.
//!
//! All four functions process the whole `PARTITION BY` group at once, exactly like the
//! PostGIS window implementations: the window frame is not part of the contract, and the
//! `eps`/`minpoints`/`k`/`max_radius` parameters are read from the first row of the
//! partition.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int32Array};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{PartitionEvaluator, Signature, Volatility, WindowUDFImpl};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use geoarrow_array::downcast_geoarrow_array;

use super::algorithms::{
    ClusteringRow, KMeansObject, clustering_rows, dbscan, dbscan_minpoints_one,
    intersecting_clusters, kmeans_objects,
};
use super::common::{
    coerce_float, coerce_geometry, coerce_int, float_at, int_at, user_defined_signature,
};
use super::kmeans;
use crate::udf::native::geometry::geometry_array_from_arrow;

/// Which PostGIS window function is being evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum WindowKind {
    Dbscan,
    Intersecting,
    Within,
    KMeans,
}

impl WindowKind {
    fn name(self) -> &'static str {
        match self {
            Self::Dbscan => "st_clusterdbscan",
            Self::Intersecting => "st_clusterintersectingwin",
            Self::Within => "st_clusterwithinwin",
            Self::KMeans => "st_clusterkmeans",
        }
    }

    fn volatility(self) -> Volatility {
        match self {
            Self::KMeans => Volatility::Volatile,
            _ => Volatility::Immutable,
        }
    }
}

/// One public window UDF object shared by all four clustering window functions.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ClusteringWindow {
    kind: WindowKind,
    signature: Signature,
}

impl ClusteringWindow {
    pub fn dbscan() -> Self {
        Self::new(WindowKind::Dbscan)
    }

    pub fn cluster_intersecting_win() -> Self {
        Self::new(WindowKind::Intersecting)
    }

    pub fn cluster_within_win() -> Self {
        Self::new(WindowKind::Within)
    }

    pub fn cluster_kmeans() -> Self {
        Self::new(WindowKind::KMeans)
    }

    fn new(kind: WindowKind) -> Self {
        Self {
            kind,
            signature: user_defined_signature(kind.volatility()),
        }
    }
}

impl WindowUDFImpl for ClusteringWindow {
    fn name(&self) -> &str {
        self.kind.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let function = self.kind.name();
        match self.kind {
            WindowKind::Intersecting => {
                expect_arity(function, arg_types, 1, 1)?;
                Ok(vec![coerce_geometry(function, 0, &arg_types[0])?])
            }
            WindowKind::Within => {
                expect_arity(function, arg_types, 2, 2)?;
                Ok(vec![
                    coerce_geometry(function, 0, &arg_types[0])?,
                    coerce_float(function, 1, &arg_types[1])?,
                ])
            }
            WindowKind::Dbscan => {
                expect_arity(function, arg_types, 3, 3)?;
                Ok(vec![
                    coerce_geometry(function, 0, &arg_types[0])?,
                    coerce_float(function, 1, &arg_types[1])?,
                    coerce_int(function, 2, &arg_types[2])?,
                ])
            }
            WindowKind::KMeans => {
                expect_arity(function, arg_types, 2, 3)?;
                let mut coerced = vec![
                    coerce_geometry(function, 0, &arg_types[0])?,
                    coerce_int(function, 1, &arg_types[1])?,
                ];
                if arg_types.len() == 3 {
                    coerced.push(coerce_float(function, 2, &arg_types[2])?);
                }
                Ok(coerced)
            }
        }
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(ClusteringPartitionEvaluator {
            kind: self.kind,
            input_fields: args.input_fields().to_vec(),
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            field_args.name(),
            DataType::Int32,
            true,
        )))
    }
}

fn expect_arity(function: &str, arg_types: &[DataType], min: usize, max: usize) -> Result<()> {
    if arg_types.len() < min || arg_types.len() > max {
        if min == max {
            return Err(DataFusionError::Plan(format!(
                "Function '{function}' expects {min} argument(s), got {}",
                arg_types.len()
            )));
        }
        return Err(DataFusionError::Plan(format!(
            "Function '{function}' expects between {min} and {max} arguments, got {}",
            arg_types.len()
        )));
    }
    Ok(())
}

#[derive(Debug)]
struct ClusteringPartitionEvaluator {
    kind: WindowKind,
    input_fields: Vec<FieldRef>,
}

impl PartitionEvaluator for ClusteringPartitionEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        if num_rows == 0 {
            return Ok(Arc::new(Int32Array::from(Vec::<Option<i32>>::new())));
        }

        let function = self.kind.name();
        let geometry_field = self.input_fields.first().ok_or_else(|| {
            DataFusionError::Internal(format!("{function} received no input fields"))
        })?;

        match self.kind {
            WindowKind::Intersecting => {
                let rows = clustering_rows_from_values(function, values, geometry_field, num_rows)?;
                Ok(Arc::new(cluster_ids(&rows, intersecting_clusters(&rows))))
            }
            WindowKind::Within => {
                let distance = tolerance_at(function, values, 1, num_rows)?;
                let rows = clustering_rows_from_values(function, values, geometry_field, num_rows)?;
                Ok(Arc::new(cluster_ids(
                    &rows,
                    dbscan_minpoints_one(&rows, distance),
                )))
            }
            WindowKind::Dbscan => {
                let eps = tolerance_at(function, values, 1, num_rows)?;
                let minpoints = minpoints_at(function, values, num_rows)?;
                let rows = clustering_rows_from_values(function, values, geometry_field, num_rows)?;
                let ids = if minpoints <= 1 {
                    dbscan_minpoints_one(&rows, eps)
                } else {
                    dbscan(&rows, eps, minpoints as usize)
                };
                Ok(Arc::new(cluster_ids(&rows, ids)))
            }
            WindowKind::KMeans => Ok(Arc::new(kmeans_evaluate(
                function,
                values,
                geometry_field,
                num_rows,
            )?)),
        }
    }
}

fn clustering_rows_from_values(
    function: &str,
    values: &[ArrayRef],
    field: &FieldRef,
    num_rows: usize,
) -> Result<Vec<Option<ClusteringRow>>> {
    let array = values.first().ok_or_else(|| {
        DataFusionError::Internal(format!("{function} received no geometry argument"))
    })?;
    if array.len() != num_rows {
        return Err(DataFusionError::Internal(format!(
            "{function} geometry argument has {} rows, expected {num_rows}",
            array.len()
        )));
    }
    if matches!(field.data_type(), DataType::Null) || matches!(array.data_type(), DataType::Null) {
        return Ok(vec![None; num_rows]);
    }
    let array = geometry_array_from_arrow(array, field)?;
    let array_ref = array.as_ref();
    downcast_geoarrow_array!(array_ref, clustering_rows)
}

/// `Tolerance` handling shared by `ST_ClusterDBSCAN` and `ST_ClusterWithinWin`.
fn tolerance_at(function: &str, values: &[ArrayRef], index: usize, num_rows: usize) -> Result<f64> {
    let array = values.get(index).ok_or_else(|| {
        DataFusionError::Internal(format!("{function} received no tolerance argument"))
    })?;
    if array.len() != num_rows {
        return Err(DataFusionError::Internal(format!(
            "{function} tolerance argument has {} rows, expected {num_rows}",
            array.len()
        )));
    }
    let parameter = float_at(function, array, 0)?;
    let value = parameter.unwrap_or(0.0);
    if parameter.is_none() || value < 0.0 {
        return Err(DataFusionError::Execution(format!(
            "Tolerance must be a positive number, got {value}"
        )));
    }
    Ok(value)
}

fn minpoints_at(function: &str, values: &[ArrayRef], num_rows: usize) -> Result<i64> {
    let array = values.get(2).ok_or_else(|| {
        DataFusionError::Internal(format!("{function} received no minpoints argument"))
    })?;
    if array.len() != num_rows {
        return Err(DataFusionError::Internal(format!(
            "{function} minpoints argument has {} rows, expected {num_rows}",
            array.len()
        )));
    }
    let value = int_at(function, array, 0)?;
    match value {
        Some(value) if value >= 0 => Ok(value),
        Some(value) => Err(DataFusionError::Execution(format!(
            "Minpoints must be a positive number, got {value}"
        ))),
        None => Err(DataFusionError::Execution(
            "Minpoints must be a positive number, got 0".to_string(),
        )),
    }
}

fn cluster_ids(rows: &[Option<ClusteringRow>], ids: Vec<Option<u32>>) -> Int32Array {
    let values = rows
        .iter()
        .zip(ids)
        .map(|(row, id)| match (row, id) {
            (Some(_), Some(id)) => Some(id as i32),
            _ => None,
        })
        .collect::<Vec<_>>();
    Int32Array::from(values)
}

fn kmeans_evaluate(
    function: &str,
    values: &[ArrayRef],
    geometry_field: &FieldRef,
    num_rows: usize,
) -> Result<Int32Array> {
    let k_array = values.get(1).ok_or_else(|| {
        DataFusionError::Internal(format!("{function} received no cluster count argument"))
    })?;
    if k_array.len() != num_rows {
        return Err(DataFusionError::Internal(format!(
            "{function} cluster count argument has {} rows, expected {num_rows}",
            k_array.len()
        )));
    }
    let Some(k) = int_at(function, k_array, 0)? else {
        return Ok(Int32Array::new_null(num_rows));
    };
    if k <= 0 {
        return Ok(Int32Array::new_null(num_rows));
    }
    if (num_rows as i64) < k {
        return Err(DataFusionError::Execution(format!(
            "K ({k}) must be smaller than the number of rows in the group ({num_rows})"
        )));
    }

    let max_radius = match values.get(2) {
        Some(array) => {
            if array.len() != num_rows {
                return Err(DataFusionError::Internal(format!(
                    "{function} max_radius argument has {} rows, expected {num_rows}",
                    array.len()
                )));
            }
            float_at(function, array, 0)?.unwrap_or(0.0)
        }
        None => 0.0,
    };
    let max_radius = if max_radius < 0.0 { 0.0 } else { max_radius };

    let objects = kmeans_objects_from_values(function, values, geometry_field, num_rows)?;
    let mut valid_objects = objects.iter().flatten().copied().collect::<Vec<_>>();
    let mut effective_k = k as usize;
    if valid_objects.len() < effective_k {
        effective_k = valid_objects.len();
    }

    let mut labels = vec![-1i32; num_rows];
    if effective_k > 0 {
        let solution = kmeans::cluster(&valid_objects, effective_k, max_radius)?;
        valid_objects.clear();
        let mut object_index = 0usize;
        for (row, object) in objects.iter().enumerate() {
            if object.is_some() {
                labels[row] = solution.clusters[object_index] as i32;
                object_index += 1;
            }
        }
    }

    Ok(Int32Array::from(labels))
}

fn kmeans_objects_from_values(
    function: &str,
    values: &[ArrayRef],
    field: &FieldRef,
    num_rows: usize,
) -> Result<Vec<Option<KMeansObject>>> {
    let array = values.first().ok_or_else(|| {
        DataFusionError::Internal(format!("{function} received no geometry argument"))
    })?;
    if matches!(field.data_type(), DataType::Null) || matches!(array.data_type(), DataType::Null) {
        return Ok(vec![None; num_rows]);
    }
    let array = geometry_array_from_arrow(array, field)?;
    let array_ref = array.as_ref();
    downcast_geoarrow_array!(array_ref, kmeans_objects)
}
