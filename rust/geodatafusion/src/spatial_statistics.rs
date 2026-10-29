//! Spatial statistics published by table providers.
//!
//! DataFusion's generic [`Statistics`] type cannot represent a two-dimensional
//! bounding box for a geometry column. Providers that have a metadata-derived
//! extent therefore wrap themselves in [`SpatialStatisticsTable`] instead of
//! projecting the extent onto scalar min/max statistics. This is the carrier
//! consumed by the `ST_EstimatedExtent` scalar function.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::{Constraints, DFSchemaRef, Result, Statistics};
use datafusion::logical_expr::dml::{InsertOp, MergeIntoClause};
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

/// A two-dimensional bounding box for one geometry column.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SpatialExtent {
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
}

impl SpatialExtent {
    /// Create an extent from already validated bounds.
    pub fn new(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> Self {
        Self {
            xmin,
            ymin,
            xmax,
            ymax,
        }
    }

    /// Parse a two- or three-dimensional metadata bounding box.
    ///
    /// GeoParquet encodes 3D bounding boxes as
    /// `[minx, miny, minz, maxx, maxy, maxz]`, so the Z values are ignored.
    /// Non-finite and inverted bounds return `None` rather than producing an
    /// invalid extent.
    pub fn from_bbox(bbox: &[f64]) -> Option<Self> {
        let (xmin, ymin, xmax, ymax) = match bbox {
            [xmin, ymin, xmax, ymax] => (*xmin, *ymin, *xmax, *ymax),
            [xmin, ymin, _zmin, xmax, ymax, _zmax] => (*xmin, *ymin, *xmax, *ymax),
            _ => return None,
        };

        if ![xmin, ymin, xmax, ymax]
            .iter()
            .all(|value| value.is_finite())
            || xmin > xmax
            || ymin > ymax
        {
            return None;
        }

        Some(Self::new(xmin, ymin, xmax, ymax))
    }

    pub fn xmin(&self) -> f64 {
        self.xmin
    }

    pub fn ymin(&self) -> f64 {
        self.ymin
    }

    pub fn xmax(&self) -> f64 {
        self.xmax
    }

    pub fn ymax(&self) -> f64 {
        self.ymax
    }

    /// Return the smallest extent that contains both inputs.
    pub fn union(self, other: Self) -> Self {
        Self::new(
            self.xmin.min(other.xmin),
            self.ymin.min(other.ymin),
            self.xmax.max(other.xmax),
            self.ymax.max(other.ymax),
        )
    }
}

/// Metadata-derived extents keyed by geometry column name.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SpatialStatistics {
    extents: BTreeMap<String, SpatialExtent>,
}

impl SpatialStatistics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace the extent for `column`.
    pub fn insert(
        &mut self,
        column: impl Into<String>,
        extent: SpatialExtent,
    ) -> Option<SpatialExtent> {
        self.extents.insert(column.into(), extent)
    }

    pub fn get(&self, column: &str) -> Option<SpatialExtent> {
        self.extents.get(column).copied()
    }
}

/// A [`TableProvider`] wrapper that carries [`SpatialStatistics`].
///
/// Every table and scan operation is delegated to `inner`, so wrapping a
/// provider only adds metadata and does not change how data is read.
#[derive(Debug)]
pub struct SpatialStatisticsTable {
    inner: Arc<dyn TableProvider>,
    statistics: SpatialStatistics,
}

impl SpatialStatisticsTable {
    pub fn new(inner: Arc<dyn TableProvider>, statistics: SpatialStatistics) -> Self {
        Self { inner, statistics }
    }

    pub fn statistics(&self) -> &SpatialStatistics {
        &self.statistics
    }
}

#[async_trait]
impl TableProvider for SpatialStatisticsTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        self.inner.scan_with_args(state, args).await
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }

    async fn truncate(&self, state: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.truncate(state).await
    }

    async fn merge_into(
        &self,
        state: &dyn Session,
        source: Arc<dyn ExecutionPlan>,
        merge_schema: DFSchemaRef,
        on: Expr,
        clauses: Vec<MergeIntoClause>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner
            .merge_into(state, source, merge_schema, on, clauses)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_2d_and_3d_bboxes() {
        assert_eq!(
            SpatialExtent::from_bbox(&[1.0, 2.0, 5.0, 6.0]),
            Some(SpatialExtent::new(1.0, 2.0, 5.0, 6.0))
        );
        assert_eq!(
            SpatialExtent::from_bbox(&[1.0, 2.0, -3.0, 5.0, 6.0, 7.0]),
            Some(SpatialExtent::new(1.0, 2.0, 5.0, 6.0))
        );
    }

    #[test]
    fn rejects_invalid_bboxes() {
        assert!(SpatialExtent::from_bbox(&[]).is_none());
        assert!(SpatialExtent::from_bbox(&[1.0, 2.0, 5.0]).is_none());
        assert!(SpatialExtent::from_bbox(&[f64::NAN, 2.0, 5.0, 6.0]).is_none());
        assert!(SpatialExtent::from_bbox(&[5.0, 2.0, 1.0, 6.0]).is_none());
    }

    #[test]
    fn unions_extents() {
        let left = SpatialExtent::new(0.0, 1.0, 2.0, 3.0);
        let right = SpatialExtent::new(-1.0, 2.0, 4.0, 5.0);
        assert_eq!(left.union(right), SpatialExtent::new(-1.0, 1.0, 4.0, 5.0));
    }
}
