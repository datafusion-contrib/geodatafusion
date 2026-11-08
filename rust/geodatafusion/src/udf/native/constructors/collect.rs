use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray};
use arrow_buffer::{NullBuffer, NullBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator, Signature,
};
use datafusion::scalar::ScalarValue;
use geoarrow_schema::{CoordType, GeometryType, Metadata};

mod container;
mod output;
mod state;

use output::assemble_output;
use state::{
    MemberBatches, StateEncoding, compact_retained_batches, decode_state_values,
    filter_to_null_buffer, normalize_input_for_encoding, state_list_array,
};

use crate::data_types::any_single_geometry_type_input;
use crate::error::GeoDataFusionResult;

/// `ST_CollectAgg` aggregate: collects a set of geometries into one MULTI*/GEOMETRYCOLLECTION.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CollectAggregate {
    coord_type: CoordType,
    aliases: Vec<String>,
}

impl CollectAggregate {
    /// Creates an aggregate with the requested output coordinate layout.
    ///
    /// ```
    /// use geoarrow_schema::CoordType;
    /// use geodatafusion::udf::native::constructors::CollectAggregate;
    ///
    /// let collect = CollectAggregate::new(CoordType::Interleaved);
    /// ```
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            coord_type,
            aliases: vec!["st_collect_agg".to_string()],
        }
    }
}

impl Default for CollectAggregate {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl AggregateUDFImpl for CollectAggregate {
    fn name(&self) -> &str {
        "st_collectagg"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        any_single_geometry_type_input()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let metadata = Arc::new(Metadata::try_from(arg_fields[0].as_ref()).unwrap_or_default());
        Ok(Arc::new(
            GeometryType::new(metadata)
                .with_coord_type(self.coord_type)
                .to_field("", true),
        ))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // NOTE: This looks like it is immediately rejecting a lot of queries,
        // but in practice most combinations still work.
        // Neither of these limitations should be too surprising to users,
        // since they are not universally supported in other databases.
        if args.is_distinct {
            return Err(DataFusionError::NotImplemented(
                "ST_CollectAgg does not support DISTINCT".to_string(),
            ));
        }
        if !args.ordering_fields.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "ST_CollectAgg does not support aggregate ORDER BY".to_string(),
            ));
        }

        let input_field = args.input_fields[0].as_ref();
        let metadata = Arc::new(Metadata::try_from(input_field).unwrap_or_default());
        let encoding = StateEncoding::for_field(input_field)?;
        Ok(vec![encoding.state_field(
            args.name,
            &metadata,
            self.coord_type,
        )])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if acc_args.is_distinct {
            return Err(DataFusionError::NotImplemented(
                "ST_CollectAgg does not support DISTINCT yet".to_string(),
            ));
        }
        if !acc_args.order_bys.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "ST_CollectAgg does not support aggregate ORDER BY yet".to_string(),
            ));
        }

        let input_field = acc_args.exprs[0].return_field(acc_args.schema)?;
        let metadata = Arc::new(Metadata::try_from(input_field.as_ref()).unwrap_or_default());
        let encoding = StateEncoding::for_field(input_field.as_ref())?;
        Ok(Box::new(CollectAccumulator {
            batches: MemberBatches::new(encoding),
            input_field,
            metadata,
            coord_type: self.coord_type,
        }))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        // It's not yet decided if we would implement these as simple or groups accumulators.
        // This follows the same pattern as DataFusion's ARRAY_AGG
        // in using ordinary accumulators.
        !args.is_distinct && args.order_bys.is_empty()
    }

    fn create_groups_accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        if acc_args.is_distinct {
            return Err(DataFusionError::NotImplemented(
                "ST_CollectAgg does not support DISTINCT yet".to_string(),
            ));
        }
        if !acc_args.order_bys.is_empty() {
            return Err(DataFusionError::NotImplemented(
                "ST_CollectAgg does not support aggregate ORDER BY yet".to_string(),
            ));
        }

        let input_field = acc_args.exprs[0].return_field(acc_args.schema)?;
        let metadata = Arc::new(Metadata::try_from(input_field.as_ref()).unwrap_or_default());
        let encoding = StateEncoding::for_field(input_field.as_ref())?;
        Ok(Box::new(CollectGroupsAccumulator::new(
            input_field,
            metadata,
            self.coord_type,
            encoding,
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Aggregate that turns a set of geometries (often involving a GROUP BY clause) into a single collection without any changes. \
                 Unlike ST_Union, the included geometries are not processed in any way (e.g. no merging of overlapping geometries, nor inserting nodes at LineString intersections). \
                 Returns a MULTI* if all inputs share a single atomic type (Point/LineString/Polygon), otherwise a GEOMETRYCOLLECTION. \
                 NULL inputs are skipped; an all-NULL or empty group yields NULL.
                 Z and M are preserved, but all inputs must be of the same coordinate dimension
                 (mixing e.g. XY and XYZ is an error).",
                "ST_CollectAgg(geom)",
            )
            .with_argument("geom", "geometry")
            .build()
        }))
    }
}

/// Simple accumulator.
#[derive(Debug)]
struct CollectAccumulator {
    batches: MemberBatches,
    input_field: FieldRef,
    metadata: Arc<Metadata>,
    coord_type: CoordType,
}

impl CollectAccumulator {
    fn update_inner(&mut self, values: &[ArrayRef]) -> GeoDataFusionResult<()> {
        let batch = normalize_input_for_encoding(
            self.batches.encoding(),
            &values[0],
            &self.input_field,
            &self.metadata,
            self.coord_type,
        )?;
        self.batches.push(batch)?;
        Ok(())
    }

    fn evaluate_inner(&self) -> GeoDataFusionResult<ScalarValue> {
        // One group spanning every non-NULL row across all batches.
        let entries = self.batches.non_null_order();
        let groups: [&[(usize, usize)]; 1] = [entries.as_slice()];
        let arr = assemble_output(
            &self.batches,
            &groups,
            self.metadata.clone(),
            self.coord_type,
        )?;
        Ok(ScalarValue::try_from_array(arr.as_ref(), 0)?)
    }
}

impl Accumulator for CollectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        Ok(self.update_inner(values)?)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.evaluate_inner()?)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let order = self.batches.non_null_order();
        let values =
            self.batches
                .state_values(Some(order.as_slice()), &self.metadata, self.coord_type)?;
        let offsets = OffsetBuffer::<i32>::from_lengths([values.len()]);
        // An all-NULL/empty group has no members: emit a NULL list row (matching the groups path)
        // instead of a non-null empty list, so the two partial-state shapes agree.
        let nulls = order.is_empty().then(|| NullBuffer::new_null(1));
        let list = state_list_array(
            self.batches.encoding(),
            offsets,
            values,
            nulls,
            &self.metadata,
            self.coord_type,
        );
        Ok(vec![ScalarValue::List(Arc::new(list))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Each non-NULL list row carries member values in the accumulator's state encoding.
        for inner in states[0].as_list::<i32>().iter().flatten() {
            let batch = decode_state_values(
                self.batches.encoding(),
                &inner,
                &self.metadata,
                self.coord_type,
            )?;
            self.batches.push(batch)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.batches.memory_size()
    }
}

type Entry = (u32, u32);

/// `GroupsAccumulator` for more efficient high-cardinality grouped aggregates.
#[derive(Debug)]
struct CollectGroupsAccumulator {
    input_field: FieldRef,
    metadata: Arc<Metadata>,
    coord_type: CoordType,
    /// Member source arrays referenced by `batch_entries`, fixed to either WKB or native Geometry
    /// state for the lifetime of this accumulator.
    batches: MemberBatches,
    /// Per-batch `(group_idx, row_idx)` pairs for rows that survived filtering and were non-NULL.
    batch_entries: Vec<Vec<Entry>>,
    /// Running sum of the heap capacity of every inner `batch_entries` vector.
    entries_bytes: usize,
    num_groups: usize,
}

impl CollectGroupsAccumulator {
    fn new(
        input_field: FieldRef,
        metadata: Arc<Metadata>,
        coord_type: CoordType,
        encoding: StateEncoding,
    ) -> Self {
        Self {
            input_field,
            metadata,
            coord_type,
            batches: MemberBatches::new(encoding),
            batch_entries: Vec::new(),
            entries_bytes: 0,
            num_groups: 0,
        }
    }

    /// Counting-sort the retained entries for groups `[0, emit_groups)` into group order.
    ///
    /// Returns `(offsets, order)`: `offsets` has `emit_groups + 1` entries delimiting each group's
    /// run, and `order[k] = (batch_idx, row_idx)` is the source of the k-th member in group order. A
    /// group with no members has an empty run (`offsets[g] == offsets[g + 1]`).
    fn group_order(&self, emit_groups: usize) -> (Vec<i32>, Vec<(usize, usize)>) {
        let mut offsets = vec![0i32; emit_groups + 1];
        for entries in &self.batch_entries {
            for &(g, _) in entries {
                let g = g as usize;
                if g < emit_groups {
                    offsets[g + 1] += 1;
                }
            }
        }
        for i in 0..emit_groups {
            offsets[i + 1] += offsets[i];
        }
        let total = offsets[emit_groups] as usize;
        let mut write_pos: Vec<i32> = offsets[..emit_groups].to_vec();
        let mut order = vec![(0usize, 0usize); total];
        for (bi, entries) in self.batch_entries.iter().enumerate() {
            for &(g, r) in entries {
                let g = g as usize;
                if g < emit_groups {
                    let wp = write_pos[g] as usize;
                    order[wp] = (bi, r as usize);
                    write_pos[g] += 1;
                }
            }
        }
        (offsets, order)
    }

    /// Release state for the groups just emitted per `emit_to`.
    fn reset_after_emit(&mut self, emit_to: EmitTo) -> Result<()> {
        match emit_to {
            EmitTo::All => {
                self.batches.clear();
                self.batch_entries.clear();
                self.entries_bytes = 0;
                self.num_groups = 0;
            }
            EmitTo::First(n) => {
                // Retained groups are renumbered to start at zero, matching DataFusion's contract
                // that subsequent group indices are shifted down by the emitted prefix length.
                self.entries_bytes = compact_retained_batches(
                    &mut self.batches,
                    &mut self.batch_entries,
                    n,
                    &self.metadata,
                    self.coord_type,
                )?;
                self.num_groups -= n;
            }
        }
        Ok(())
    }
}

impl GroupsAccumulator for CollectGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // Normalize the input batch once, then retain only `(group,row)` references
        // for retained members.
        let batch = normalize_input_for_encoding(
            self.batches.encoding(),
            &values[0],
            &self.input_field,
            &self.metadata,
            self.coord_type,
        )?;
        self.num_groups = self.num_groups.max(total_num_groups);
        let mut entries = Vec::new();
        for (row, &group) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter
                && (filter.is_null(row) || !filter.value(row))
            {
                continue;
            }
            // NULL geometries contribute nothing (matches PostGIS ST_Collect semantics).
            if batch.is_null(row) {
                continue;
            }
            entries.push((group as u32, row as u32));
        }
        if !entries.is_empty() {
            self.batches.push(batch)?;
            self.entries_bytes += entries.capacity() * std::mem::size_of::<Entry>();
            self.batch_entries.push(entries);
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let emit_groups = match emit_to {
            EmitTo::All => self.num_groups,
            EmitTo::First(n) => n,
        };
        let (offsets, order) = self.group_order(emit_groups);
        let groups: Vec<&[(usize, usize)]> = (0..emit_groups)
            .map(|g| &order[offsets[g] as usize..offsets[g + 1] as usize])
            .collect();
        let array = assemble_output(
            &self.batches,
            &groups,
            self.metadata.clone(),
            self.coord_type,
        )?;
        self.reset_after_emit(emit_to)?;
        Ok(array)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        // Partial state is one list of members per group, in group-index order.
        let emit_groups = match emit_to {
            EmitTo::All => self.num_groups,
            EmitTo::First(n) => n,
        };
        let (offsets, order) = self.group_order(emit_groups);
        let values = self.batches.state_values(
            (!order.is_empty()).then_some(order.as_slice()),
            &self.metadata,
            self.coord_type,
        )?;
        let mut nulls = NullBufferBuilder::new(emit_groups);
        for g in 0..emit_groups {
            if offsets[g] == offsets[g + 1] {
                nulls.append_null();
            } else {
                nulls.append_non_null();
            }
        }
        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let list = state_list_array(
            self.batches.encoding(),
            offsets,
            values,
            nulls.finish(),
            &self.metadata,
            self.coord_type,
        );
        self.reset_after_emit(emit_to)?;
        Ok(vec![Arc::new(list)])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        let list = values[0].as_list::<i32>();
        self.num_groups = self.num_groups.max(total_num_groups);
        let list_offsets = list.offsets();
        let mut entries = Vec::new();
        for (row, &group) in group_indices.iter().enumerate() {
            if list.is_null(row) {
                continue;
            }
            let start = list_offsets[row] as u32;
            let end = list_offsets[row + 1] as u32;
            for pos in start..end {
                entries.push((group as u32, pos));
            }
        }
        if !entries.is_empty() {
            let batch = decode_state_values(
                self.batches.encoding(),
                list.values(),
                &self.metadata,
                self.coord_type,
            )?;
            self.batches.push(batch)?;
            self.entries_bytes += entries.capacity() * std::mem::size_of::<Entry>();
            self.batch_entries.push(entries);
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // Converts to a regular state (DataFusion calls this when the cardinality gets too high).
        let batch = normalize_input_for_encoding(
            self.batches.encoding(),
            &values[0],
            &self.input_field,
            &self.metadata,
            self.coord_type,
        )?;
        let len = batch.len();
        let members = batch.to_array_ref();
        // One length-1 run per row (offsets [0,1,…,len]): list row i wraps member i.
        let offsets = OffsetBuffer::<i32>::from_lengths(std::iter::repeat_n(1usize, len));
        // A list row is non-NULL only where the row passed the filter AND its geometry is non-NULL;
        // otherwise it is a NULL entry that merge_batch skips
        // NullBuffer::union keeps a row valid only where both inputs are valid.
        let filter_nulls = opt_filter.and_then(filter_to_null_buffer);
        let geometry_nulls = batch.logical_nulls();
        let list_nulls = NullBuffer::union(filter_nulls.as_ref(), geometry_nulls.as_ref());
        let list = state_list_array(
            self.batches.encoding(),
            offsets,
            members,
            list_nulls,
            &self.metadata,
            self.coord_type,
        );
        Ok(vec![Arc::new(list)])
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.batches.memory_size()
            + self.batch_entries.capacity() * std::mem::size_of::<Vec<Entry>>()
            + self.entries_bytes
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use geoarrow_array::GeoArrowArray;
    use geoarrow_array::array::from_arrow_array;
    use geoarrow_schema::GeoArrowType;

    use super::*;
    use crate::udf::native::accessors::Dump;
    use crate::udf::native::io::{AsBinary, AsText, GeomFromText};

    //
    // Helpers
    //

    fn register(ctx: &SessionContext) {
        ctx.register_udaf(CollectAggregate::default().into());
        ctx.register_udf(GeomFromText::default().into());
        ctx.register_udf(AsBinary.into());
        ctx.register_udf(AsText.into());
        ctx.register_udf(Dump::default().into());
    }

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        register(&ctx);
        ctx
    }

    /// A context whose plans split aggregation across batches/partitions,
    /// which forces a two-phase aggregation.
    fn ctx_two_phase() -> SessionContext {
        let config = SessionConfig::new()
            .with_batch_size(1)
            .with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);
        register(&ctx);
        ctx
    }

    /// Build a geometry array encoded using arrow-native geometries.
    async fn geom_array(ctx: &SessionContext, wkts: &[Option<&str>]) -> (ArrayRef, FieldRef) {
        let values = wkts
            .iter()
            .map(|w| match w {
                Some(w) => format!("('{w}')"),
                None => "(CAST(NULL AS TEXT))".to_string(),
            })
            .collect::<Vec<_>>()
            .join(",");
        let df = ctx
            .sql(&format!(
                "SELECT ST_GeomFromText(w) AS g FROM (VALUES {values}) AS t(w)"
            ))
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let array = batch.column_by_name("g").unwrap().clone();
        let field = Arc::new(batch.schema().field_with_name("g").unwrap().clone());
        (array, field)
    }

    /// Build a geometry array encoded as WKB.
    async fn wkb_array(ctx: &SessionContext, wkts: &[Option<&str>]) -> (ArrayRef, FieldRef) {
        let values = wkts
            .iter()
            .map(|w| match w {
                Some(w) => format!("('{w}')"),
                None => "(CAST(NULL AS TEXT))".to_string(),
            })
            .collect::<Vec<_>>()
            .join(",");
        let df = ctx
            .sql(&format!(
                "SELECT ST_AsBinary(ST_GeomFromText(w)) AS g FROM (VALUES {values}) AS t(w)"
            ))
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let array = batch.column_by_name("g").unwrap().clone();
        let field = Arc::new(batch.schema().field_with_name("g").unwrap().clone());
        (array, field)
    }

    /// Render a mixed-`Geometry` output array (as produced by `evaluate`)
    /// to WKT, mapping NULL rows to the literal `"NULL"`.
    fn geom_wkts(array: &ArrayRef, input_field: &FieldRef, coord_type: CoordType) -> Vec<String> {
        let metadata = Arc::new(Metadata::try_from(input_field.as_ref()).unwrap());
        let geom_type = GeometryType::new(metadata).with_coord_type(coord_type);
        let output_field = geom_type.to_field("", true);
        let geo = from_arrow_array(array, &output_field).unwrap();
        let wkt = geoarrow_array::cast::to_wkt::<i32>(geo.as_ref())
            .unwrap()
            .to_array_ref();
        let strings = wkt.as_string::<i32>();
        (0..array.len())
            .map(|i| {
                if strings.is_null(i) {
                    "NULL".to_string()
                } else {
                    strings.value(i).to_string()
                }
            })
            .collect()
    }

    /// Build a grouped accumulator the same way the aggregate implementation does.
    ///
    /// Metadata and state encoding both come from the input field: WKB inputs keep WKB aggregate
    /// state, while native/WKT inputs use native `Geometry` state. Tests use the default coordinate
    /// type because that is what `CollectAggregate::default()` registers.
    fn groups_accumulator_for_field(field: FieldRef) -> CollectGroupsAccumulator {
        let metadata = Arc::new(Metadata::try_from(field.as_ref()).unwrap());
        let encoding = StateEncoding::for_field(field.as_ref()).unwrap();
        CollectGroupsAccumulator::new(field, metadata, CoordType::default(), encoding)
    }

    /// Asserts that the input is a GeoArrow Geometry array.
    fn assert_is_native_geometry(array: &ArrayRef) {
        let DataType::List(field) = array.data_type() else {
            panic!("expected List, got {:?}", array.data_type());
        };
        assert!(
            matches!(
                GeoArrowType::from_arrow_field(field.as_ref()).unwrap(),
                GeoArrowType::Geometry(_)
            ),
            "expected List<Geometry> state, got {:?}",
            field
        );
    }

    /// Asserts that the input is a binary array.
    fn array_is_binary(array: &ArrayRef) {
        let DataType::List(field) = array.data_type() else {
            panic!("expected List, got {:?}", array.data_type());
        };
        assert_eq!(field.data_type(), &DataType::Binary);
    }

    /// Run `sql` in `ctx` and return the (nullable) first column of the first row.
    async fn exec_single(ctx: &SessionContext, sql: &str) -> Option<String> {
        let df = ctx.sql(sql).await.unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let col = batch.column(0).as_string::<i32>();
        (!col.is_null(0)).then(|| col.value(0).to_string())
    }

    /// Collect a single VALUES list of WKT literals (single group).
    async fn collect_wkts(ctx: &SessionContext, wkts: &[&str]) -> Option<String> {
        let values = wkts
            .iter()
            .map(|w| format!("('{w}')"))
            .collect::<Vec<_>>()
            .join(",");
        exec_single(
            ctx,
            &format!(
                "SELECT ST_AsText(ST_CollectAgg(ST_GeomFromText(w))) FROM (VALUES {values}) AS t(w)"
            ),
        )
        .await
    }

    #[track_caller]
    fn assert_one_of<T: PartialEq + Debug>(actual: T, expected: &[T]) {
        assert!(
            expected.contains(&actual),
            "expected one of {expected:?}, got {actual:?}"
        );
    }

    //
    // Tests
    //

    #[tokio::test]
    async fn collect_distinct_sql_dedupes_before_udaf() {
        let ctx = ctx();
        let out = exec_single(
            &ctx,
            "SELECT ST_AsText(ST_CollectAgg(DISTINCT ST_GeomFromText(w))) \
             FROM (VALUES ('POINT(0 0)'), ('POINT(0 0)'), ('POINT(1 1)')) AS t(w)",
        )
        .await;
        let out = out.unwrap();
        assert!(out.starts_with("MULTIPOINT"), "{out}");
        assert_eq!(out.matches('(').count(), 3, "{out}");
        assert!(out.contains("(0 0)"), "{out}");
        assert!(out.contains("(1 1)"), "{out}");
    }

    #[tokio::test]
    async fn collect_order_by_not_implemented() {
        // NB: This is not implemented in PostGIS either.
        let ctx = ctx();
        let sql = "SELECT ST_AsText(ST_CollectAgg(ST_GeomFromText(w) ORDER BY k)) \
                   FROM (VALUES (2, 'POINT(2 2)'), (1, 'POINT(1 1)')) AS t(k,w)";
        let err = match ctx.sql(sql).await {
            Ok(df) => df.collect().await.unwrap_err().to_string(),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("not implemented") || err.contains("NotImplemented"),
            "expected a not-implemented error, got: {err}"
        );
        assert!(err.contains("ST_CollectAgg"), "{err}");
        assert!(err.contains("ORDER BY"), "{err}");
    }

    // NB: The following tests have what looks like a lot of work to ensure order-independence.
    // While the "obvious" aggregation is what we get today,
    // the ordering is technically undefined,
    // so I wrote the tests in a way that they won't cause spurious failures later.

    #[tokio::test]
    async fn collect_homogeneous() {
        // Homogeneous atomic inputs collect into the matching MULTI*.
        let ctx = ctx();
        // PostGIS doc example shape: points -> MultiPoint.
        assert_one_of(
            collect_wkts(&ctx, &["POINT(0 0)", "POINT(1 1)", "POINT(2 2)"])
                .await
                .as_deref(),
            &[
                Some("MULTIPOINT((0 0),(1 1),(2 2))"),
                Some("MULTIPOINT((0 0),(2 2),(1 1))"),
                Some("MULTIPOINT((1 1),(0 0),(2 2))"),
                Some("MULTIPOINT((1 1),(2 2),(0 0))"),
                Some("MULTIPOINT((2 2),(0 0),(1 1))"),
                Some("MULTIPOINT((2 2),(1 1),(0 0))"),
            ],
        );
        // PostGIS doc example: lines -> MultiLineString.
        assert_one_of(
            collect_wkts(&ctx, &["LINESTRING(1 2,3 4)", "LINESTRING(3 4,4 5)"])
                .await
                .as_deref(),
            &[
                Some("MULTILINESTRING((1 2,3 4),(3 4,4 5))"),
                Some("MULTILINESTRING((3 4,4 5),(1 2,3 4))"),
            ],
        );
        // A single input still becomes a MULTI* of one.
        assert_eq!(
            collect_wkts(&ctx, &["POINT(5 5)"]).await.as_deref(),
            Some("MULTIPOINT((5 5))")
        );
    }

    #[tokio::test]
    async fn collect_mixed_to_gc() {
        let ctx = ctx();
        assert_one_of(
            collect_wkts(&ctx, &["POINT(0 0)", "LINESTRING(1 1,2 2)"])
                .await
                .as_deref(),
            &[
                Some("GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 2))"),
                Some("GEOMETRYCOLLECTION(LINESTRING(1 1,2 2),POINT(0 0))"),
            ],
        );
    }

    #[tokio::test]
    async fn collect_mixed_dimensions_errors() {
        let ctx = ctx();
        let df = ctx
            .sql(
                "SELECT ST_AsText(ST_CollectAgg(ST_GeomFromText(w))) \
                 FROM (VALUES ('POINT(0 0)'), ('POINT Z(1 1 1)')) AS t(w)",
            )
            .await
            .unwrap();
        let err = df.collect().await.unwrap_err();
        assert!(
            err.to_string().contains("dimension"),
            "expected a dimension error, got: {err}",
        );
    }

    #[tokio::test]
    async fn collect_empty_member() {
        let ctx = ctx();
        assert_one_of(
            collect_wkts(&ctx, &["LINESTRING(0 0,1 1)", "LINESTRING EMPTY"])
                .await
                .as_deref(),
            &[
                Some("MULTILINESTRING((0 0,1 1),())"),
                Some("MULTILINESTRING((),(0 0,1 1))"),
            ],
        );
    }

    #[tokio::test]
    async fn collect_preserves_z() {
        let ctx = ctx();
        assert_one_of(
            collect_wkts(&ctx, &["POINT Z(1 2 3)", "POINT Z(1 2 4)"])
                .await
                .as_deref(),
            &[
                Some("MULTIPOINT Z((1 2 3),(1 2 4))"),
                Some("MULTIPOINT Z((1 2 4),(1 2 3))"),
            ],
        );
    }

    #[tokio::test]
    async fn collect_nulls() {
        let ctx = ctx();
        let one_null = exec_single(
            &ctx,
            "SELECT ST_AsText(ST_CollectAgg(ST_GeomFromText(w))) \
             FROM (VALUES ('POINT(0 0)'), (CAST(NULL AS TEXT)), ('POINT(1 1)')) AS t(w)",
        )
        .await;
        assert_one_of(
            one_null.as_deref(),
            &[
                Some("MULTIPOINT((0 0),(1 1))"),
                Some("MULTIPOINT((1 1),(0 0))"),
            ],
        );

        let all_null = exec_single(
            &ctx,
            "SELECT ST_AsText(ST_CollectAgg(ST_GeomFromText(w))) \
             FROM (VALUES (CAST(NULL AS TEXT))) AS t(w)",
        )
        .await;
        assert_eq!(all_null, None);
    }

    #[tokio::test]
    async fn collect_group_by() {
        let ctx = ctx();
        let df = ctx
            .sql(
                "SELECT k, ST_AsText(ST_CollectAgg(ST_GeomFromText(w))) AS g \
                 FROM (VALUES (1,'POINT(0 0)'),(1,'POINT(1 1)'),(2,'POINT(9 9)')) AS t(k,w) \
                 GROUP BY k ORDER BY k",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let g = batch.column_by_name("g").unwrap().as_string::<i32>();
        assert_one_of(
            Some(g.value(0)),
            &[
                Some("MULTIPOINT((0 0),(1 1))"),
                Some("MULTIPOINT((1 1),(0 0))"),
            ],
        );
        assert_eq!(g.value(1), "MULTIPOINT((9 9))");
    }

    #[tokio::test]
    async fn roundtrip_dump_collect() {
        let ctx = ctx();

        // Extract the component WKTs that ST_Dump produces for `wkt`.
        async fn dump_component_wkts(ctx: &SessionContext, wkt: &str) -> Vec<String> {
            let df = ctx
                .sql(&format!("SELECT ST_Dump(ST_GeomFromText('{wkt}'))"))
                .await
                .unwrap();
            let batch = df.collect().await.unwrap().into_iter().next().unwrap();
            let structs = batch.column(0).as_list::<i32>().value(0);
            let structs = structs.as_struct();
            let DataType::Struct(fields) = structs.data_type() else {
                panic!("ST_Dump returns a list of structs");
            };
            let geo = from_arrow_array(structs.column(1).as_ref(), fields[1].as_ref()).unwrap();
            let wkt_ref = geoarrow_array::cast::to_wkt::<i32>(geo.as_ref())
                .unwrap()
                .to_array_ref();
            let wkts = wkt_ref.as_string::<i32>();
            (0..structs.len())
                .map(|i| wkts.value(i).to_string())
                .collect()
        }

        let cases: &[(&str, &[&str])] = &[
            (
                "MULTIPOINT((0 0),(1 1),(2 2))",
                &[
                    "MULTIPOINT((0 0),(1 1),(2 2))",
                    "MULTIPOINT((0 0),(2 2),(1 1))",
                    "MULTIPOINT((1 1),(0 0),(2 2))",
                    "MULTIPOINT((1 1),(2 2),(0 0))",
                    "MULTIPOINT((2 2),(0 0),(1 1))",
                    "MULTIPOINT((2 2),(1 1),(0 0))",
                ],
            ),
            (
                "MULTILINESTRING((0 0,1 1),(2 2,3 3))",
                &[
                    "MULTILINESTRING((0 0,1 1),(2 2,3 3))",
                    "MULTILINESTRING((2 2,3 3),(0 0,1 1))",
                ],
            ),
            (
                "MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((10 10,10 20,20 20,20 10,10 10)))",
                &[
                    "MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)),((10 10,10 20,20 20,20 10,10 10)))",
                    "MULTIPOLYGON(((10 10,10 20,20 20,20 10,10 10)),((0 0,0 1,1 1,1 0,0 0)))",
                ],
            ),
        ];
        for (orig, expected) in cases {
            let parts = dump_component_wkts(&ctx, orig).await;
            let reconstructed =
                collect_wkts(&ctx, &parts.iter().map(String::as_str).collect::<Vec<_>>())
                    .await
                    .unwrap();
            assert_one_of(reconstructed.as_str(), expected);
        }
    }

    #[tokio::test]
    async fn wkb_output_matches_native_for_collections_and_coordinate_layouts() {
        // This test verifies that WKB-backed state must produce the same geometry as native state,
        // regardless of whether output coordinates are interleaved (XYZXYZ) or separated (XX, YY, ZZ).
        let cases: &[(&[&str], &[&str])] = &[
            (
                &["POINT(0 0)", "POINT(1 2)"],
                &["MULTIPOINT((0 0),(1 2))", "MULTIPOINT((1 2),(0 0))"],
            ),
            (
                &["LINESTRING(0 0,1 1)", "LINESTRING(1 1,2 3)"],
                &[
                    "MULTILINESTRING((0 0,1 1),(1 1,2 3))",
                    "MULTILINESTRING((1 1,2 3),(0 0,1 1))",
                ],
            ),
            (
                &["POLYGON((0 0,1 0,0 1,0 0))", "POLYGON((2 2,3 2,2 3,2 2))"],
                &[
                    "MULTIPOLYGON(((0 0,1 0,0 1,0 0)),((2 2,3 2,2 3,2 2)))",
                    "MULTIPOLYGON(((2 2,3 2,2 3,2 2)),((0 0,1 0,0 1,0 0)))",
                ],
            ),
            (
                &["POLYGON EMPTY", "LINESTRING(0 0,1 1)"],
                &[
                    "GEOMETRYCOLLECTION(POLYGON EMPTY,LINESTRING(0 0,1 1))",
                    "GEOMETRYCOLLECTION(LINESTRING(0 0,1 1),POLYGON EMPTY)",
                ],
            ),
            (
                &["POINT(0 0)", "LINESTRING(0 0,1 1)"],
                &[
                    "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,1 1))",
                    "GEOMETRYCOLLECTION(LINESTRING(0 0,1 1),POINT(0 0))",
                ],
            ),
            (
                &["MULTILINESTRING((0 0,1 1))", "MULTILINESTRING((2 2,3 3))"],
                &[
                    "GEOMETRYCOLLECTION(MULTILINESTRING((0 0,1 1)),MULTILINESTRING((2 2,3 3)))",
                    "GEOMETRYCOLLECTION(MULTILINESTRING((2 2,3 3)),MULTILINESTRING((0 0,1 1)))",
                ],
            ),
            (
                &["LINESTRING EMPTY", "LINESTRING EMPTY"],
                &["MULTILINESTRING((),())"],
            ),
            (
                &["POINT Z(0 0 1)", "POINT Z(2 3 4)"],
                &[
                    "MULTIPOINT Z((0 0 1),(2 3 4))",
                    "MULTIPOINT Z((2 3 4),(0 0 1))",
                ],
            ),
            (
                &["POINT M(0 0 1)", "POINT M(2 3 4)"],
                &[
                    "MULTIPOINT M((0 0 1),(2 3 4))",
                    "MULTIPOINT M((2 3 4),(0 0 1))",
                ],
            ),
            (
                &["POINT ZM(0 0 1 2)", "POINT ZM(3 4 5 6)"],
                &[
                    "MULTIPOINT ZM((0 0 1 2),(3 4 5 6))",
                    "MULTIPOINT ZM((3 4 5 6),(0 0 1 2))",
                ],
            ),
        ];
        for coord_type in [CoordType::Interleaved, CoordType::Separated] {
            let ctx = ctx();
            ctx.register_udaf(CollectAggregate::new(coord_type).into());
            for (wkts, expected) in cases {
                let native = collect_wkts(&ctx, wkts).await.unwrap();
                assert_one_of(native.as_str(), expected);
                // ST_AsBinary forces WKB state; both paths must match an allowed result.
                let values = wkts
                    .iter()
                    .map(|w| format!("('{w}')"))
                    .collect::<Vec<_>>()
                    .join(",");
                let sql = format!(
                    "SELECT ST_AsText(ST_CollectAgg(ST_AsBinary(ST_GeomFromText(w)))) \
                     FROM (VALUES {values}) AS t(w)"
                );
                let wkb = exec_single(&ctx, &sql).await.unwrap();
                assert_one_of(wkb.as_str(), expected);
            }
            // All-NULL input produces NULL, unlike a group containing empty geometries.
            assert_eq!(
                exec_single(
                    &ctx,
                    "SELECT ST_AsText(ST_CollectAgg(ST_AsBinary(ST_GeomFromText(w)))) \
                     FROM (VALUES (CAST(NULL AS VARCHAR)), (NULL)) AS t(w)"
                )
                .await,
                None,
                "all-NULL WKB members must yield NULL with {coord_type:?} output"
            );

            // Different dimensions must be rejected, rather than dropping or inventing ordinates.
            let error = ctx
                .sql(
                    "SELECT ST_CollectAgg(ST_AsBinary(ST_GeomFromText(w))) \
                 FROM (VALUES ('POINT(0 0)'), ('POINT Z(1 2 3)')) AS t(w)",
                )
                .await
                .unwrap()
                .collect()
                .await
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("differing coordinate dimensions"),
                "mixed XY/XYZ WKB must fail for {coord_type:?} output: {error}"
            );
        }
    }

    //
    // Various contrived cases that we verified against PostGIS
    //

    #[tokio::test]
    async fn collect_parity_collection_members() {
        let ctx = ctx();
        // atomic + its own MULTI* -> GC (NOT flattened into a single MULTILINESTRING).
        assert_one_of(
            collect_wkts(
                &ctx,
                &[
                    "LINESTRING(0 0,1 1)",
                    "MULTILINESTRING((2 2,3 3),(4 4,5 5))",
                ],
            )
            .await
            .as_deref(),
            &[
                Some(
                    "GEOMETRYCOLLECTION(LINESTRING(0 0,1 1),MULTILINESTRING((2 2,3 3),(4 4,5 5)))",
                ),
                Some(
                    "GEOMETRYCOLLECTION(MULTILINESTRING((2 2,3 3),(4 4,5 5)),LINESTRING(0 0,1 1))",
                ),
            ],
        );
        // two MULTI* of the same kind -> GC of two multis (still not flattened).
        assert_one_of(
            collect_wkts(
                &ctx,
                &["MULTILINESTRING((0 0,1 1))", "MULTILINESTRING((2 2,3 3))"],
            )
            .await
            .as_deref(),
            &[
                Some("GEOMETRYCOLLECTION(MULTILINESTRING((0 0,1 1)),MULTILINESTRING((2 2,3 3)))"),
                Some("GEOMETRYCOLLECTION(MULTILINESTRING((2 2,3 3)),MULTILINESTRING((0 0,1 1)))"),
            ],
        );
        // atomic + a MULTI* of another kind -> GC.
        assert_one_of(
            collect_wkts(&ctx, &["POINT(0 0)", "MULTIPOINT((1 1),(2 2))"])
                .await
                .as_deref(),
            &[
                Some("GEOMETRYCOLLECTION(POINT(0 0),MULTIPOINT((1 1),(2 2)))"),
                Some("GEOMETRYCOLLECTION(MULTIPOINT((1 1),(2 2)),POINT(0 0))"),
            ],
        );
    }

    #[tokio::test]
    async fn collect_parity_empty_members() {
        let ctx = ctx();
        // Homogeneous empties still promote to the MULTI* (not NULL); each empty is kept.
        assert_eq!(
            collect_wkts(&ctx, &["LINESTRING EMPTY", "LINESTRING EMPTY"])
                .await
                .as_deref(),
            Some("MULTILINESTRING((),())"), // PostGIS: MULTILINESTRING(EMPTY,EMPTY)
        );
        // Mixed bases (empty polygon + line) -> GC, the empty preserved.
        assert_one_of(
            collect_wkts(&ctx, &["POLYGON EMPTY", "LINESTRING(0 0,1 1)"])
                .await
                .as_deref(),
            &[
                Some("GEOMETRYCOLLECTION(POLYGON EMPTY,LINESTRING(0 0,1 1))"),
                Some("GEOMETRYCOLLECTION(LINESTRING(0 0,1 1),POLYGON EMPTY)"),
            ],
        );
    }

    #[tokio::test]
    async fn collect_parity_zm_dimensions() {
        let ctx = ctx();
        assert_one_of(
            collect_wkts(&ctx, &["POINT ZM(0 0 1 5)", "POINT ZM(1 1 2 6)"])
                .await
                .as_deref(),
            &[
                Some("MULTIPOINT ZM((0 0 1 5),(1 1 2 6))"),
                Some("MULTIPOINT ZM((1 1 2 6),(0 0 1 5))"),
            ],
        );
        assert_one_of(
            collect_wkts(&ctx, &["POINT M(0 0 5)", "POINT M(1 1 6)"])
                .await
                .as_deref(),
            &[
                Some("MULTIPOINT M((0 0 5),(1 1 6))"),
                Some("MULTIPOINT M((1 1 6),(0 0 5))"),
            ],
        );
    }

    //
    // Grouping tests
    //

    #[tokio::test]
    async fn collect_group_by_two_phase() {
        let ctx = ctx_two_phase();
        let df = ctx
            .sql(
                "SELECT k, ST_AsText(ST_CollectAgg(ST_GeomFromText(w))) AS g FROM (VALUES \
                 (1,'POINT(0 0)'),(2,'POINT(5 5)'),(1,'POINT(1 1)'),(2,'POINT(6 6)'),(1,'POINT(2 2)') \
                 ) AS t(k,w) GROUP BY k ORDER BY k",
            )
            .await
            .unwrap();
        // `batch_size(1)` splits the output into one-row batches; gather them in (ORDER BY k) order.
        let batches = df.collect().await.unwrap();
        let rows: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let g = b.column_by_name("g").unwrap().as_string::<i32>();
                (0..b.num_rows())
                    .map(|i| g.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(rows.len(), 2);
        for (row, pts) in [
            (0usize, ["(0 0)", "(1 1)", "(2 2)"].as_slice()),
            (1usize, ["(5 5)", "(6 6)"].as_slice()),
        ] {
            let v = &rows[row];
            assert!(v.starts_with("MULTIPOINT"), "row {row}: {v}");
            assert_eq!(v.matches('(').count(), pts.len() + 1, "row {row}: {v}");
            for p in pts {
                assert!(v.contains(p), "row {row}: {v} missing {p}");
            }
        }
    }

    #[tokio::test]
    async fn collect_group_by_heterogeneous() {
        let ctx = ctx();
        let df = ctx
            .sql(
                "SELECT k, ST_AsText(ST_CollectAgg(ST_GeomFromText(w))) AS g FROM (VALUES \
                 (1,'POINT(0 0)'),(1,'POINT(1 1)'), \
                 (2,'POINT(2 2)'),(2,'LINESTRING(3 3,4 4)'), \
                 (3,CAST(NULL AS TEXT)) \
                 ) AS t(k,w) GROUP BY k ORDER BY k",
            )
            .await
            .unwrap();
        let batch = df.collect().await.unwrap().into_iter().next().unwrap();
        let g = batch.column_by_name("g").unwrap().as_string::<i32>();
        assert_one_of(
            Some(g.value(0)),
            &[
                Some("MULTIPOINT((0 0),(1 1))"),
                Some("MULTIPOINT((1 1),(0 0))"),
            ],
        );
        assert_one_of(
            Some(g.value(1)),
            &[
                Some("GEOMETRYCOLLECTION(POINT(2 2),LINESTRING(3 3,4 4))"),
                Some("GEOMETRYCOLLECTION(LINESTRING(3 3,4 4),POINT(2 2))"),
            ],
        );
        assert!(g.is_null(2), "all-NULL group should be NULL");
    }

    //
    // Accumulator edges
    //

    #[tokio::test]
    async fn collect_ungrouped_multi_batch() {
        let ctx = ctx_two_phase();
        let v = exec_single(
            &ctx,
            "SELECT ST_AsText(ST_CollectAgg(ST_GeomFromText(w))) FROM (VALUES \
             ('POINT(0 0)'),('POINT(1 1)'),('POINT(2 2)'),('POINT(3 3)')) AS t(w)",
        )
        .await
        .unwrap();
        assert!(v.starts_with("MULTIPOINT"), "{v}");
        assert_eq!(v.matches('(').count(), 5, "{v}"); // 4 members + outer paren
        for p in ["(0 0)", "(1 1)", "(2 2)", "(3 3)"] {
            assert!(v.contains(p), "{v} missing {p}");
        }
    }

    #[tokio::test]
    async fn groups_accumulator_convert_to_state() {
        let ctx = ctx();
        let (array, field) = geom_array(
            &ctx,
            &[Some("POINT(0 0)"), Some("POINT(1 1)"), Some("POINT(2 2)")],
        )
        .await;
        let acc = groups_accumulator_for_field(field.clone());
        let states = acc.convert_to_state(&[array], None).unwrap();
        assert_is_native_geometry(&states[0]);

        // rows 0 & 2 -> group 0, row 1 -> group 1.
        let mut merged = groups_accumulator_for_field(field.clone());
        merged.merge_batch(&states, &[0, 1, 0], 2).unwrap();
        let out = merged.evaluate(EmitTo::All).unwrap();

        assert_one_of(
            geom_wkts(&out, &field, CoordType::default()),
            &[
                vec![
                    "MULTIPOINT((0 0),(2 2))".to_string(),
                    "MULTIPOINT((1 1))".to_string(),
                ],
                vec![
                    "MULTIPOINT((2 2),(0 0))".to_string(),
                    "MULTIPOINT((1 1))".to_string(),
                ],
            ],
        );
    }

    #[tokio::test]
    async fn groups_accumulator_convert_to_state_wkb_encoding() {
        let ctx = ctx();
        let (array, field) = wkb_array(
            &ctx,
            &[Some("POINT(0 0)"), Some("POINT(1 1)"), Some("POINT(2 2)")],
        )
        .await;
        let acc = groups_accumulator_for_field(field.clone());
        let states = acc.convert_to_state(&[array], None).unwrap();
        array_is_binary(&states[0]);

        let mut merged = groups_accumulator_for_field(field.clone());
        merged.merge_batch(&states, &[0, 1, 0], 2).unwrap();
        let out = merged.evaluate(EmitTo::All).unwrap();

        assert_one_of(
            geom_wkts(&out, &field, CoordType::default()),
            &[
                vec![
                    "MULTIPOINT((0 0),(2 2))".to_string(),
                    "MULTIPOINT((1 1))".to_string(),
                ],
                vec![
                    "MULTIPOINT((2 2),(0 0))".to_string(),
                    "MULTIPOINT((1 1))".to_string(),
                ],
            ],
        );
    }

    #[tokio::test]
    async fn groups_accumulator_update_batch_filter() {
        let ctx = ctx();
        let (array, field) = geom_array(
            &ctx,
            &[
                Some("POINT(0 0)"),
                Some("POINT(1 1)"),
                Some("POINT(2 2)"),
                Some("POINT(3 3)"),
            ],
        )
        .await;
        let mut acc = groups_accumulator_for_field(field.clone());
        let filter = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);
        acc.update_batch(&[array], &[0, 0, 0, 1], Some(&filter), 2)
            .unwrap();

        let out = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            geom_wkts(&out, &field, CoordType::default()),
            vec![
                "MULTIPOINT((0 0))".to_string(),
                "MULTIPOINT((3 3))".to_string()
            ],
        );
    }

    #[tokio::test]
    async fn groups_accumulator_state_emit_first_then_merge() {
        let ctx = ctx();
        let (array, field) = geom_array(
            &ctx,
            &[Some("POINT(0 0)"), Some("POINT(1 1)"), Some("POINT(2 2)")],
        )
        .await;
        let mut acc = groups_accumulator_for_field(field.clone());
        acc.update_batch(&[array], &[0, 1, 2], None, 3).unwrap();

        let first_state = acc.state(EmitTo::First(1)).unwrap();
        let mut merged_first = groups_accumulator_for_field(field.clone());
        merged_first.merge_batch(&first_state, &[0], 1).unwrap();
        let first_out = merged_first.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            geom_wkts(&first_out, &field, CoordType::default()),
            vec!["MULTIPOINT((0 0))".to_string()],
        );

        let (array2, _) = geom_array(&ctx, &[Some("POINT(9 9)")]).await;
        acc.update_batch(&[array2], &[1], None, 2).unwrap();
        let rest = acc.evaluate(EmitTo::All).unwrap();
        assert_one_of(
            geom_wkts(&rest, &field, CoordType::default()),
            &[
                vec![
                    "MULTIPOINT((1 1))".to_string(),
                    "MULTIPOINT((2 2),(9 9))".to_string(),
                ],
                vec![
                    "MULTIPOINT((1 1))".to_string(),
                    "MULTIPOINT((9 9),(2 2))".to_string(),
                ],
            ],
        );
    }

    #[tokio::test]
    async fn groups_accumulator_emit_first_wkb_encoding() {
        let ctx = ctx();
        let (array, field) = wkb_array(
            &ctx,
            &[Some("POINT(0 0)"), Some("POINT(1 1)"), Some("POINT(2 2)")],
        )
        .await;
        let mut acc = groups_accumulator_for_field(field.clone());
        acc.update_batch(&[array], &[0, 1, 2], None, 3).unwrap();

        let first = acc.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!(
            geom_wkts(&first, &field, CoordType::default()),
            vec!["MULTIPOINT((0 0))".to_string()],
        );

        let (array2, _) = wkb_array(&ctx, &[Some("POINT(9 9)")]).await;
        acc.update_batch(&[array2], &[1], None, 2).unwrap();
        let rest = acc.evaluate(EmitTo::All).unwrap();
        assert_one_of(
            geom_wkts(&rest, &field, CoordType::default()),
            &[
                vec![
                    "MULTIPOINT((1 1))".to_string(),
                    "MULTIPOINT((2 2),(9 9))".to_string(),
                ],
                vec![
                    "MULTIPOINT((1 1))".to_string(),
                    "MULTIPOINT((9 9),(2 2))".to_string(),
                ],
            ],
        );
    }

    #[tokio::test]
    async fn groups_accumulator_emit_first() {
        // `EmitTo::First` emits a prefix of groups and shifts the rest down, exercising retained-row
        // compaction and group renumbering.
        let ctx = ctx();
        let (array, field) = geom_array(
            &ctx,
            &[Some("POINT(0 0)"), Some("POINT(1 1)"), Some("POINT(2 2)")],
        )
        .await;
        let mut acc = groups_accumulator_for_field(field.clone());
        acc.update_batch(&[array], &[0, 1, 2], None, 3).unwrap();

        // Emit group 0 only; groups 1,2 shift down to 0,1.
        let first = acc.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!(
            geom_wkts(&first, &field, CoordType::default()),
            vec!["MULTIPOINT((0 0))".to_string()],
        );

        // Add a member to (shifted) group 1, originally group 2.
        let (array2, _) = geom_array(&ctx, &[Some("POINT(9 9)")]).await;
        acc.update_batch(&[array2], &[1], None, 2).unwrap();
        let rest = acc.evaluate(EmitTo::All).unwrap();
        assert_one_of(
            geom_wkts(&rest, &field, CoordType::default()),
            &[
                vec![
                    "MULTIPOINT((1 1))".to_string(),
                    "MULTIPOINT((2 2),(9 9))".to_string(),
                ],
                vec![
                    "MULTIPOINT((1 1))".to_string(),
                    "MULTIPOINT((9 9),(2 2))".to_string(),
                ],
            ],
        );
    }

    #[tokio::test]
    async fn groups_accumulator_emit_first_shrinks_footprint() {
        // `EmitTo::First` must shrink the incrementally-tracked footprint as members drain.
        // One batch straddles the emit cutoff, exercising the `Replace`/`take` compaction path:
        // group 0 (the large emitted prefix) is dropped, group 1 (one member) is retained.
        let ctx = ctx();
        let mut wkts: Vec<Option<&str>> = vec![Some("POINT(0 0)"); 100];
        wkts.push(Some("POINT(9 9)"));
        let (array, field) = geom_array(&ctx, &wkts).await;
        let mut groups: Vec<usize> = vec![0; 100];
        groups.push(1);
        let mut acc = groups_accumulator_for_field(field.clone());
        acc.update_batch(&[array], &groups, None, 2).unwrap();

        let before = acc.size();
        acc.evaluate(EmitTo::First(1)).unwrap();
        let after = acc.size();
        assert!(
            after < before,
            "footprint should shrink after EmitTo::First drops a group: {before} -> {after}"
        );

        // The retained group (1 -> 0) still assembles correctly from the compacted batch.
        let rest = acc.evaluate(EmitTo::All).unwrap();
        assert_eq!(
            geom_wkts(&rest, &field, CoordType::default()),
            vec!["MULTIPOINT((9 9))".to_string()],
        );
    }

    #[tokio::test]
    async fn groups_accumulator_convert_to_state_nulls() {
        let ctx = ctx();
        let (array, field) =
            geom_array(&ctx, &[Some("POINT(0 0)"), None, Some("POINT(2 2)")]).await;
        let acc = groups_accumulator_for_field(field.clone());
        let states = acc.convert_to_state(&[array], None).unwrap();
        assert_is_native_geometry(&states[0]);

        // row 0 -> group 0, NULL row 1 -> group 1 (stays empty -> NULL), row 2 -> group 0.
        let mut merged = groups_accumulator_for_field(field.clone());
        merged.merge_batch(&states, &[0, 1, 0], 2).unwrap();
        let out = merged.evaluate(EmitTo::All).unwrap();

        // NULL and filtered-out rows are dropped by `convert_to_state` (NULL lists), so they never reach
        // a group; an all-NULL group emits NULL.
        assert_one_of(
            geom_wkts(&out, &field, CoordType::default()),
            &[
                vec!["MULTIPOINT((0 0),(2 2))".to_string(), "NULL".to_string()],
                vec!["MULTIPOINT((2 2),(0 0))".to_string(), "NULL".to_string()],
            ],
        );
    }

    #[tokio::test]
    async fn groups_accumulator_convert_to_state_filter_and_nulls() {
        let ctx = ctx();
        let (array, field) = geom_array(
            &ctx,
            &[
                Some("POINT(0 0)"),
                None,
                Some("POINT(2 2)"),
                Some("POINT(3 3)"),
            ],
        )
        .await;
        let acc = groups_accumulator_for_field(field.clone());
        let filter = BooleanArray::from(vec![Some(true), Some(true), Some(false), Some(true)]);
        let states = acc.convert_to_state(&[array], Some(&filter)).unwrap();
        assert_is_native_geometry(&states[0]);

        let mut merged = groups_accumulator_for_field(field.clone());
        merged.merge_batch(&states, &[0, 1, 0, 1], 2).unwrap();
        let out = merged.evaluate(EmitTo::All).unwrap();

        assert_eq!(
            geom_wkts(&out, &field, CoordType::default()),
            vec![
                "MULTIPOINT((0 0))".to_string(),
                "MULTIPOINT((3 3))".to_string()
            ],
        );
    }
}
