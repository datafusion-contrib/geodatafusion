use std::sync::{Arc, OnceLock};

use arrow_array::builder::{BooleanBuilder, Int32Builder};
use arrow_array::cast::AsArray;
use arrow_array::{Array, Int32Array};
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use geo::Orientation;
use geo::algorithm::kernels::{Kernel, SimpleKernel};
use geo_traits::{CoordTrait, GeometryTrait, GeometryType, LineStringTrait};
use geoarrow_array::cast::to_wkb;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::Metadata;
use wkb::Endianness;
use wkb::reader::Wkb;
use wkb::writer::{WriteOptions, write_geometry};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{
    geometry_array_from_arrow, is_null_geometry_input, merge_metadata,
};

const DIMENSION_DONT_CARE: i32 = -3;
const DIMENSION_TRUE: i32 = -2;
const DIMENSION_FALSE: i32 = -1;
const DIMENSION_POINT: i32 = 0;
const DIMENSION_LINE: i32 = 1;
const DIMENSION_AREA: i32 = 2;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct OrderingEquals {
    signature: Signature,
}

impl OrderingEquals {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(2, any_geometry_type(), Volatility::Immutable),
        }
    }
}

impl Default for OrderingEquals {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for OrderingEquals {
    fn name(&self) -> &str {
        "st_orderingequals"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ordering_equals_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        static DOCUMENTATION: std::sync::OnceLock<Documentation> = std::sync::OnceLock::new();
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns true only when both geometries have the same serialized structure, including coordinate order, Z/M dimensions, SRID, and signed zero values.",
                "ST_OrderingEquals(geomA, geomB)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("geomB", "geometry")
            .build()
        }))
    }
}

fn ordering_equals_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let left_metadata = Arc::new(Metadata::try_from(args.arg_fields[0].as_ref())?);
    let right_metadata = Arc::new(Metadata::try_from(args.arg_fields[1].as_ref())?);
    let crs_equal = crs_equal(&left_metadata, &right_metadata);

    if matches!(args.arg_fields[0].data_type(), DataType::Null)
        || matches!(args.arg_fields[1].data_type(), DataType::Null)
    {
        let mut builder = BooleanBuilder::new();
        builder.append_nulls(args.number_rows);
        return Ok(ColumnarValue::Array(Arc::new(builder.finish())));
    }

    let left = geometry_array_from_arrow(arrays[0].as_ref(), args.arg_fields[0].as_ref())?;
    let right = geometry_array_from_arrow(arrays[1].as_ref(), args.arg_fields[1].as_ref())?;
    let left_wkb = to_wkb::<i32>(left.as_ref())?.into_array_ref();
    let right_wkb = to_wkb::<i32>(right.as_ref())?.into_array_ref();
    let left_wkb = left_wkb.as_binary::<i32>();
    let right_wkb = right_wkb.as_binary::<i32>();

    let mut builder = BooleanBuilder::with_capacity(args.number_rows);
    for index in 0..args.number_rows {
        if left_wkb.is_null(index) || right_wkb.is_null(index) {
            builder.append_null();
        } else {
            let left = canonical_wkb(left_wkb.value(index))?;
            let right = canonical_wkb(right_wkb.value(index))?;
            builder.append_value(crs_equal && left == right);
        }
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn canonical_wkb(bytes: &[u8]) -> GeoDataFusionResult<Vec<u8>> {
    let geometry = Wkb::try_new(bytes).map_err(|error| {
        DataFusionError::Execution(format!("ST_OrderingEquals input WKB is invalid: {error}"))
    })?;
    let mut output = Vec::new();
    write_geometry(
        &mut output,
        &geometry,
        &WriteOptions {
            endianness: Endianness::LittleEndian,
        },
    )
    .map_err(|error| {
        DataFusionError::Execution(format!(
            "ST_OrderingEquals failed to encode canonical WKB: {error}"
        ))
    })?;
    Ok(output)
}

fn crs_equal(left: &Metadata, right: &Metadata) -> bool {
    let left_unknown = crs_is_unknown(left);
    let right_unknown = crs_is_unknown(right);
    if left_unknown || right_unknown {
        left_unknown && right_unknown
    } else {
        left.crs() == right.crs()
    }
}

fn crs_is_unknown(metadata: &Metadata) -> bool {
    let Some(value) = metadata.crs().crs_value() else {
        return true;
    };
    matches!(value.as_str(), Some("EPSG:0" | "0"))
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct RelateMatch {
    signature: Signature,
}

impl RelateMatch {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                2,
                vec![DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for RelateMatch {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RelateMatch {
    fn name(&self) -> &str {
        "st_relatematch"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(relate_match_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        static DOCUMENTATION: std::sync::OnceLock<Documentation> = std::sync::OnceLock::new();
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Tests whether a DE-9IM matrix text value matches a nine-character matrix pattern. The pattern may use F, T, *, 0, 1, and 2.",
                "ST_RelateMatch(matrix, matrixPattern)",
            )
            .with_argument("matrix", "DE-9IM matrix text")
            .with_argument("matrixPattern", "nine-character DE-9IM pattern")
            .build()
        }))
    }
}

fn relate_match_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let mut builder = BooleanBuilder::with_capacity(args.number_rows);

    for index in 0..args.number_rows {
        if arrays[0].is_null(index) || arrays[1].is_null(index) {
            builder.append_null();
            continue;
        }

        let matrix = string_value(arrays[0].as_ref(), index)?;
        let pattern = string_value(arrays[1].as_ref(), index)?;
        builder.append_value(relate_match(matrix, pattern)?);
    }

    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn string_value(array: &dyn Array, index: usize) -> GeoDataFusionResult<&str> {
    Ok(match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(index),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<arrow_array::LargeStringArray>()
            .unwrap()
            .value(index),
        DataType::Utf8View => array
            .as_any()
            .downcast_ref::<arrow_array::StringViewArray>()
            .unwrap()
            .value(index),
        _ => {
            return Err(DataFusionError::Internal(
                "ST_RelateMatch arguments must be strings".to_string(),
            )
            .into());
        }
    })
}

fn relate_match(matrix: &str, pattern: &str) -> GeoDataFusionResult<bool> {
    if pattern.len() != 9 {
        return Err(DataFusionError::Execution(format!(
            "ST_RelateMatch: Should be length 9, is [{pattern}] instead"
        ))
        .into());
    }

    let mut dimensions = [DIMENSION_FALSE; 9];
    for (index, symbol) in matrix.chars().take(9).enumerate() {
        dimensions[index] = dimension_value(symbol)?;
    }

    Ok(pattern
        .bytes()
        .enumerate()
        .all(|(index, symbol)| dimension_matches(dimensions[index], symbol)))
}

fn dimension_value(symbol: char) -> GeoDataFusionResult<i32> {
    match symbol.to_ascii_uppercase() {
        'F' => Ok(DIMENSION_FALSE),
        'T' => Ok(DIMENSION_TRUE),
        '*' => Ok(DIMENSION_DONT_CARE),
        '0' => Ok(DIMENSION_POINT),
        '1' => Ok(DIMENSION_LINE),
        '2' => Ok(DIMENSION_AREA),
        _ => Err(DataFusionError::Execution(format!(
            "ST_RelateMatch: Unknown dimension symbol: {symbol}"
        ))
        .into()),
    }
}

fn dimension_matches(actual: i32, pattern: u8) -> bool {
    match pattern {
        b'*' => true,
        b'T' => actual >= 0 || actual == DIMENSION_TRUE,
        b'F' => actual == DIMENSION_FALSE,
        b'0' => actual == DIMENSION_POINT,
        b'1' => actual == DIMENSION_LINE,
        b'2' => actual == DIMENSION_AREA,
        _ => false,
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct LineCrossingDirection {
    signature: Signature,
}

impl LineCrossingDirection {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(2, any_geometry_type(), Volatility::Immutable),
        }
    }
}

impl Default for LineCrossingDirection {
    fn default() -> Self {
        Self::new()
    }
}

static LINE_CROSSING_DIRECTION_DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for LineCrossingDirection {
    fn name(&self) -> &str {
        "st_linecrossingdirection"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(line_crossing_direction_impl(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(LINE_CROSSING_DIRECTION_DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns 0 for no crossing, -1/1 for a single crossing to the left or right, -2/2 for multiple crossings ending on different sides, and -3/3 for an even number of crossings ending on the same first side.",
                "ST_LineCrossingDirection(lineA, lineB)",
            )
            .with_argument("lineA", "LineString")
            .with_argument("lineB", "LineString")
            .build()
        }))
    }
}

fn line_crossing_direction_impl(args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if is_null_geometry_input(&args.arg_fields[0], arrays[0].as_ref())
        || is_null_geometry_input(&args.arg_fields[1], arrays[1].as_ref())
    {
        return Ok(ColumnarValue::Array(Arc::new(Int32Array::new_null(
            args.number_rows,
        ))));
    }

    merge_metadata(
        [args.arg_fields[0].as_ref(), args.arg_fields[1].as_ref()],
        "ST_LineCrossingDirection",
    )?;

    let left = geometry_array_from_arrow(arrays[0].as_ref(), args.arg_fields[0].as_ref())?;
    let right = geometry_array_from_arrow(arrays[1].as_ref(), args.arg_fields[1].as_ref())?;
    let left_ref = left.as_ref();
    let right_ref = right.as_ref();
    let left = downcast_geoarrow_array!(left_ref, line_coordinates)?;
    let right = downcast_geoarrow_array!(right_ref, line_coordinates)?;

    let mut builder = Int32Builder::with_capacity(args.number_rows);
    for index in 0..args.number_rows {
        match (&left[index], &right[index]) {
            (Some(left), Some(right)) => builder.append_value(line_crossing_direction(left, right)),
            _ => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

fn line_coordinates<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoDataFusionResult<Vec<Option<Vec<[f64; 2]>>>> {
    let mut values = Vec::with_capacity(array.len());
    for item in array.iter() {
        let geometry = match item {
            Some(geometry) => geometry?,
            None => {
                values.push(None);
                continue;
            }
        };

        let GeometryType::LineString(line_string) = geometry.as_type() else {
            return Err(DataFusionError::Execution(
                "This function only accepts LINESTRING as arguments.".to_string(),
            )
            .into());
        };
        values.push(Some(
            line_string
                .coords()
                .map(|coord| [coord.x(), coord.y()])
                .collect(),
        ));
    }
    Ok(values)
}

fn line_crossing_direction(left: &[[f64; 2]], right: &[[f64; 2]]) -> i32 {
    if left.len() < 2 || right.len() < 2 {
        return 0;
    }
    if is_degenerate_line(left) || is_degenerate_line(right) {
        return 0;
    }

    let mut left_crossings = 0;
    let mut right_crossings = 0;
    let mut first_crossing = None;
    for right_segment in right.windows(2) {
        for left_segment in left.windows(2) {
            let Some(side) = crossing_side(
                left_segment[0],
                left_segment[1],
                right_segment[0],
                right_segment[1],
            ) else {
                continue;
            };
            match side {
                CrossingSide::Left => left_crossings += 1,
                CrossingSide::Right => right_crossings += 1,
            }
            first_crossing.get_or_insert(side);
        }
    }

    if left_crossings == 0 && right_crossings == 0 {
        return 0;
    }
    if left_crossings == 0 && right_crossings == 1 {
        return 1;
    }
    if right_crossings == 0 && left_crossings == 1 {
        return -1;
    }
    if left_crossings - right_crossings == 1 {
        return -2;
    }
    if left_crossings - right_crossings == -1 {
        return 2;
    }
    if left_crossings == right_crossings && first_crossing == Some(CrossingSide::Left) {
        return -3;
    }
    if left_crossings == right_crossings && first_crossing == Some(CrossingSide::Right) {
        return 3;
    }
    0
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum CrossingSide {
    Left,
    Right,
}

fn is_degenerate_line(line: &[[f64; 2]]) -> bool {
    line.windows(2)
        .all(|pair| pair[0][0] == pair[1][0] && pair[0][1] == pair[1][1])
}

/// Classify the direction of the crossing contributed by one right segment
/// against one left segment.
///
/// Starting-point touches count as crossings, while ending-point touches do
/// not. This asymmetry is part of the observable PostGIS contract; the
/// orientation predicate itself is provided by `geo` rather than copied from
/// the PostGIS implementation.
fn crossing_side(
    left_start: [f64; 2],
    left_end: [f64; 2],
    right_start: [f64; 2],
    right_end: [f64; 2],
) -> Option<CrossingSide> {
    let right_start_side = orientation(left_start, left_end, right_start);
    let right_end_side = orientation(left_start, left_end, right_end);
    let left_start_side = orientation(right_start, right_end, left_start);
    let left_end_side = orientation(right_start, right_end, left_end);

    if strictly_same_side(right_start_side, right_end_side)
        || strictly_same_side(left_start_side, left_end_side)
    {
        return None;
    }
    if right_start_side == Orientation::Collinear
        && right_end_side == Orientation::Collinear
        && left_start_side == Orientation::Collinear
        && left_end_side == Orientation::Collinear
    {
        return None;
    }
    if right_end_side == Orientation::Collinear || left_end_side == Orientation::Collinear {
        return None;
    }
    if right_start_side == Orientation::Collinear {
        return Some(if right_end_side == Orientation::Clockwise {
            CrossingSide::Right
        } else {
            CrossingSide::Left
        });
    }
    if left_start_side == Orientation::Collinear {
        return Some(
            if right_start_side == Orientation::CounterClockwise
                && right_end_side == Orientation::Clockwise
            {
                CrossingSide::Right
            } else {
                CrossingSide::Left
            },
        );
    }
    if right_start_side != right_end_side && left_start_side != left_end_side {
        return Some(
            if right_start_side == Orientation::Clockwise
                && right_end_side == Orientation::CounterClockwise
            {
                CrossingSide::Left
            } else {
                CrossingSide::Right
            },
        );
    }
    None
}

fn orientation(start: [f64; 2], end: [f64; 2], point: [f64; 2]) -> Orientation {
    SimpleKernel::orient2d(
        geo::Coord {
            x: start[0],
            y: start[1],
        },
        geo::Coord {
            x: end[0],
            y: end[1],
        },
        geo::Coord {
            x: point[0],
            y: point[1],
        },
    )
}

fn strictly_same_side(first: Orientation, second: Orientation) -> bool {
    matches!(
        (first, second),
        (Orientation::CounterClockwise, Orientation::CounterClockwise)
            | (Orientation::Clockwise, Orientation::Clockwise)
    )
}

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(OrderingEquals::default().into());
    session_context.register_udf(RelateMatch::default().into());
    session_context.register_udf(LineCrossingDirection::default().into());
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;

    use super::*;

    fn context() -> SessionContext {
        let ctx = SessionContext::new();
        crate::udf::native::register(&ctx);
        ctx
    }

    #[tokio::test]
    async fn ordering_equals_structural_contract() {
        let ctx = context();
        let batch = ctx
            .sql(
                "SELECT
                    ST_OrderingEquals(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)')) AS same,
                    ST_OrderingEquals(ST_GeomFromText('LINESTRING(0 0,1 1)'), ST_GeomFromText('LINESTRING(1 1,0 0)')) AS reversed,
                    ST_OrderingEquals(ST_GeomFromText('POINT Z(1 2 3)'), ST_GeomFromText('POINT Z(1 2 4)')) AS z_diff,
                    ST_OrderingEquals(ST_GeomFromText('POINT Z(1 2 3)'), ST_GeomFromText('POINT ZM(1 2 3 0)')) AS dimension_diff,
                    ST_OrderingEquals(ST_GeomFromText('POINT(-0.0 0)'), ST_GeomFromText('POINT(0.0 0)')) AS signed_zero,
                    ST_OrderingEquals(ST_GeomFromText('POINT EMPTY'), ST_GeomFromText('POINT EMPTY')) AS empty,
                    ST_OrderingEquals(ST_PointFromText('POINT(1 2)', 4326), ST_PointFromText('POINT(1 2)', 3857)) AS srid_diff",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let values = batch.column(0).as_boolean();
        assert!(values.value(0));
        assert!(!batch.column(1).as_boolean().value(0));
        assert!(!batch.column(2).as_boolean().value(0));
        assert!(!batch.column(3).as_boolean().value(0));
        assert!(!batch.column(4).as_boolean().value(0));
        assert!(batch.column(5).as_boolean().value(0));
        assert!(!batch.column(6).as_boolean().value(0));
    }

    #[tokio::test]
    async fn ordering_equals_nulls() {
        let ctx = context();
        let batch = ctx
            .sql(
                "SELECT
                    ST_OrderingEquals(ST_GeomFromText(NULL), ST_GeomFromText('POINT(0 0)')),
                    ST_OrderingEquals(ST_GeomFromText('POINT(0 0)'), ST_GeomFromText(NULL))",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(1).is_null(0));
    }

    #[tokio::test]
    async fn relate_match_geos_contract() {
        let ctx = context();
        let batch = ctx
            .sql(
                "SELECT
                    ST_RelateMatch('101010101', 'T*T***T**') AS yes,
                    ST_RelateMatch('101010101', 'FF*FF****') AS no,
                    ST_RelateMatch('10101010', 'T*T***T*F') AS short_defaults_false,
                    ST_RelateMatch('tftftftft', 'T*T***T**') AS lower_matrix,
                    ST_RelateMatch(NULL, 'T*T***T**') AS null_matrix,
                    ST_RelateMatch('101010101', NULL) AS null_pattern",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let values = batch.column(0).as_boolean();
        assert!(values.value(0));
        assert!(!batch.column(1).as_boolean().value(0));
        assert!(batch.column(2).as_boolean().value(0));
        assert!(batch.column(3).as_boolean().value(0));
        assert!(batch.column(4).is_null(0));
        assert!(batch.column(5).is_null(0));
    }

    #[tokio::test]
    async fn relate_match_invalid_inputs_fail() {
        let ctx = context();
        let short_pattern = ctx
            .sql("SELECT ST_RelateMatch('101010101', 'T*T***T*')")
            .await
            .unwrap();
        let error = short_pattern.collect().await.unwrap_err().to_string();
        assert!(error.contains("length 9"), "{error}");

        let invalid_matrix = ctx
            .sql("SELECT ST_RelateMatch('10101010X', 'T*T***T**')")
            .await;
        match invalid_matrix {
            Ok(dataframe) => {
                let error = dataframe.collect().await.unwrap_err().to_string();
                assert!(error.contains("Unknown dimension symbol: X"), "{error}");
            }
            Err(error) => assert!(error.to_string().contains("Unknown dimension symbol: X")),
        }
    }
}
