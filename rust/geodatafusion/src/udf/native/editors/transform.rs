//! Dimension-aware coordinate editing.
//!
//! Affine transformations use a geometry view that preserves the input dimensions.
//! Editors also change the coordinate dimension itself, so this module uses the existing `wkt`
//! geometry model as an intermediate and pushes results through the shared GeoArrow builders.
//! It does not define another `GeometryTrait` proxy.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs};
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::builder::{
    GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder, RectBuilder,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::{BoxType, Dimension, GeoArrowType, GeometryType};
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension as WktDimension, GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon,
};

use super::coordinate;
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::{StrictGeometryBuilder, geometry_array_from_arrow};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum Ordinate {
    X,
    Y,
    Z,
    M,
}

impl Ordinate {
    fn name(self) -> &'static str {
        match self {
            Self::X => "X",
            Self::Y => "Y",
            Self::Z => "Z",
            Self::M => "M",
        }
    }

    fn article(self) -> &'static str {
        match self {
            Self::M => "an",
            Self::X | Self::Y | Self::Z => "a",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::X => 0,
            Self::Y => 1,
            Self::Z => 2,
            Self::M => 3,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CoordinateOperation {
    Flip,
    Force {
        target: WktDimension,
        z_default: Option<f64>,
        m_default: Option<f64>,
    },
    Swap {
        first: Ordinate,
        second: Ordinate,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum GeometryEdit {
    Coordinate(CoordinateOperation),
    Reverse,
    AddPoint {
        point: Wkt<f64>,
        position: Option<i64>,
    },
    RemovePoint {
        index: i64,
    },
    SetPoint {
        index: i64,
        point: Wkt<f64>,
    },
    Scroll {
        point: Wkt<f64>,
    },
    LineExtend {
        forward: f64,
        backward: f64,
    },
    Quantize {
        precision: [i32; 4],
    },
    RemoveRepeatedPoints {
        tolerance: f64,
    },
    Segmentize {
        max_length: f64,
    },
    ShiftLongitude,
    WrapX {
        cut: f64,
        amount: f64,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum OutputKind {
    Preserve,
    Force(Dimension),
    LineString,
    Geometry,
}

impl CoordinateOperation {
    fn target_dimension(self, input: Dimensions) -> GeoArrowResult<WktDimension> {
        match self {
            Self::Flip | Self::Swap { .. } => wkt_dimension(input),
            Self::Force { target, .. } => Ok(target),
        }
    }

    fn validate_dimension(self, input: Dimensions) -> GeoArrowResult<()> {
        let Self::Swap { first, second } = self else {
            return Ok(());
        };

        for ordinate in [first, second] {
            if !has_ordinate(input, ordinate) {
                return Err(GeoArrowError::InvalidGeoArrow(format!(
                    "Geometry does not have {} {} ordinate",
                    ordinate.article(),
                    ordinate.name()
                )));
            }
        }

        Ok(())
    }

    fn edit_coord(self, coord: &impl CoordTrait<T = f64>) -> GeoArrowResult<Coord<f64>> {
        let coord = geo_coord_to_wkt(coord)?;
        match self {
            Self::Flip => Ok(Coord {
                x: coord.y,
                y: coord.x,
                z: coord.z,
                m: coord.m,
            }),
            Self::Force {
                target,
                z_default,
                m_default,
            } => {
                let z = matches!(target, WktDimension::XYZ | WktDimension::XYZM)
                    .then(|| coord.z.unwrap_or(z_default.unwrap_or(0.0)));
                let m = matches!(target, WktDimension::XYM | WktDimension::XYZM)
                    .then(|| coord.m.unwrap_or(m_default.unwrap_or(0.0)));
                Ok(Coord {
                    x: coord.x,
                    y: coord.y,
                    z,
                    m,
                })
            }
            Self::Swap { first, second } => {
                let mut values = [Some(coord.x), Some(coord.y), coord.z, coord.m];
                values.swap(first.index(), second.index());
                Ok(Coord {
                    x: values[0].expect("X is always present"),
                    y: values[1].expect("Y is always present"),
                    z: values[2],
                    m: values[3],
                })
            }
        }
    }
}

pub(crate) fn parse_ordinate_spec(ords: &str) -> GeoDataFusionResult<CoordinateOperation> {
    if ords.len() != 2 {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "Invalid ordinate specification. Need two letters from the set (x,y,z,m). Got '{ords}'"
        ))
        .into());
    }

    let first = parse_ordinate(ords.as_bytes()[0])?;
    let second = parse_ordinate(ords.as_bytes()[1])?;
    Ok(CoordinateOperation::Swap { first, second })
}

fn parse_ordinate(value: u8) -> GeoDataFusionResult<Ordinate> {
    match value.to_ascii_lowercase() {
        b'x' => Ok(Ordinate::X),
        b'y' => Ok(Ordinate::Y),
        b'z' => Ok(Ordinate::Z),
        b'm' => Ok(Ordinate::M),
        invalid => Err(GeoArrowError::InvalidGeoArrow(format!(
            "Invalid ordinate name '{}'. Expected x,y,z or m",
            char::from(invalid)
        ))
        .into()),
    }
}

pub(crate) fn return_field(
    args: ReturnFieldArgs,
    target_dimension: Option<Dimension>,
) -> GeoDataFusionResult<FieldRef> {
    return_field_for(
        args,
        target_dimension
            .map(OutputKind::Force)
            .unwrap_or(OutputKind::Preserve),
    )
}

pub(crate) fn return_field_for(
    args: ReturnFieldArgs,
    output_kind: OutputKind,
) -> GeoDataFusionResult<FieldRef> {
    let input_field = args.arg_fields[0].as_ref();
    let input_type = if input_field.data_type() == &DataType::Null {
        GeoArrowType::Geometry(GeometryType::new(Default::default()))
    } else {
        GeoArrowType::from_arrow_field(input_field)?
    };
    let output_type = output_type(&input_type, output_kind)?;
    let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
    Ok(Arc::new(output_type.to_field(input_field.name(), nullable)))
}

pub(crate) fn output_kind_for_input(
    field: &arrow_schema::Field,
) -> GeoDataFusionResult<OutputKind> {
    if field.data_type() == &DataType::Null {
        return Ok(OutputKind::Preserve);
    }

    Ok(match GeoArrowType::from_arrow_field(field)? {
        GeoArrowType::Rect(_) => OutputKind::Geometry,
        _ => OutputKind::Preserve,
    })
}

pub(crate) fn invoke(
    args: ScalarFunctionArgs,
    target_dimension: Option<Dimension>,
    operation_for_row: impl Fn(usize, &[ArrayRef]) -> GeoDataFusionResult<Option<CoordinateOperation>>,
) -> GeoDataFusionResult<ColumnarValue> {
    invoke_with_edits(
        args,
        target_dimension
            .map(OutputKind::Force)
            .unwrap_or(OutputKind::Preserve),
        |index, arrays| Ok(operation_for_row(index, arrays)?.map(GeometryEdit::Coordinate)),
    )
}

pub(crate) fn invoke_with_edits(
    args: ScalarFunctionArgs,
    output_kind: OutputKind,
    edit_for_row: impl Fn(usize, &[ArrayRef]) -> GeoDataFusionResult<Option<GeometryEdit>>,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;

    if arrays[0].data_type() == &DataType::Null {
        let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
        return Ok(ColumnarValue::Array(null_array(
            &output_type,
            arrays[0].len(),
        )?));
    }

    let geometry = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let output_type = output_type(&geometry.data_type(), output_kind)?;
    let edits = (0..arrays[0].len())
        .map(|index| edit_for_row(index, &arrays))
        .collect::<GeoDataFusionResult<Vec<_>>>()?;
    let edited = edit_array(geometry.as_ref(), &output_type, &edits)?;
    Ok(ColumnarValue::Array(edited))
}

pub(crate) fn invoke_with_wkt_edits<F>(
    args: ScalarFunctionArgs,
    output_kind: OutputKind,
    edit_for_row: F,
) -> GeoDataFusionResult<ColumnarValue>
where
    F: Fn(usize, &[ArrayRef], &Wkt<f64>) -> GeoDataFusionResult<Option<Wkt<f64>>>,
{
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;

    if arrays[0].data_type() == &DataType::Null {
        let output_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
        return Ok(ColumnarValue::Array(null_array(
            &output_type,
            arrays[0].len(),
        )?));
    }

    let geometry = geometry_array_from_arrow(&arrays[0], &args.arg_fields[0])?;
    let output_type = output_type(&geometry.data_type(), output_kind)?;
    let edited = wkt_edit_array(geometry.as_ref(), &output_type, &arrays, &edit_for_row)?;
    Ok(ColumnarValue::Array(edited))
}

pub(crate) type WktValue = Result<Wkt<f64>, String>;

pub(crate) fn wkt_values(
    array: &ArrayRef,
    field: &FieldRef,
) -> GeoDataFusionResult<Vec<Option<WktValue>>> {
    let geometry = geometry_array_from_arrow(array, field)?;
    let geometry_ref = geometry.as_ref();
    Ok(downcast_geoarrow_array!(geometry_ref, collect_wkt_values)?)
}

fn collect_wkt_values<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
) -> GeoArrowResult<Vec<Option<WktValue>>> {
    Ok(array
        .iter()
        .map(|value| match value {
            None => None,
            Some(Ok(geometry)) => {
                Some(geometry_to_wkt(&geometry).map_err(|error| error.to_string()))
            }
            Some(Err(error)) => Some(Err(error.to_string())),
        })
        .collect())
}

pub(crate) fn integer_value(array: &ArrayRef, index: usize) -> GeoDataFusionResult<Option<i64>> {
    match array.data_type() {
        DataType::Int32 => {
            let values = array.as_primitive::<Int32Type>();
            if values.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(i64::from(values.value(index))))
            }
        }
        DataType::Int64 => {
            let values = array.as_primitive::<Int64Type>();
            if values.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(values.value(index)))
            }
        }
        DataType::Null => Ok(None),
        data_type => Err(DataFusionError::Execution(format!(
            "expected an integer index, got {data_type}"
        ))
        .into()),
    }
}

fn output_type(input_type: &GeoArrowType, output_kind: OutputKind) -> GeoArrowResult<GeoArrowType> {
    if output_kind == OutputKind::Geometry {
        return Ok(GeoArrowType::Geometry(
            GeometryType::new(input_type.metadata().clone())
                .with_coord_type(input_type.coord_type().unwrap_or_default()),
        ));
    }

    if output_kind == OutputKind::LineString {
        return Ok(match input_type {
            GeoArrowType::LineString(typ) => GeoArrowType::LineString(typ.clone()),
            _ => {
                let metadata = input_type.metadata().clone();
                let coord_type = input_type.coord_type().unwrap_or_default();
                GeoArrowType::Geometry(GeometryType::new(metadata).with_coord_type(coord_type))
            }
        });
    }

    let target_dimension = match output_kind {
        OutputKind::Preserve => None,
        OutputKind::Force(dimension) => Some(dimension),
        OutputKind::LineString | OutputKind::Geometry => unreachable!(),
    };
    let with_dimension = |dimension| target_dimension.unwrap_or(dimension);

    Ok(match input_type {
        GeoArrowType::Point(typ) => {
            GeoArrowType::Point(typ.clone().with_dimension(with_dimension(typ.dimension())))
        }
        GeoArrowType::LineString(typ) => {
            GeoArrowType::LineString(typ.clone().with_dimension(with_dimension(typ.dimension())))
        }
        GeoArrowType::Polygon(typ) => {
            GeoArrowType::Polygon(typ.clone().with_dimension(with_dimension(typ.dimension())))
        }
        GeoArrowType::MultiPoint(typ) => {
            GeoArrowType::MultiPoint(typ.clone().with_dimension(with_dimension(typ.dimension())))
        }
        GeoArrowType::MultiLineString(typ) => GeoArrowType::MultiLineString(
            typ.clone().with_dimension(with_dimension(typ.dimension())),
        ),
        GeoArrowType::MultiPolygon(typ) => {
            GeoArrowType::MultiPolygon(typ.clone().with_dimension(with_dimension(typ.dimension())))
        }
        GeoArrowType::GeometryCollection(typ) => GeoArrowType::GeometryCollection(
            typ.clone().with_dimension(with_dimension(typ.dimension())),
        ),
        GeoArrowType::Rect(typ) => {
            GeoArrowType::Rect(typ.clone().with_dimension(with_dimension(typ.dimension())))
        }
        GeoArrowType::Geometry(typ) => GeoArrowType::Geometry(
            GeometryType::new(typ.metadata().clone()).with_coord_type(typ.coord_type()),
        ),
        GeoArrowType::Wkb(typ) => GeoArrowType::Geometry(GeometryType::new(typ.metadata().clone())),
        GeoArrowType::LargeWkb(typ) => {
            GeoArrowType::Geometry(GeometryType::new(typ.metadata().clone()))
        }
        GeoArrowType::WkbView(typ) => {
            GeoArrowType::Geometry(GeometryType::new(typ.metadata().clone()))
        }
        GeoArrowType::Wkt(typ) => GeoArrowType::Geometry(GeometryType::new(typ.metadata().clone())),
        GeoArrowType::LargeWkt(typ) => {
            GeoArrowType::Geometry(GeometryType::new(typ.metadata().clone()))
        }
        GeoArrowType::WktView(typ) => {
            GeoArrowType::Geometry(GeometryType::new(typ.metadata().clone()))
        }
    })
}

fn null_array(output_type: &GeoArrowType, length: usize) -> GeoArrowResult<ArrayRef> {
    macro_rules! build_nulls {
        ($builder:expr) => {{
            let mut builder = $builder;
            for _ in 0..length {
                builder.push_geometry(None::<&Wkt<f64>>)?;
            }
            Ok(builder.finish().to_array_ref())
        }};
    }

    match output_type {
        GeoArrowType::Point(typ) => build_nulls!(PointBuilder::new(typ.clone())),
        GeoArrowType::LineString(typ) => build_nulls!(LineStringBuilder::new(typ.clone())),
        GeoArrowType::Polygon(typ) => build_nulls!(PolygonBuilder::new(typ.clone())),
        GeoArrowType::MultiPoint(typ) => build_nulls!(MultiPointBuilder::new(typ.clone())),
        GeoArrowType::MultiLineString(typ) => {
            build_nulls!(MultiLineStringBuilder::new(typ.clone()))
        }
        GeoArrowType::MultiPolygon(typ) => {
            build_nulls!(MultiPolygonBuilder::new(typ.clone()))
        }
        GeoArrowType::GeometryCollection(typ) => {
            build_nulls!(GeometryCollectionBuilder::new(typ.clone()))
        }
        GeoArrowType::Geometry(typ) => {
            build_nulls!(StrictGeometryBuilder::new(typ.clone()))
        }
        GeoArrowType::Rect(typ) => {
            let mut builder = RectBuilder::with_capacity(typ.clone(), length);
            for _ in 0..length {
                builder.push_null();
            }
            Ok(builder.finish().to_array_ref())
        }
        _ => Err(GeoArrowError::InvalidGeoArrow(format!(
            "Unsupported null editor output type: {output_type:?}"
        ))),
    }
}

fn edit_array(
    array: &dyn GeoArrowArray,
    output_type: &GeoArrowType,
    edits: &[Option<GeometryEdit>],
) -> GeoArrowResult<ArrayRef> {
    downcast_geoarrow_array!(array, edit_array_impl, output_type, edits)
}

fn edit_array_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeoArrowType,
    edits: &[Option<GeometryEdit>],
) -> GeoArrowResult<ArrayRef> {
    if edits.len() != array.len() {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "Expected {} geometry edits, found {}",
            array.len(),
            edits.len()
        )));
    }

    match output_type {
        GeoArrowType::Point(typ) => edit_with_builder(array, PointBuilder::new(typ.clone()), edits),
        GeoArrowType::LineString(typ) => {
            edit_with_builder(array, LineStringBuilder::new(typ.clone()), edits)
        }
        GeoArrowType::Polygon(typ) => {
            edit_with_builder(array, PolygonBuilder::new(typ.clone()), edits)
        }
        GeoArrowType::MultiPoint(typ) => {
            edit_with_builder(array, MultiPointBuilder::new(typ.clone()), edits)
        }
        GeoArrowType::MultiLineString(typ) => {
            edit_with_builder(array, MultiLineStringBuilder::new(typ.clone()), edits)
        }
        GeoArrowType::MultiPolygon(typ) => {
            edit_with_builder(array, MultiPolygonBuilder::new(typ.clone()), edits)
        }
        GeoArrowType::GeometryCollection(typ) => {
            edit_with_builder(array, GeometryCollectionBuilder::new(typ.clone()), edits)
        }
        GeoArrowType::Geometry(typ) => {
            edit_with_builder(array, StrictGeometryBuilder::new(typ.clone()), edits)
        }
        GeoArrowType::Rect(typ) => edit_rects(array, typ.clone(), edits),
        _ => Err(GeoArrowError::InvalidGeoArrow(format!(
            "Unsupported editor output type: {output_type:?}"
        ))),
    }
}

fn wkt_edit_array<F>(
    array: &dyn GeoArrowArray,
    output_type: &GeoArrowType,
    arrays: &[ArrayRef],
    edit_for_row: &F,
) -> GeoDataFusionResult<ArrayRef>
where
    F: Fn(usize, &[ArrayRef], &Wkt<f64>) -> GeoDataFusionResult<Option<Wkt<f64>>>,
{
    downcast_geoarrow_array!(
        array,
        wkt_edit_array_impl,
        output_type,
        arrays,
        edit_for_row
    )
}

fn wkt_edit_array_impl<'a, F>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeoArrowType,
    arrays: &[ArrayRef],
    edit_for_row: &F,
) -> GeoDataFusionResult<ArrayRef>
where
    F: Fn(usize, &[ArrayRef], &Wkt<f64>) -> GeoDataFusionResult<Option<Wkt<f64>>>,
{
    match output_type {
        GeoArrowType::Point(typ) => {
            wkt_edit_with_builder(array, PointBuilder::new(typ.clone()), arrays, edit_for_row)
        }
        GeoArrowType::LineString(typ) => wkt_edit_with_builder(
            array,
            LineStringBuilder::new(typ.clone()),
            arrays,
            edit_for_row,
        ),
        GeoArrowType::Polygon(typ) => wkt_edit_with_builder(
            array,
            PolygonBuilder::new(typ.clone()),
            arrays,
            edit_for_row,
        ),
        GeoArrowType::MultiPoint(typ) => wkt_edit_with_builder(
            array,
            MultiPointBuilder::new(typ.clone()),
            arrays,
            edit_for_row,
        ),
        GeoArrowType::MultiLineString(typ) => wkt_edit_with_builder(
            array,
            MultiLineStringBuilder::new(typ.clone()),
            arrays,
            edit_for_row,
        ),
        GeoArrowType::MultiPolygon(typ) => wkt_edit_with_builder(
            array,
            MultiPolygonBuilder::new(typ.clone()),
            arrays,
            edit_for_row,
        ),
        GeoArrowType::GeometryCollection(typ) => wkt_edit_with_builder(
            array,
            GeometryCollectionBuilder::new(typ.clone()),
            arrays,
            edit_for_row,
        ),
        GeoArrowType::Geometry(typ) => wkt_edit_with_builder(
            array,
            StrictGeometryBuilder::new(typ.clone()),
            arrays,
            edit_for_row,
        ),
        GeoArrowType::Rect(_) => Err(DataFusionError::Internal(
            "WKT editors require Box inputs to use a generic Geometry output".to_string(),
        )
        .into()),
        _ => Err(GeoArrowError::InvalidGeoArrow(format!(
            "Unsupported WKT editor output type: {output_type:?}"
        ))
        .into()),
    }
}

fn wkt_edit_with_builder<'a, F>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    mut builder: impl GeometryOutputBuilder,
    arrays: &[ArrayRef],
    edit_for_row: &F,
) -> GeoDataFusionResult<ArrayRef>
where
    F: Fn(usize, &[ArrayRef], &Wkt<f64>) -> GeoDataFusionResult<Option<Wkt<f64>>>,
{
    for (index, geometry) in array.iter().enumerate() {
        let geometry = geometry.transpose()?;
        let Some(geometry) = geometry else {
            builder.push_geometry(None)?;
            continue;
        };

        let geometry = geometry_to_wkt_for_wkt_edits(&geometry)?;
        match edit_for_row(index, arrays, &geometry)? {
            Some(edited) => builder.push_geometry(Some(&edited))?,
            None => builder.push_geometry(None)?,
        }
    }

    Ok(builder.finish().to_array_ref())
}

trait GeometryOutputBuilder {
    fn push_geometry(&mut self, geometry: Option<&Wkt<f64>>) -> GeoArrowResult<()>;
    fn finish(self) -> Arc<dyn GeoArrowArray>;
}

macro_rules! impl_output_builder {
    ($builder:ty) => {
        impl GeometryOutputBuilder for $builder {
            fn push_geometry(&mut self, geometry: Option<&Wkt<f64>>) -> GeoArrowResult<()> {
                <$builder>::push_geometry(self, geometry)
            }

            fn finish(self) -> Arc<dyn GeoArrowArray> {
                Arc::new(<$builder>::finish(self))
            }
        }
    };
}

impl_output_builder!(PointBuilder);
impl_output_builder!(LineStringBuilder);
impl_output_builder!(PolygonBuilder);
impl_output_builder!(MultiPointBuilder);
impl_output_builder!(MultiLineStringBuilder);
impl_output_builder!(MultiPolygonBuilder);
impl_output_builder!(GeometryCollectionBuilder);
impl_output_builder!(StrictGeometryBuilder);

fn edit_with_builder<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    mut builder: impl GeometryOutputBuilder,
    edits: &[Option<GeometryEdit>],
) -> GeoArrowResult<ArrayRef> {
    for (index, geometry) in array.iter().enumerate() {
        let geometry = geometry.transpose()?;
        let edit = edits[index].as_ref();

        if let (Some(geometry), Some(edit)) = (geometry, edit) {
            let geometry = geometry_to_wkt(&geometry)?;
            match edit_geometry(geometry, edit)? {
                Some(geometry) => builder.push_geometry(Some(&geometry))?,
                None => builder.push_geometry(None)?,
            }
        } else {
            builder.push_geometry(None)?;
        }
    }

    Ok(builder.finish().to_array_ref())
}

fn edit_rects<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    typ: BoxType,
    edits: &[Option<GeometryEdit>],
) -> GeoArrowResult<ArrayRef> {
    let mut builder = RectBuilder::with_capacity(typ, array.len());

    for (index, geometry) in array.iter().enumerate() {
        let geometry = geometry.transpose()?;
        let edit = edits[index].as_ref();

        match (geometry, edit) {
            (Some(geometry), Some(edit)) => match geometry.as_type() {
                geo_traits::GeometryType::Rect(rect) => match edit {
                    GeometryEdit::Coordinate(operation) => {
                        operation.validate_dimension(rect.dim())?;
                        let lower = operation.edit_coord(&rect.min())?;
                        let upper = operation.edit_coord(&rect.max())?;
                        builder.push_min_max(&lower, &upper);
                    }
                    GeometryEdit::Reverse => {
                        builder.push_min_max(&rect.min(), &rect.max());
                    }
                    GeometryEdit::Quantize { precision } => {
                        let lower =
                            coordinate::quantize_coord(&rect.min(), rect.dim(), *precision)?;
                        let upper =
                            coordinate::quantize_coord(&rect.max(), rect.dim(), *precision)?;
                        builder.push_min_max(&lower, &upper);
                    }
                    GeometryEdit::RemoveRepeatedPoints { .. } => {
                        builder.push_min_max(&rect.min(), &rect.max());
                    }
                    GeometryEdit::ShiftLongitude => {
                        let lower = coordinate::shift_longitude_coord(&rect.min(), rect.dim())?;
                        let upper = coordinate::shift_longitude_coord(&rect.max(), rect.dim())?;
                        builder.push_min_max(&lower, &upper);
                    }
                    GeometryEdit::Segmentize { .. } => {
                        return Err(GeoArrowError::InvalidGeoArrow(
                            "ST_Segmentize does not support BOX geometry".to_string(),
                        ));
                    }
                    _ => {
                        return Err(GeoArrowError::InvalidGeoArrow(
                            first_argument_error(edit).to_string(),
                        ));
                    }
                },
                _ => {
                    return Err(GeoArrowError::InvalidGeoArrow(
                        "Expected a Box geometry".to_string(),
                    ));
                }
            },
            _ => builder.push_null(),
        }
    }

    Ok(builder.finish().to_array_ref())
}

fn edit_geometry(geometry: Wkt<f64>, edit: &GeometryEdit) -> GeoArrowResult<Option<Wkt<f64>>> {
    Ok(match edit {
        GeometryEdit::Coordinate(operation) => Some(edit_wkt_coordinates(geometry, *operation)?),
        GeometryEdit::Reverse => Some(reverse_wkt(geometry)),
        GeometryEdit::AddPoint { point, position } => {
            Some(edit_add_point(geometry, point, *position)?)
        }
        GeometryEdit::RemovePoint { index } => Some(edit_remove_point(geometry, *index)?),
        GeometryEdit::SetPoint { index, point } => Some(edit_set_point(geometry, *index, point)?),
        GeometryEdit::Scroll { point } => Some(edit_scroll(geometry, point)?),
        GeometryEdit::LineExtend { forward, backward } => {
            coordinate::edit_line_extend(geometry, *forward, *backward)?
        }
        GeometryEdit::Quantize { precision } => {
            Some(coordinate::edit_quantize(geometry, *precision))
        }
        GeometryEdit::RemoveRepeatedPoints { tolerance } => Some(
            coordinate::edit_remove_repeated_points(geometry, *tolerance),
        ),
        GeometryEdit::Segmentize { max_length } => {
            coordinate::edit_segmentize(geometry, *max_length)?
        }
        GeometryEdit::ShiftLongitude => Some(coordinate::edit_shift_longitude(geometry)),
        GeometryEdit::WrapX { cut, amount } => {
            Some(coordinate::edit_wrap_x(geometry, *cut, *amount)?)
        }
    })
}

fn edit_wkt_coordinates(
    geometry: Wkt<f64>,
    operation: CoordinateOperation,
) -> GeoArrowResult<Wkt<f64>> {
    let input_dimension = geometry.dim();
    operation.validate_dimension(input_dimension)?;
    let target_dimension = operation.target_dimension(input_dimension)?;

    Ok(match geometry {
        Wkt::Point(point) => {
            let (coord, _) = point.into_inner();
            let coord = coord
                .map(|coord| operation.edit_coord(&coord))
                .transpose()?;
            Wkt::Point(Point::new(coord, target_dimension))
        }
        Wkt::LineString(line_string) => {
            let (coords, _) = line_string.into_inner();
            Wkt::LineString(edit_line_string(coords, target_dimension, operation)?)
        }
        Wkt::Polygon(polygon) => {
            let (rings, _) = polygon.into_inner();
            let rings = rings
                .into_iter()
                .map(|ring| {
                    let (coords, _) = ring.into_inner();
                    edit_line_string(coords, target_dimension, operation)
                })
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::Polygon(Polygon::new(rings, target_dimension))
        }
        Wkt::MultiPoint(multi_point) => {
            let (points, _) = multi_point.into_inner();
            let points = points
                .into_iter()
                .map(|point| {
                    let (coord, _) = point.into_inner();
                    let coord = coord
                        .map(|coord| operation.edit_coord(&coord))
                        .transpose()?;
                    Ok(Point::new(coord, target_dimension))
                })
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::MultiPoint(MultiPoint::new(points, target_dimension))
        }
        Wkt::MultiLineString(multi_line_string) => {
            let (line_strings, _) = multi_line_string.into_inner();
            let line_strings = line_strings
                .into_iter()
                .map(|line_string| {
                    let (coords, _) = line_string.into_inner();
                    edit_line_string(coords, target_dimension, operation)
                })
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::MultiLineString(MultiLineString::new(line_strings, target_dimension))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, _) = multi_polygon.into_inner();
            let polygons = polygons
                .into_iter()
                .map(|polygon| {
                    let (rings, _) = polygon.into_inner();
                    let rings = rings
                        .into_iter()
                        .map(|ring| {
                            let (coords, _) = ring.into_inner();
                            edit_line_string(coords, target_dimension, operation)
                        })
                        .collect::<GeoArrowResult<Vec<_>>>()?;
                    Ok(Polygon::new(rings, target_dimension))
                })
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::MultiPolygon(MultiPolygon::new(polygons, target_dimension))
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, _) = collection.into_inner();
            let geometries = geometries
                .into_iter()
                .map(|geometry| edit_wkt_coordinates(geometry, operation))
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::GeometryCollection(GeometryCollection::new(geometries, target_dimension))
        }
    })
}

fn first_argument_error(edit: &GeometryEdit) -> &'static str {
    match edit {
        GeometryEdit::Scroll { .. } => "First argument must be a line",
        _ => "First argument must be a LINESTRING",
    }
}

fn edit_add_point(
    geometry: Wkt<f64>,
    point: &Wkt<f64>,
    position: Option<i64>,
) -> GeoArrowResult<Wkt<f64>> {
    let Wkt::LineString(line) = geometry else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "First argument must be a LINESTRING".to_string(),
        ));
    };
    let Wkt::Point(point) = point else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Second argument must be a POINT".to_string(),
        ));
    };

    let (mut coords, dimension) = line.into_inner();
    let Some(point_coord) = point.coord() else {
        return Ok(Wkt::LineString(LineString::new(coords, dimension)));
    };

    let point_coord = coord_for_dimension(point_coord, dimension);
    let position = match position {
        None | Some(-1) => coords.len(),
        Some(position) if position < 0 || position > coords.len() as i64 => {
            return Err(GeoArrowError::InvalidGeoArrow(
                "LWGEOM_addpoint: Invalid offset".to_string(),
            ));
        }
        Some(position) => position as usize,
    };
    coords.insert(position, point_coord);
    Ok(Wkt::LineString(LineString::new(coords, dimension)))
}

fn edit_remove_point(geometry: Wkt<f64>, index: i64) -> GeoArrowResult<Wkt<f64>> {
    let Wkt::LineString(line) = geometry else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "First argument must be a LINESTRING".to_string(),
        ));
    };

    let (mut coords, dimension) = line.into_inner();
    let length = coords.len() as i64;
    if index < 0 || (length > 0 && index >= length) {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "Point index out of range (0..{})",
            point_index_upper_bound(coords.len())
        )));
    }
    if coords.len() < 3 {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Can't remove points from a single segment line".to_string(),
        ));
    }

    coords.remove(index as usize);
    Ok(Wkt::LineString(LineString::new(coords, dimension)))
}

fn edit_set_point(geometry: Wkt<f64>, index: i64, point: &Wkt<f64>) -> GeoArrowResult<Wkt<f64>> {
    let Wkt::Point(point) = point else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Third argument must be a POINT".to_string(),
        ));
    };
    let Some(point_coord) = point.coord() else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Third argument must be a non-empty point".to_string(),
        ));
    };

    let Wkt::LineString(line) = geometry else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "First argument must be a LINESTRING".to_string(),
        ));
    };
    let (mut coords, dimension) = line.into_inner();
    if coords.is_empty() {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Line has no points".to_string(),
        ));
    }

    let length = coords.len() as i64;
    let index = if index < 0 { index + length } else { index };
    if index < 0 || index >= length {
        return Err(GeoArrowError::InvalidGeoArrow(format!(
            "abs(Point index) out of range (-)(0..{})",
            length - 1
        )));
    }

    coords[index as usize] = coord_for_dimension(point_coord, dimension);
    Ok(Wkt::LineString(LineString::new(coords, dimension)))
}

fn edit_scroll(geometry: Wkt<f64>, point: &Wkt<f64>) -> GeoArrowResult<Wkt<f64>> {
    let Wkt::LineString(line) = geometry else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "First argument must be a line".to_string(),
        ));
    };
    let Wkt::Point(point) = point else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Second argument must be a point".to_string(),
        ));
    };
    let Some(point_coord) = point.coord() else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "Second argument must be a non-empty point".to_string(),
        ));
    };

    let (coords, dimension) = line.into_inner();
    let Some(first) = coords.first() else {
        return Err(scroll_not_closed_error());
    };
    let last = coords.last().expect("non-empty line has a last coordinate");
    if first.x.to_bits() != last.x.to_bits() || first.y.to_bits() != last.y.to_bits() {
        return Err(scroll_not_closed_error());
    }

    let point_slots = point_coord_slots(point_coord);
    let slot_count = dimension_size(dimension);
    let Some(index) = coords.iter().position(|coord| {
        line_coord_slots(coord, dimension)
            .into_iter()
            .zip(point_slots)
            .take(slot_count)
            .all(|(left, right)| left.to_bits() == right.to_bits())
    }) else {
        return Err(GeoArrowError::InvalidGeoArrow(
            "ptarray_scroll_in_place: input POINTARRAY does not contain the given point"
                .to_string(),
        ));
    };

    if index == 0 {
        return Ok(Wkt::LineString(LineString::new(coords, dimension)));
    }

    let mut rotated = Vec::with_capacity(coords.len());
    rotated.extend_from_slice(&coords[index..]);
    rotated.extend_from_slice(&coords[1..=index]);
    Ok(Wkt::LineString(LineString::new(rotated, dimension)))
}

fn scroll_not_closed_error() -> GeoArrowError {
    GeoArrowError::InvalidGeoArrow(
        "ptarray_scroll_in_place: input POINTARRAY is not closed".to_string(),
    )
}

fn point_index_upper_bound(length: usize) -> u32 {
    if length == 0 {
        u32::MAX
    } else {
        (length - 1) as u32
    }
}

fn dimension_size(dimension: WktDimension) -> usize {
    match dimension {
        WktDimension::XY => 2,
        WktDimension::XYZ | WktDimension::XYM => 3,
        WktDimension::XYZM => 4,
    }
}

fn coord_for_dimension(coord: &Coord<f64>, dimension: WktDimension) -> Coord<f64> {
    Coord {
        x: coord.x,
        y: coord.y,
        z: matches!(dimension, WktDimension::XYZ | WktDimension::XYZM)
            .then(|| coord.z.unwrap_or(0.0)),
        m: matches!(dimension, WktDimension::XYM | WktDimension::XYZM)
            .then(|| coord.m.unwrap_or(0.0)),
    }
}

fn line_coord_slots(coord: &Coord<f64>, dimension: WktDimension) -> [f64; 4] {
    match dimension {
        WktDimension::XY => [coord.x, coord.y, 0.0, 0.0],
        WktDimension::XYZ => [coord.x, coord.y, coord.z.unwrap_or(0.0), 0.0],
        WktDimension::XYM => [coord.x, coord.y, coord.m.unwrap_or(0.0), 0.0],
        WktDimension::XYZM => [
            coord.x,
            coord.y,
            coord.z.unwrap_or(0.0),
            coord.m.unwrap_or(0.0),
        ],
    }
}

fn point_coord_slots(coord: &Coord<f64>) -> [f64; 4] {
    // ST_Scroll compares raw postgis buffers: M points keep the measure in the
    // third slot, while missing Z and M ordinates are zero-filled.
    [
        coord.x,
        coord.y,
        coord.z.unwrap_or(0.0),
        coord.m.unwrap_or(0.0),
    ]
}

fn reverse_wkt(geometry: Wkt<f64>) -> Wkt<f64> {
    match geometry {
        Wkt::Point(point) => Wkt::Point(point),
        Wkt::MultiPoint(multi_point) => Wkt::MultiPoint(multi_point),
        Wkt::LineString(line_string) => Wkt::LineString(reverse_line_string(line_string)),
        Wkt::Polygon(polygon) => Wkt::Polygon(reverse_polygon(polygon)),
        Wkt::MultiLineString(multi_line_string) => {
            let (line_strings, dimension) = multi_line_string.into_inner();
            Wkt::MultiLineString(MultiLineString::new(
                line_strings.into_iter().map(reverse_line_string).collect(),
                dimension,
            ))
        }
        Wkt::MultiPolygon(multi_polygon) => {
            let (polygons, dimension) = multi_polygon.into_inner();
            Wkt::MultiPolygon(MultiPolygon::new(
                polygons.into_iter().map(reverse_polygon).collect(),
                dimension,
            ))
        }
        Wkt::GeometryCollection(collection) => {
            let (geometries, dimension) = collection.into_inner();
            Wkt::GeometryCollection(GeometryCollection::new(
                geometries.into_iter().map(reverse_wkt).collect(),
                dimension,
            ))
        }
    }
}

fn reverse_line_string(line_string: LineString<f64>) -> LineString<f64> {
    let (mut coords, dimension) = line_string.into_inner();
    coords.reverse();
    LineString::new(coords, dimension)
}

fn reverse_polygon(polygon: Polygon<f64>) -> Polygon<f64> {
    let (rings, dimension) = polygon.into_inner();
    Polygon::new(
        rings.into_iter().map(reverse_line_string).collect(),
        dimension,
    )
}

fn edit_line_string(
    coords: Vec<Coord<f64>>,
    dimension: WktDimension,
    operation: CoordinateOperation,
) -> GeoArrowResult<LineString<f64>> {
    let coords = coords
        .iter()
        .map(|coord| operation.edit_coord(coord))
        .collect::<GeoArrowResult<Vec<_>>>()?;
    Ok(LineString::new(coords, dimension))
}

pub(super) fn geometry_to_wkt(geometry: &impl GeometryTrait<T = f64>) -> GeoArrowResult<Wkt<f64>> {
    use geo_traits::GeometryType::*;

    Ok(match geometry.as_type() {
        Point(point) => Wkt::Point(point_to_wkt(point)?),
        LineString(line_string) => Wkt::LineString(line_string_to_wkt(line_string)?),
        Polygon(polygon) => Wkt::Polygon(polygon_to_wkt(polygon)?),
        MultiPoint(multi_point) => Wkt::MultiPoint(multi_point_to_wkt(multi_point)?),
        MultiLineString(multi_line_string) => {
            Wkt::MultiLineString(multi_line_string_to_wkt(multi_line_string)?)
        }
        MultiPolygon(multi_polygon) => Wkt::MultiPolygon(multi_polygon_to_wkt(multi_polygon)?),
        GeometryCollection(collection) => {
            let geometries = collection
                .geometries()
                .map(|geometry| geometry_to_wkt(&geometry))
                .collect::<GeoArrowResult<Vec<_>>>()?;
            Wkt::GeometryCollection(wkt::types::GeometryCollection::new(
                geometries,
                wkt_dimension(collection.dim())?,
            ))
        }
        Rect(rect) => rect_to_wkt(rect)?,
        Triangle(triangle) => triangle_to_wkt(triangle)?,
        Line(line) => Wkt::LineString(wkt::types::LineString::new(
            line.coords()
                .into_iter()
                .map(|coord| geo_coord_to_wkt(&coord))
                .collect::<GeoArrowResult<Vec<_>>>()?,
            wkt_dimension(line.dim())?,
        )),
    })
}

fn point_to_wkt(point: &impl PointTrait<T = f64>) -> GeoArrowResult<Point<f64>> {
    Ok(Point::new(
        point
            .coord()
            .map(|coord| geo_coord_to_wkt(&coord))
            .transpose()?,
        wkt_dimension(point.dim())?,
    ))
}

fn line_string_to_wkt(
    line_string: &impl LineStringTrait<T = f64>,
) -> GeoArrowResult<LineString<f64>> {
    Ok(LineString::new(
        line_string
            .coords()
            .map(|coord| geo_coord_to_wkt(&coord))
            .collect::<GeoArrowResult<Vec<_>>>()?,
        wkt_dimension(line_string.dim())?,
    ))
}

fn polygon_to_wkt(polygon: &impl PolygonTrait<T = f64>) -> GeoArrowResult<Polygon<f64>> {
    let mut rings = Vec::with_capacity(1 + polygon.num_interiors());
    if let Some(exterior) = polygon.exterior() {
        rings.push(line_string_to_wkt(&exterior)?);
        for interior in polygon.interiors() {
            rings.push(line_string_to_wkt(&interior)?);
        }
    }
    Ok(Polygon::new(rings, wkt_dimension(polygon.dim())?))
}

fn multi_point_to_wkt(
    multi_point: &impl MultiPointTrait<T = f64>,
) -> GeoArrowResult<MultiPoint<f64>> {
    Ok(MultiPoint::new(
        multi_point
            .points()
            .map(|point| point_to_wkt(&point))
            .collect::<GeoArrowResult<Vec<_>>>()?,
        wkt_dimension(multi_point.dim())?,
    ))
}

fn multi_line_string_to_wkt(
    multi_line_string: &impl MultiLineStringTrait<T = f64>,
) -> GeoArrowResult<MultiLineString<f64>> {
    Ok(MultiLineString::new(
        multi_line_string
            .line_strings()
            .map(|line_string| line_string_to_wkt(&line_string))
            .collect::<GeoArrowResult<Vec<_>>>()?,
        wkt_dimension(multi_line_string.dim())?,
    ))
}

fn multi_polygon_to_wkt(
    multi_polygon: &impl MultiPolygonTrait<T = f64>,
) -> GeoArrowResult<MultiPolygon<f64>> {
    Ok(MultiPolygon::new(
        multi_polygon
            .polygons()
            .map(|polygon| polygon_to_wkt(&polygon))
            .collect::<GeoArrowResult<Vec<_>>>()?,
        wkt_dimension(multi_polygon.dim())?,
    ))
}

fn rect_to_wkt(rect: &impl RectTrait<T = f64>) -> GeoArrowResult<Wkt<f64>> {
    let dimension = wkt_dimension(rect.dim())?;
    let min = geo_coord_to_wkt(&rect.min())?;
    let max = geo_coord_to_wkt(&rect.max())?;
    let coords = vec![
        min,
        corner(&min, max.x, min.y),
        max,
        corner(&min, min.x, max.y),
        min,
    ];
    Ok(Wkt::Polygon(Polygon::new(
        vec![LineString::new(coords, dimension)],
        dimension,
    )))
}

/// Convert a geometry for the WKT editing harness.
///
/// PostGIS exposes `box2d` as a POLYGON with the lower-left corner first and a
/// counter-clockwise ring. `rect_to_polygon` owns that conversion and rejects a
/// full `box3d`, whose POLYHEDRALSURFACE cannot be represented here.
fn geometry_to_wkt_for_wkt_edits(
    geometry: &impl GeometryTrait<T = f64>,
) -> GeoArrowResult<Wkt<f64>> {
    let geo_traits::GeometryType::Rect(rect) = geometry.as_type() else {
        return geometry_to_wkt(geometry);
    };

    crate::udf::native::geometry::rect_to_polygon(rect)
        .map_err(|error| GeoArrowError::External(Box::new(error)))
}

fn triangle_to_wkt(triangle: &impl TriangleTrait<T = f64>) -> GeoArrowResult<Wkt<f64>> {
    let dimension = wkt_dimension(triangle.dim())?;
    let mut coords = triangle
        .coords()
        .iter()
        .map(geo_coord_to_wkt)
        .collect::<GeoArrowResult<Vec<_>>>()?;
    coords.push(coords[0]);
    Ok(Wkt::Polygon(Polygon::new(
        vec![LineString::new(coords, dimension)],
        dimension,
    )))
}

fn corner(coord: &Coord<f64>, x: f64, y: f64) -> Coord<f64> {
    Coord {
        x,
        y,
        z: coord.z,
        m: coord.m,
    }
}

fn geo_coord_to_wkt(coord: &impl CoordTrait<T = f64>) -> GeoArrowResult<Coord<f64>> {
    let dimension = wkt_dimension(coord.dim())?;
    let z = matches!(dimension, WktDimension::XYZ | WktDimension::XYZM)
        .then(|| {
            coord.nth(2).ok_or_else(|| {
                GeoArrowError::InvalidGeoArrow(
                    "Coordinate is missing its declared Z ordinate".to_string(),
                )
            })
        })
        .transpose()?;
    let m = matches!(dimension, WktDimension::XYM | WktDimension::XYZM)
        .then(|| {
            coord
                .nth(if dimension == WktDimension::XYM { 2 } else { 3 })
                .ok_or_else(|| {
                    GeoArrowError::InvalidGeoArrow(
                        "Coordinate is missing its declared M ordinate".to_string(),
                    )
                })
        })
        .transpose()?;

    Ok(Coord {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    })
}

fn wkt_dimension(dimension: Dimensions) -> GeoArrowResult<WktDimension> {
    match dimension {
        Dimensions::Xy | Dimensions::Unknown(2) => Ok(WktDimension::XY),
        Dimensions::Xyz | Dimensions::Unknown(3) => Ok(WktDimension::XYZ),
        Dimensions::Xym => Ok(WktDimension::XYM),
        Dimensions::Xyzm | Dimensions::Unknown(4) => Ok(WktDimension::XYZM),
        Dimensions::Unknown(_) => Err(GeoArrowError::InvalidGeoArrow(format!(
            "Unsupported coordinate dimension: {dimension:?}"
        ))),
    }
}

fn has_ordinate(dimension: Dimensions, ordinate: Ordinate) -> bool {
    match ordinate {
        Ordinate::X | Ordinate::Y => true,
        Ordinate::Z => matches!(
            dimension,
            Dimensions::Xyz | Dimensions::Xyzm | Dimensions::Unknown(3) | Dimensions::Unknown(4)
        ),
        Ordinate::M => matches!(
            dimension,
            Dimensions::Xym | Dimensions::Xyzm | Dimensions::Unknown(4)
        ),
    }
}
