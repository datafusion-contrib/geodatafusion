use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, Float64Array};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geo_traits::{
    CoordTrait, Dimensions, GeometryCollectionTrait, GeometryTrait, LineStringTrait, LineTrait,
    MultiLineStringTrait, MultiPointTrait, MultiPolygonTrait, PointTrait, PolygonTrait, RectTrait,
    TriangleTrait,
};
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::builder::{
    GeometryBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{
    CoordType, Dimension, GeoArrowType, GeometryCollectionType, GeometryType, LineStringType,
    MultiLineStringType, MultiPointType, MultiPolygonType, PointType, PolygonType,
};
use wkt::Wkt;
use wkt::types::{
    Coord, Dimension as WktDimension, GeometryCollection, LineString, MultiLineString, MultiPoint,
    MultiPolygon, Point, Polygon,
};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::io::geometry::{
    GeometryArrayBuilder, GeometryCollectionArrayBuilder, decoded_row_geometry_collection,
    is_polyhedral_surface_field, is_polyhedral_surface_row, with_polyhedral_surface_metadata,
};

/// Alias for [`Force3DZ`], matching PostGIS `ST_Force3D`.
pub type Force3D = Force3DZ;

static FORCE_2D_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| optional_numeric_signature(0));
static FORCE_3D_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| optional_numeric_signature(1));
static FORCE_4D_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| optional_numeric_signature(2));

/// Keeps a bare `NULL` geometry argument as `DataType::Null` while coercing optional
/// numeric defaults (`zvalue` / `mvalue`) to `DataType::Float64`.
fn optional_numeric_signature(max_extra_args: usize) -> Signature {
    let geometries = any_geometry_type();
    let mut signatures = Vec::with_capacity((geometries.len() + 1) * (max_extra_args + 1));
    for extra_count in 0..=max_extra_args {
        let mut argument_types = vec![DataType::Null];
        argument_types.extend(std::iter::repeat_n(DataType::Float64, extra_count));
        signatures.push(TypeSignature::Exact(argument_types));
    }
    for geometry in geometries {
        for extra_count in 0..=max_extra_args {
            let mut argument_types = vec![geometry.clone()];
            argument_types.extend(std::iter::repeat_n(DataType::Float64, extra_count));
            signatures.push(TypeSignature::Exact(argument_types));
        }
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

/// `ST_Force2D` scalar UDF implementation: forces geometries into 2D (`XY`) coordinate mode.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force2D {
    coord_type: CoordType,
}

impl Force2D {
    /// Creates a new `ST_Force2D` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Force2D {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FORCE_2D_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force2D {
    fn name(&self) -> &str {
        "st_force2d"
    }

    fn signature(&self) -> &Signature {
        &FORCE_2D_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(force_return_field(args, Dimension::XY, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(force_dimension_impl(
            args,
            Dimension::XY,
            self.coord_type,
            None,
            None,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_2D_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Force the geometries into a \"2-dimensional mode\" so that all output representations will only have the X and Y coordinates.",
                "ST_Force2D(geometry)",
            )
            .with_argument("geomA", "geometry")
            .build()
        }))
    }
}

/// `ST_Force3DZ` (`ST_Force3D`) scalar UDF implementation: forces geometries into `XYZ` coordinate mode.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force3DZ {
    coord_type: CoordType,
    aliases: Vec<String>,
}

impl Force3DZ {
    /// Creates a new `ST_Force3DZ` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            coord_type,
            aliases: vec!["st_force3d".to_string()],
        }
    }
}

impl Default for Force3DZ {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FORCE_3DZ_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force3DZ {
    fn name(&self) -> &str {
        "st_force3dz"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &FORCE_3D_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(force_return_field(args, Dimension::XYZ, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(force_dimension_impl(
            args,
            Dimension::XYZ,
            self.coord_type,
            Some(1),
            None,
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_3DZ_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Force the geometries into XYZ mode. This is an alias for ST_Force3D.",
                "ST_Force3DZ(geometry, zvalue = 0.0)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("zvalue", "float8 (optional, default 0.0)")
            .build()
        }))
    }
}

/// `ST_Force3DM` scalar UDF implementation: forces geometries into `XYM` coordinate mode.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force3DM {
    coord_type: CoordType,
}

impl Force3DM {
    /// Creates a new `ST_Force3DM` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Force3DM {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FORCE_3DM_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force3DM {
    fn name(&self) -> &str {
        "st_force3dm"
    }

    fn signature(&self) -> &Signature {
        &FORCE_3D_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(force_return_field(args, Dimension::XYM, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(force_dimension_impl(
            args,
            Dimension::XYM,
            self.coord_type,
            None,
            Some(1),
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_3DM_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Force the geometries into XYM mode.",
                "ST_Force3DM(geometry, mvalue = 0.0)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("mvalue", "float8 (optional, default 0.0)")
            .build()
        }))
    }
}

/// `ST_Force4D` scalar UDF implementation: forces geometries into `XYZM` coordinate mode.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force4D {
    coord_type: CoordType,
}

impl Force4D {
    /// Creates a new `ST_Force4D` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

impl Default for Force4D {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static FORCE_4D_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force4D {
    fn name(&self) -> &str {
        "st_force4d"
    }

    fn signature(&self) -> &Signature {
        &FORCE_4D_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(force_return_field(args, Dimension::XYZM, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(force_dimension_impl(
            args,
            Dimension::XYZM,
            self.coord_type,
            Some(1),
            Some(2),
        )?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_4D_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Force the geometries into XYZM mode.",
                "ST_Force4D(geometry, zvalue = 0.0, mvalue = 0.0)",
            )
            .with_argument("geomA", "geometry")
            .with_argument("zvalue", "float8 (optional, default 0.0)")
            .with_argument("mvalue", "float8 (optional, default 0.0)")
            .build()
        }))
    }
}

fn force_return_field(
    args: ReturnFieldArgs,
    target_dim: Dimension,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    if args.arg_fields[0].data_type() == &DataType::Null {
        let output_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
        return Ok(Arc::new(output_type.to_field("", true)));
    }

    let input_field = args.arg_fields[0].as_ref();
    let input_type = GeoArrowType::from_arrow_field(input_field)?;
    let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
    let is_polyhedral_surface = is_polyhedral_surface_field(input_field)
        || matches!(
            &input_type,
            GeoArrowType::Rect(t) if matches!(t.dimension(), Dimension::XYZ | Dimension::XYZM)
        );

    let output_type = match input_type {
        GeoArrowType::Point(t) => GeoArrowType::Point(
            PointType::new(target_dim, t.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::LineString(t) => GeoArrowType::LineString(
            LineStringType::new(target_dim, t.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::Polygon(t) => GeoArrowType::Polygon(
            PolygonType::new(target_dim, t.metadata().clone()).with_coord_type(coord_type),
        ),
        // Convert 2D Box/Rect inputs to Polygon (`box2d::geometry`) and 3D Box/Rect inputs
        // to a 6-face PolyhedralSurface (`box3d::geometry`, backed by MultiPolygon).
        GeoArrowType::Rect(t) => match t.dimension() {
            Dimension::XY | Dimension::XYM => GeoArrowType::Polygon(
                PolygonType::new(target_dim, t.metadata().clone()).with_coord_type(coord_type),
            ),
            Dimension::XYZ | Dimension::XYZM => GeoArrowType::MultiPolygon(
                MultiPolygonType::new(target_dim, t.metadata().clone()).with_coord_type(coord_type),
            ),
        },
        GeoArrowType::MultiPoint(t) => GeoArrowType::MultiPoint(
            MultiPointType::new(target_dim, t.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::MultiLineString(t) => GeoArrowType::MultiLineString(
            MultiLineStringType::new(target_dim, t.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::MultiPolygon(t) => GeoArrowType::MultiPolygon(
            MultiPolygonType::new(target_dim, t.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::GeometryCollection(t) => GeoArrowType::GeometryCollection(
            GeometryCollectionType::new(target_dim, t.metadata().clone())
                .with_coord_type(coord_type),
        ),
        GeoArrowType::Geometry(t) => GeoArrowType::Geometry(
            GeometryType::new(t.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::Wkb(t) | GeoArrowType::LargeWkb(t) | GeoArrowType::WkbView(t) => {
            GeoArrowType::Geometry(
                GeometryType::new(t.metadata().clone()).with_coord_type(coord_type),
            )
        }
        GeoArrowType::Wkt(t) | GeoArrowType::LargeWkt(t) | GeoArrowType::WktView(t) => {
            GeoArrowType::Geometry(
                GeometryType::new(t.metadata().clone()).with_coord_type(coord_type),
            )
        }
    };

    let mut field = output_type.to_field("", nullable);
    if is_polyhedral_surface {
        field = with_polyhedral_surface_metadata(field);
    }
    Ok(Arc::new(field))
}

#[derive(Clone)]
enum FloatDefaultColumn {
    Scalar(Option<f64>),
    Array(Float64Array),
}

impl FloatDefaultColumn {
    fn from_arg(args: &[ColumnarValue], index: Option<usize>) -> GeoDataFusionResult<Self> {
        let Some(arg) = index.and_then(|idx| args.get(idx)) else {
            return Ok(Self::Scalar(Some(0.0)));
        };
        match arg.cast_to(&DataType::Float64, None)? {
            ColumnarValue::Scalar(ScalarValue::Float64(val)) => Ok(Self::Scalar(val)),
            ColumnarValue::Array(arr) => Ok(Self::Array(arr.as_primitive::<Float64Type>().clone())),
            other => Err(DataFusionError::Internal(format!(
                "Expected Float64 argument in force_dimension, got {other:?}"
            ))
            .into()),
        }
    }

    #[inline]
    fn value(&self, row: usize) -> Option<f64> {
        match self {
            Self::Scalar(val) => *val,
            Self::Array(arr) => (!arr.is_null(row)).then(|| arr.value(row)),
        }
    }
}

fn force_dimension_impl(
    args: ScalarFunctionArgs,
    target_dim: Dimension,
    coord_type: CoordType,
    z_arg_idx: Option<usize>,
    m_arg_idx: Option<usize>,
) -> GeoDataFusionResult<ColumnarValue> {
    if args.arg_fields[0].data_type() == &DataType::Null {
        let output_type = GeometryType::new(Default::default()).with_coord_type(coord_type);
        let mut builder = GeometryBuilder::new(output_type);
        for _ in 0..args.number_rows {
            builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
        }
        return Ok(builder.finish().into_array_ref().into());
    }

    let geom_array = ColumnarValue::values_to_arrays(&args.args[..1])?
        .into_iter()
        .next()
        .unwrap();
    let geo_array = from_arrow_array(&geom_array, &args.arg_fields[0])?;
    let z_defaults = FloatDefaultColumn::from_arg(&args.args, z_arg_idx)?;
    let m_defaults = FloatDefaultColumn::from_arg(&args.args, m_arg_idx)?;
    let out_type = GeoArrowType::from_arrow_field(args.return_field.as_ref())?;
    let geo_array_ref = geo_array.as_ref();
    let input_field = args.arg_fields[0].as_ref();

    let result = downcast_geoarrow_array!(
        geo_array_ref,
        force_array_impl,
        input_field,
        out_type,
        target_dim,
        &z_defaults,
        &m_defaults
    )?;
    Ok(result.into_array_ref().into())
}

fn force_array_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    input_field: &arrow_schema::Field,
    out_type: GeoArrowType,
    target_dim: Dimension,
    z_defaults: &FloatDefaultColumn,
    m_defaults: &FloatDefaultColumn,
) -> GeoDataFusionResult<Arc<dyn GeoArrowArray>> {
    let wkt_dim = wkt_dimension(target_dim);
    let len = array.len();

    match out_type {
        GeoArrowType::Point(typ) => {
            let mut builder = PointBuilder::with_capacity(typ, len);
            for (row, item) in array.iter().enumerate() {
                match (
                    item.transpose()?,
                    z_defaults.value(row),
                    m_defaults.value(row),
                ) {
                    (Some(geom), Some(z_def), Some(m_def)) => {
                        let geo_traits::GeometryType::Point(pt) = geom.as_type() else {
                            return Err(DataFusionError::Internal(
                                "Expected Point geometry".to_string(),
                            )
                            .into());
                        };
                        let forced = force_point(pt, wkt_dim, z_def, m_def);
                        builder.push_point(Some(&forced));
                    }
                    _ => builder.push_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::LineString(typ) => {
            let mut builder = LineStringBuilder::new(typ);
            for (row, item) in array.iter().enumerate() {
                match (
                    item.transpose()?,
                    z_defaults.value(row),
                    m_defaults.value(row),
                ) {
                    (Some(geom), Some(z_def), Some(m_def)) => {
                        let geo_traits::GeometryType::LineString(ls) = geom.as_type() else {
                            return Err(DataFusionError::Internal(
                                "Expected LineString geometry".to_string(),
                            )
                            .into());
                        };
                        let forced = force_line_string(ls, wkt_dim, z_def, m_def);
                        builder.push_line_string(Some(&forced))?;
                    }
                    _ => builder.push_line_string(None::<&LineString<f64>>)?,
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::Polygon(typ) => {
            let mut builder = PolygonBuilder::new(typ);
            for (row, item) in array.iter().enumerate() {
                match (
                    item.transpose()?,
                    z_defaults.value(row),
                    m_defaults.value(row),
                ) {
                    (Some(geom), Some(z_def), Some(m_def)) => {
                        let forced = match geom.as_type() {
                            geo_traits::GeometryType::Polygon(poly) => {
                                force_polygon(poly, wkt_dim, z_def, m_def)
                            }
                            geo_traits::GeometryType::Rect(rect) => {
                                force_rect_as_polygon(rect, wkt_dim, z_def, m_def)
                            }
                            _ => {
                                return Err(DataFusionError::Internal(
                                    "Expected Polygon or Rect geometry".to_string(),
                                )
                                .into());
                            }
                        };
                        builder.push_polygon(Some(&forced))?;
                    }
                    _ => builder.push_polygon(None::<&Polygon<f64>>)?,
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::MultiPoint(typ) => {
            let mut builder = MultiPointBuilder::new(typ);
            for (row, item) in array.iter().enumerate() {
                match (
                    item.transpose()?,
                    z_defaults.value(row),
                    m_defaults.value(row),
                ) {
                    (Some(geom), Some(z_def), Some(m_def)) => {
                        let geo_traits::GeometryType::MultiPoint(mp) = geom.as_type() else {
                            return Err(DataFusionError::Internal(
                                "Expected MultiPoint geometry".to_string(),
                            )
                            .into());
                        };
                        let forced = force_multi_point(mp, wkt_dim, z_def, m_def);
                        builder.push_multi_point(Some(&forced))?;
                    }
                    _ => builder.push_multi_point(None::<&MultiPoint<f64>>)?,
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::MultiLineString(typ) => {
            let mut builder = MultiLineStringBuilder::new(typ);
            for (row, item) in array.iter().enumerate() {
                match (
                    item.transpose()?,
                    z_defaults.value(row),
                    m_defaults.value(row),
                ) {
                    (Some(geom), Some(z_def), Some(m_def)) => {
                        let geo_traits::GeometryType::MultiLineString(mls) = geom.as_type() else {
                            return Err(DataFusionError::Internal(
                                "Expected MultiLineString geometry".to_string(),
                            )
                            .into());
                        };
                        let forced = force_multi_line_string(mls, wkt_dim, z_def, m_def);
                        builder.push_multi_line_string(Some(&forced))?;
                    }
                    _ => builder.push_multi_line_string(None::<&MultiLineString<f64>>)?,
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::MultiPolygon(typ) => {
            let mut builder = MultiPolygonBuilder::new(typ);
            for (row, item) in array.iter().enumerate() {
                match (
                    item.transpose()?,
                    z_defaults.value(row),
                    m_defaults.value(row),
                ) {
                    (Some(geom), Some(z_def), Some(m_def)) => {
                        let forced = match geom.as_type() {
                            geo_traits::GeometryType::MultiPolygon(mpoly) => {
                                force_multi_polygon(mpoly, wkt_dim, z_def, m_def)
                            }
                            geo_traits::GeometryType::Rect(rect) => {
                                force_rect_3d_as_multi_polygon(rect, wkt_dim, z_def, m_def)
                            }
                            _ => {
                                return Err(DataFusionError::Internal(
                                    "Expected MultiPolygon or 3D Rect geometry".to_string(),
                                )
                                .into());
                            }
                        };
                        builder.push_multi_polygon(Some(&forced))?;
                    }
                    _ => builder.push_multi_polygon(None::<&MultiPolygon<f64>>)?,
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::GeometryCollection(typ) => {
            let mut builder = GeometryCollectionArrayBuilder::new(typ);
            for (row, item) in array.iter().enumerate() {
                match (
                    item.transpose()?,
                    z_defaults.value(row),
                    m_defaults.value(row),
                ) {
                    (Some(geom), Some(z_def), Some(m_def)) => {
                        let forced =
                            if let Some(decoded) = decoded_row_geometry_collection(array, row)? {
                                force_geometry_collection(&decoded, wkt_dim, z_def, m_def)
                            } else if let geo_traits::GeometryType::GeometryCollection(gc) =
                                geom.as_type()
                            {
                                force_geometry_collection(gc, wkt_dim, z_def, m_def)
                            } else {
                                return Err(DataFusionError::Internal(
                                    "Expected GeometryCollection geometry".to_string(),
                                )
                                .into());
                            };
                        builder.push_geometry_collection(Some(&forced))?;
                    }
                    _ => builder.push_geometry_collection(None::<&GeometryCollection<f64>>)?,
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        GeoArrowType::Geometry(typ) => {
            let mut builder = GeometryArrayBuilder::new(typ);
            for (row, item) in array.iter().enumerate() {
                match (
                    item.transpose()?,
                    z_defaults.value(row),
                    m_defaults.value(row),
                ) {
                    (Some(geom), Some(z_def), Some(m_def)) => {
                        let is_polyhedral = is_polyhedral_surface_row(array, input_field, row)
                            || matches!(
                                geom.as_type(),
                                geo_traits::GeometryType::Rect(_)
                                    if matches!(geom.dim(), Dimensions::Xyz | Dimensions::Xyzm)
                            );
                        let forced =
                            if let Some(decoded) = decoded_row_geometry_collection(array, row)? {
                                Wkt::GeometryCollection(force_geometry_collection(
                                    &decoded, wkt_dim, z_def, m_def,
                                ))
                            } else {
                                force_geometry(&geom, wkt_dim, z_def, m_def)
                            };
                        if is_polyhedral {
                            builder.push_polyhedral_surface(Some(&forced))?;
                        } else {
                            builder.push_geometry(Some(&forced))?;
                        }
                    }
                    _ => builder.push_geometry(None::<&Wkt<f64>>)?,
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(DataFusionError::Internal(format!(
            "Unexpected output GeoArrowType in force_dimension: {other:?}"
        ))
        .into()),
    }
}

#[inline]
fn wkt_dimension(dimension: Dimension) -> WktDimension {
    match dimension {
        Dimension::XY => WktDimension::XY,
        Dimension::XYZ => WktDimension::XYZ,
        Dimension::XYM => WktDimension::XYM,
        Dimension::XYZM => WktDimension::XYZM,
    }
}

#[inline]
fn force_coord(
    coord: &impl CoordTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> Coord<f64> {
    let (in_z, in_m) = match coord.dim() {
        Dimensions::Xy => (None, None),
        Dimensions::Xyz => (coord.nth(2), None),
        Dimensions::Xym => (None, coord.nth(2)),
        Dimensions::Xyzm | Dimensions::Unknown(_) => (coord.nth(2), coord.nth(3)),
    };

    let (z, m) = match target_dim {
        WktDimension::XY => (None, None),
        WktDimension::XYZ => (Some(in_z.unwrap_or(z_default)), None),
        WktDimension::XYM => (None, Some(in_m.unwrap_or(m_default))),
        WktDimension::XYZM => (
            Some(in_z.unwrap_or(z_default)),
            Some(in_m.unwrap_or(m_default)),
        ),
    };

    Coord {
        x: coord.x(),
        y: coord.y(),
        z,
        m,
    }
}

fn force_point(
    point: &impl PointTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> Point<f64> {
    match point.coord() {
        Some(coord) => Point::from_coord(force_coord(&coord, target_dim, z_default, m_default)),
        None => Point::empty(target_dim),
    }
}

fn force_line_string(
    line_string: &impl LineStringTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> LineString<f64> {
    let coords = line_string
        .coords()
        .map(|coord| force_coord(&coord, target_dim, z_default, m_default))
        .collect();
    LineString::new(coords, target_dim)
}

fn force_polygon(
    polygon: &impl PolygonTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> Polygon<f64> {
    let mut rings =
        Vec::with_capacity(usize::from(polygon.exterior().is_some()) + polygon.num_interiors());
    if let Some(exterior) = polygon.exterior() {
        rings.push(force_line_string(
            &exterior, target_dim, z_default, m_default,
        ));
    }
    for interior in polygon.interiors() {
        rings.push(force_line_string(
            &interior, target_dim, z_default, m_default,
        ));
    }
    Polygon::new(rings, target_dim)
}

fn force_rect_as_polygon(
    rect: &impl RectTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> Polygon<f64> {
    let min = force_coord(&rect.min(), target_dim, z_default, m_default);
    let max = force_coord(&rect.max(), target_dim, z_default, m_default);
    let ring = LineString::new(
        vec![
            Coord {
                x: min.x,
                y: min.y,
                z: min.z,
                m: min.m,
            },
            Coord {
                x: min.x,
                y: max.y,
                z: min.z,
                m: min.m,
            },
            Coord {
                x: max.x,
                y: max.y,
                z: max.z,
                m: max.m,
            },
            Coord {
                x: max.x,
                y: min.y,
                z: min.z,
                m: min.m,
            },
            Coord {
                x: min.x,
                y: min.y,
                z: min.z,
                m: min.m,
            },
        ],
        target_dim,
    );
    Polygon::new(vec![ring], target_dim)
}

fn force_rect_3d_as_multi_polygon(
    rect: &impl RectTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> MultiPolygon<f64> {
    let min = force_coord(&rect.min(), target_dim, z_default, m_default);
    let max = force_coord(&rect.max(), target_dim, z_default, m_default);
    let m = min.m;
    let v = |x: f64, y: f64, z: Option<f64>| Coord { x, y, z, m };
    let face = |coords: [Coord<f64>; 5]| {
        Polygon::new(
            vec![LineString::new(coords.to_vec(), target_dim)],
            target_dim,
        )
    };

    // Matches PostGIS `BOX3D_to_LWGEOM` face order and ring orientation:
    // bottom (zmin), top (zmax), left (xmin), right (xmax), front (ymin), back (ymax).
    let faces = vec![
        face([
            v(min.x, min.y, min.z),
            v(min.x, max.y, min.z),
            v(max.x, max.y, min.z),
            v(max.x, min.y, min.z),
            v(min.x, min.y, min.z),
        ]),
        face([
            v(min.x, min.y, max.z),
            v(max.x, min.y, max.z),
            v(max.x, max.y, max.z),
            v(min.x, max.y, max.z),
            v(min.x, min.y, max.z),
        ]),
        face([
            v(min.x, min.y, min.z),
            v(min.x, min.y, max.z),
            v(min.x, max.y, max.z),
            v(min.x, max.y, min.z),
            v(min.x, min.y, min.z),
        ]),
        face([
            v(max.x, min.y, min.z),
            v(max.x, max.y, min.z),
            v(max.x, max.y, max.z),
            v(max.x, min.y, max.z),
            v(max.x, min.y, min.z),
        ]),
        face([
            v(min.x, min.y, min.z),
            v(max.x, min.y, min.z),
            v(max.x, min.y, max.z),
            v(min.x, min.y, max.z),
            v(min.x, min.y, min.z),
        ]),
        face([
            v(min.x, max.y, min.z),
            v(min.x, max.y, max.z),
            v(max.x, max.y, max.z),
            v(max.x, max.y, min.z),
            v(min.x, max.y, min.z),
        ]),
    ];
    MultiPolygon::new(faces, target_dim)
}

fn force_multi_point(
    multi_point: &impl MultiPointTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> MultiPoint<f64> {
    let points = multi_point
        .points()
        .map(|pt| force_point(&pt, target_dim, z_default, m_default))
        .collect();
    MultiPoint::new(points, target_dim)
}

fn force_multi_line_string(
    multi_line_string: &impl MultiLineStringTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> MultiLineString<f64> {
    let line_strings = multi_line_string
        .line_strings()
        .map(|ls| force_line_string(&ls, target_dim, z_default, m_default))
        .collect();
    MultiLineString::new(line_strings, target_dim)
}

fn force_multi_polygon(
    multi_polygon: &impl MultiPolygonTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> MultiPolygon<f64> {
    let polygons = multi_polygon
        .polygons()
        .map(|poly| force_polygon(&poly, target_dim, z_default, m_default))
        .collect();
    MultiPolygon::new(polygons, target_dim)
}

fn force_geometry_collection(
    collection: &impl GeometryCollectionTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> GeometryCollection<f64> {
    let geometries = collection
        .geometries()
        .map(|geom| force_geometry(&geom, target_dim, z_default, m_default))
        .collect();
    GeometryCollection::new(geometries, target_dim)
}

fn force_geometry(
    geometry: &impl GeometryTrait<T = f64>,
    target_dim: WktDimension,
    z_default: f64,
    m_default: f64,
) -> Wkt<f64> {
    match geometry.as_type() {
        geo_traits::GeometryType::Point(pt) => {
            Wkt::Point(force_point(pt, target_dim, z_default, m_default))
        }
        geo_traits::GeometryType::LineString(ls) => {
            Wkt::LineString(force_line_string(ls, target_dim, z_default, m_default))
        }
        geo_traits::GeometryType::Polygon(poly) => {
            Wkt::Polygon(force_polygon(poly, target_dim, z_default, m_default))
        }
        geo_traits::GeometryType::MultiPoint(mp) => {
            Wkt::MultiPoint(force_multi_point(mp, target_dim, z_default, m_default))
        }
        geo_traits::GeometryType::MultiLineString(mls) => Wkt::MultiLineString(
            force_multi_line_string(mls, target_dim, z_default, m_default),
        ),
        geo_traits::GeometryType::MultiPolygon(mpoly) => {
            Wkt::MultiPolygon(force_multi_polygon(mpoly, target_dim, z_default, m_default))
        }
        geo_traits::GeometryType::GeometryCollection(gc) => Wkt::GeometryCollection(
            force_geometry_collection(gc, target_dim, z_default, m_default),
        ),
        geo_traits::GeometryType::Rect(rect) => match rect.dim() {
            Dimensions::Xyz | Dimensions::Xyzm => Wkt::MultiPolygon(
                force_rect_3d_as_multi_polygon(rect, target_dim, z_default, m_default),
            ),
            _ => Wkt::Polygon(force_rect_as_polygon(
                rect, target_dim, z_default, m_default,
            )),
        },
        geo_traits::GeometryType::Line(line) => {
            let start = force_coord(&line.start(), target_dim, z_default, m_default);
            let end = force_coord(&line.end(), target_dim, z_default, m_default);
            Wkt::LineString(LineString::new(vec![start, end], target_dim))
        }
        geo_traits::GeometryType::Triangle(tri) => {
            let first = force_coord(&tri.first(), target_dim, z_default, m_default);
            let second = force_coord(&tri.second(), target_dim, z_default, m_default);
            let third = force_coord(&tri.third(), target_dim, z_default, m_default);
            let ring = LineString::new(vec![first, second, third, first], target_dim);
            Wkt::Polygon(Polygon::new(vec![ring], target_dim))
        }
    }
}
