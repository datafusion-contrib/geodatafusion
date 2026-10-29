//! Native geometry editors.

use std::sync::{LazyLock, OnceLock};

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_schema::Dimension;
use transform::{CoordinateOperation, invoke, parse_ordinate_spec, return_field};
use wkt::types::Dimension as WktDimension;

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;

mod advanced;
mod collection;
mod coordinate;
mod project;
mod transform;
mod vertex;

pub(crate) use collection::collection_homogenize;
pub use coordinate::{
    LineExtend, QuantizeCoordinates, RemoveRepeatedPoints, Segmentize, ShiftLongitude, WrapX,
};
pub use project::Project;
// The GeoArrow/WKT value conversion is shared with native processing. It
// lives beside the editor that first needed it until the conversion split is
// large enough to justify its own module.
pub(crate) use transform::wkt_values;
pub use vertex::{AddPoint, RemovePoint, Reverse, Scroll, SetPoint};

/// Keeps a bare `NULL` argument as `DataType::Null` instead of letting
/// DataFusion coerce it to an unnamed canonical geometry struct.
static SINGLE_GEOMETRY_OR_NULL_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        vec![
            TypeSignature::Exact(vec![DataType::Null]),
            TypeSignature::Uniform(1, any_geometry_type()),
        ],
        Volatility::Immutable,
    )
});

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct FlipCoordinates;

impl FlipCoordinates {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FlipCoordinates {
    fn default() -> Self {
        Self::new()
    }
}

static FLIP_COORDINATES_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for FlipCoordinates {
    fn name(&self) -> &str {
        "st_flipcoordinates"
    }

    fn signature(&self) -> &Signature {
        &SINGLE_GEOMETRY_OR_NULL_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_FlipCoordinates requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field(args, None)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke(args, None, |_, _| {
            Ok(Some(CoordinateOperation::Flip))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FLIP_COORDINATES_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a version of a geometry with X and Y axis flipped.",
                "ST_FlipCoordinates(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force2D;

impl Force2D {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Force2D {
    fn default() -> Self {
        Self::new()
    }
}

static FORCE_2D_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force2D {
    fn name(&self) -> &str {
        "st_force2d"
    }

    fn signature(&self) -> &Signature {
        &SINGLE_GEOMETRY_OR_NULL_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Force2D requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field(args, Some(Dimension::XY))?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke(args, Some(Dimension::XY), |_, _| {
            Ok(Some(CoordinateOperation::Force {
                target: WktDimension::XY,
                z_default: None,
                m_default: None,
            }))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_2D_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces a geometry into XY mode, dropping Z and M ordinates.",
                "ST_Force2D(geometry)",
            )
            .with_argument("geometry", "geometry")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force3D {
    signature: Signature,
}

impl Force3D {
    pub fn new() -> Self {
        Self {
            signature: optional_numeric_signature(1),
        }
    }
}

impl Default for Force3D {
    fn default() -> Self {
        Self::new()
    }
}

static FORCE_3D_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force3D {
    fn name(&self) -> &str {
        "st_force3d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Force3D requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field(args, Some(Dimension::XYZ))?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(force_3d(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_3D_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces a geometry into XYZ mode. If Z is missing, the optional Z value defaults to 0. M is dropped.",
                "ST_Force3D(geometry[, zvalue])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("zvalue", "Z value for coordinates without a Z ordinate")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force3DZ {
    signature: Signature,
}

impl Force3DZ {
    pub fn new() -> Self {
        Self {
            signature: optional_numeric_signature(1),
        }
    }
}

impl Default for Force3DZ {
    fn default() -> Self {
        Self::new()
    }
}

static FORCE_3DZ_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force3DZ {
    fn name(&self) -> &str {
        "st_force3dz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Force3DZ requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field(args, Some(Dimension::XYZ))?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(force_3d(args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_3DZ_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces a geometry into XYZ mode. If Z is missing, the optional Z value defaults to 0. M is dropped.",
                "ST_Force3DZ(geometry[, zvalue])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("zvalue", "Z value for coordinates without a Z ordinate")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force3DM {
    signature: Signature,
}

impl Force3DM {
    pub fn new() -> Self {
        Self {
            signature: optional_numeric_signature(1),
        }
    }
}

impl Default for Force3DM {
    fn default() -> Self {
        Self::new()
    }
}

static FORCE_3DM_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force3DM {
    fn name(&self) -> &str {
        "st_force3dm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Force3DM requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field(args, Some(Dimension::XYM))?)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        add_default_numeric_arg(&mut args, 1, 0.0);
        Ok(invoke(args, Some(Dimension::XYM), |index, arrays| {
            let geometry = &arrays[0];
            let m = arrays[1].as_primitive::<Float64Type>();
            if geometry.is_null(index) || m.is_null(index) {
                return Ok(None);
            }
            Ok(Some(CoordinateOperation::Force {
                target: WktDimension::XYM,
                z_default: None,
                m_default: Some(m.value(index)),
            }))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_3DM_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces a geometry into XYM mode. If M is missing, the optional M value defaults to 0. Z is dropped.",
                "ST_Force3DM(geometry[, mvalue])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("mvalue", "M value for coordinates without an M ordinate")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Force4D {
    signature: Signature,
}

impl Force4D {
    pub fn new() -> Self {
        Self {
            signature: optional_numeric_signature(2),
        }
    }
}

impl Default for Force4D {
    fn default() -> Self {
        Self::new()
    }
}

static FORCE_4D_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Force4D {
    fn name(&self) -> &str {
        "st_force4d"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Force4D requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field(args, Some(Dimension::XYZM))?)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        add_default_numeric_arg(&mut args, 1, 0.0);
        add_default_numeric_arg(&mut args, 2, 0.0);
        Ok(invoke(args, Some(Dimension::XYZM), |index, arrays| {
            let geometry = &arrays[0];
            let z = arrays[1].as_primitive::<Float64Type>();
            let m = arrays[2].as_primitive::<Float64Type>();
            if geometry.is_null(index) || z.is_null(index) || m.is_null(index) {
                return Ok(None);
            }
            Ok(Some(CoordinateOperation::Force {
                target: WktDimension::XYZM,
                z_default: Some(z.value(index)),
                m_default: Some(m.value(index)),
            }))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(FORCE_4D_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Forces a geometry into XYZM mode. Missing Z and M values default to 0.",
                "ST_Force4D(geometry[, zvalue[, mvalue]])",
            )
            .with_argument("geometry", "geometry")
            .with_argument("zvalue", "Z value for coordinates without a Z ordinate")
            .with_argument("mvalue", "M value for coordinates without an M ordinate")
            .build()
        }))
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct SwapOrdinates {
    signature: Signature,
}

impl SwapOrdinates {
    pub fn new() -> Self {
        Self {
            signature: swap_signature(),
        }
    }
}

impl Default for SwapOrdinates {
    fn default() -> Self {
        Self::new()
    }
}

static SWAP_ORDINATES_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for SwapOrdinates {
    fn name(&self) -> &str {
        "st_swapordinates"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_SwapOrdinates requires return_field_from_args".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field(args, None)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(invoke(args, None, |index, arrays| {
            if arrays[0].is_null(index) || arrays[1].is_null(index) {
                return Ok(None);
            }
            let ords = string_value(&arrays[1], index)?;
            Ok(Some(parse_ordinate_spec(ords)?))
        })?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(SWAP_ORDINATES_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a geometry with two named ordinates swapped. The ordinate specification must contain exactly two letters from x, y, z, and m.",
                "ST_SwapOrdinates(geometry, ordinates)",
            )
            .with_argument("geometry", "geometry")
            .with_argument("ordinates", "two-character ordinate specification")
            .build()
        }))
    }
}

fn force_3d(mut args: ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    add_default_numeric_arg(&mut args, 1, 0.0);
    invoke(args, Some(Dimension::XYZ), |index, arrays| {
        let geometry = &arrays[0];
        let z = arrays[1].as_primitive::<Float64Type>();
        if geometry.is_null(index) || z.is_null(index) {
            return Ok(None);
        }
        Ok(Some(CoordinateOperation::Force {
            target: WktDimension::XYZ,
            z_default: Some(z.value(index)),
            m_default: None,
        }))
    })
}

fn add_default_numeric_arg(args: &mut ScalarFunctionArgs, index: usize, default: f64) {
    if args.args.len() == index {
        args.args
            .push(ColumnarValue::Scalar(ScalarValue::Float64(Some(default))));
    }
}

fn optional_numeric_signature(max_extra_args: usize) -> Signature {
    let mut signatures = Vec::new();
    for extra_count in 0..=max_extra_args {
        let mut argument_types = vec![DataType::Null];
        argument_types.extend(std::iter::repeat_n(DataType::Float64, extra_count));
        signatures.push(TypeSignature::Exact(argument_types));
    }
    for geometry in any_geometry_type() {
        for extra_count in 0..=max_extra_args {
            let mut argument_types = vec![geometry.clone()];
            argument_types.extend(std::iter::repeat_n(DataType::Float64, extra_count));
            signatures.push(TypeSignature::Exact(argument_types));
        }
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

fn swap_signature() -> Signature {
    let mut signatures = Vec::new();
    for string_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        signatures.push(TypeSignature::Exact(vec![DataType::Null, string_type]));
    }
    for geometry in any_geometry_type() {
        for string_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            signatures.push(TypeSignature::Exact(vec![geometry.clone(), string_type]));
        }
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

fn string_value(array: &arrow_array::ArrayRef, index: usize) -> GeoDataFusionResult<&str> {
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_string::<i32>().value(index)),
        DataType::LargeUtf8 => Ok(array.as_string::<i64>().value(index)),
        DataType::Utf8View => Ok(array.as_string_view().value(index)),
        data_type => Err(DataFusionError::Execution(format!(
            "ST_SwapOrdinates expected a string ordinate specification, got {data_type}"
        ))
        .into()),
    }
}

pub fn register(session_context: &datafusion::prelude::SessionContext) {
    session_context.register_udf(collection::CollectionExtract::new().into());
    session_context.register_udf(collection::CollectionHomogenize.into());
    session_context.register_udf(FlipCoordinates.into());
    session_context.register_udf(collection::ForceCollection.into());
    session_context.register_udf(Force2D.into());
    session_context.register_udf(Force3D::new().into());
    session_context.register_udf(Force3DZ::new().into());
    session_context.register_udf(Force3DM::new().into());
    session_context.register_udf(Force4D::new().into());
    session_context.register_udf(collection::ForcePolygonCCW.into());
    session_context.register_udf(collection::ForcePolygonCW.into());
    session_context.register_udf(collection::ForceRHR.into());
    session_context.register_udf(collection::ForceSFS::new().into());
    session_context.register_udf(collection::Multi.into());
    session_context.register_udf(Project::new().into());
    session_context.register_udf(SwapOrdinates::new().into());
    advanced::register(session_context);
    coordinate::register(session_context);
    vertex::register(session_context);
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, GeometryCollectionTrait, GeometryTrait, PointTrait};
    use geoarrow_array::array::GeometryArray;
    use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
    use geoarrow_schema::{CoordType, GeometryType};
    use wkt::Wkt;

    use crate::udf::native::geometry::StrictGeometryBuilder;

    #[tokio::test]
    async fn generic_geometry_collection_identity_is_preserved() {
        let context = SessionContext::new();
        crate::register(&context);

        let geometry_type =
            GeometryType::new(Default::default()).with_coord_type(CoordType::Separated);
        let mut builder = StrictGeometryBuilder::new(geometry_type.clone());
        let collection = Wkt::from_str("GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4))").unwrap();
        builder.push_geometry(Some(&collection)).unwrap();
        builder.push_geometry(None::<&Wkt<f64>>).unwrap();
        let array = builder.finish();

        let field = geometry_type.to_field("geom", true);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field])),
            vec![array.to_array_ref()],
        )
        .unwrap();
        context.register_batch("t", batch).unwrap();

        let dataframe = context
            .sql("SELECT ST_FlipCoordinates(geom) FROM t")
            .await
            .unwrap();
        let batch = dataframe
            .collect()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let output =
            GeometryArray::try_from((batch.column(0).as_ref(), batch.schema_ref().field(0)))
                .unwrap();
        assert_eq!(output.len(), 2);
        assert!(output.is_null(1));

        let collection = output.value(0).unwrap();
        let geo_traits::GeometryType::GeometryCollection(collection) = collection.as_type() else {
            panic!("expected a geometry collection");
        };
        assert_eq!(collection.num_geometries(), 1);
        let child = collection.geometry(0).unwrap();
        let geo_traits::GeometryType::Point(point) = child.as_type() else {
            panic!("expected a point child");
        };
        let coord = point.coord().unwrap();
        assert_eq!(
            (coord.x(), coord.y(), coord.nth(2), coord.nth(3)),
            (2.0, 1.0, Some(3.0), Some(4.0))
        );
    }
}
