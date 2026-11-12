//! Point constructors

use std::sync::{Arc, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, new_null_array};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::scalar::ScalarValue;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::{PointArray, SeparatedCoordBuffer};
use geoarrow_array::builder::PointBuilder;
use geoarrow_schema::{CoordType, Crs, Dimension, Metadata, PointType};

use crate::error::GeoDataFusionResult;

/// `ST_Point` scalar UDF implementation: creates a 2D `Point` with X, Y, and optional SRID values.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Point {
    signature: Signature,
    coord_type: CoordType,
}

impl Point {
    /// Creates a new `ST_Point` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for Point {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Point {
    fn name(&self) -> &str {
        "st_point"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let typ =
            PointType::new(Dimension::XY, Default::default()).with_coord_type(self.coord_type);
        let typ = point_type_with_srid(
            typ,
            args.scalar_arguments.get(2).map(|srid| srid.cloned()),
            "ST_Point",
        )?;

        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let point_arr = create_point_array_from_args(&args, 2)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Point with the given X and Y coordinate values.",
                "ST_Point(-71.104, 42.315) or ST_Point(-71.104, 42.315, 4326)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("srid", "integer SRID value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_pointz")
            .build()
        }))
    }
}

/// `ST_PointZ` scalar UDF implementation: creates a 3D `Point` with X, Y, Z, and optional SRID values.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PointZ {
    signature: Signature,
    coord_type: CoordType,
}

impl PointZ {
    /// Creates a new `ST_PointZ` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for PointZ {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_Z_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointZ {
    fn name(&self) -> &str {
        "st_pointz"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let typ =
            PointType::new(Dimension::XYZ, Default::default()).with_coord_type(self.coord_type);
        let typ = point_type_with_srid(
            typ,
            args.scalar_arguments.get(3).map(|srid| srid.cloned()),
            "ST_PointZ",
        )?;

        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let point_arr = create_point_array_from_args(&args, 3)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_Z_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Point with the given X, Y and Z coordinate values, and optionally an SRID number.",
                "ST_PointZ(-71.104, 42.315, 3.4) or ST_PointZ(-71.104, 42.315, 3.4, 4326)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("z", "z value")
            .with_argument("srid", "integer SRID value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_point")
            .build()
        }))
    }
}

/// `ST_PointM` scalar UDF implementation: creates a `Point` with X, Y, M, and optional SRID values.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PointM {
    signature: Signature,
    coord_type: CoordType,
}

impl PointM {
    /// Creates a new `ST_PointM` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for PointM {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_M_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointM {
    fn name(&self) -> &str {
        "st_pointm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let typ =
            PointType::new(Dimension::XYM, Default::default()).with_coord_type(self.coord_type);
        let typ = point_type_with_srid(
            typ,
            args.scalar_arguments.get(3).map(|srid| srid.cloned()),
            "ST_PointM",
        )?;

        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let point_arr = create_point_array_from_args(&args, 3)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_M_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Point with the given X, Y and M coordinate values, and optionally an SRID number.",
                "ST_PointM(-71.104, 42.315, 3.4) or ST_PointM(-71.104, 42.315, 3.4, 4326)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("m", "m value")
            .with_argument("srid", "integer SRID value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_point")
            .build()
        }))
    }
}

/// `ST_PointZM` scalar UDF implementation: creates a 4D `Point` with X, Y, Z, M, and optional SRID values.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct PointZM {
    signature: Signature,
    coord_type: CoordType,
}

impl PointZM {
    /// Creates a new `ST_PointZM` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for PointZM {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static POINT_ZM_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for PointZM {
    fn name(&self) -> &str {
        "st_pointzm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let typ =
            PointType::new(Dimension::XYZM, Default::default()).with_coord_type(self.coord_type);
        let typ = point_type_with_srid(
            typ,
            args.scalar_arguments.get(4).map(|srid| srid.cloned()),
            "ST_PointZM",
        )?;

        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let point_arr = create_point_array_from_args(&args, 4)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(POINT_ZM_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a Point with the given X, Y, Z and M coordinate values, and optionally an SRID number.",
                "ST_PointZM(-71.104, 42.315, 3.4, 4.5) or ST_PointZM(-71.104, 42.315, 3.4, 4.5, 4326)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("z", "z value")
            .with_argument("m", "m value")
            .with_argument("srid", "integer SRID value")
            .with_related_udf("st_makepoint")
            .with_related_udf("st_point")
            .build()
        }))
    }
}
/// `ST_MakePoint` scalar UDF implementation: creates a 2D (`XY`), 3D (`XYZ`), or 4D (`XYZM`) `Point` geometry.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakePoint {
    signature: Signature,
    coord_type: CoordType,
}

impl MakePoint {
    /// Creates a new `ST_MakePoint` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                        DataType::Float64,
                    ]),
                ],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for MakePoint {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_POINT_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakePoint {
    fn name(&self) -> &str {
        "st_makepoint"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let dim = match args.arg_fields.len() {
            2 => Dimension::XY,
            3 => Dimension::XYZ,
            4 => Dimension::XYZM,
            _ => unreachable!(),
        };

        let typ = PointType::new(dim, Default::default()).with_coord_type(self.coord_type);
        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let point_arr = create_point_array(arrays, &args.return_field)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_POINT_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a 2D XY or 3D XYZ or 4D XYZM Point geometry. Use ST_MakePointM to make points with XYM coordinates",
                "ST_MakePoint(-71.104, 42.315)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("z", "z value")
            .with_argument("m", "m value")
            .with_related_udf("st_point")
            .with_related_udf("st_pointz")
            .with_related_udf("st_makepointm")
            .build()
        }))
    }
}

/// `ST_MakePointM` scalar UDF implementation: creates a `Point` geometry with X, Y, and M (measure) coordinates.
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct MakePointM {
    signature: Signature,
    coord_type: CoordType,
}

impl MakePointM {
    /// Creates a new `ST_MakePointM` UDF instance with the specified output [`CoordType`].
    pub fn new(coord_type: CoordType) -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
            coord_type,
        }
    }
}

impl Default for MakePointM {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

static MAKE_POINT_M_DOC: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for MakePointM {
    fn name(&self) -> &str {
        "st_makepointm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<Arc<Field>> {
        let typ =
            PointType::new(Dimension::XYM, Default::default()).with_coord_type(self.coord_type);
        Ok(typ.to_field("", true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let point_arr = create_point_array(arrays, &args.return_field)?;
        Ok(point_arr.into_array_ref().into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(MAKE_POINT_M_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Creates a point with X, Y and M (measure) ordinates. Use ST_MakePoint to make points with XY, XYZ, or XYZM coordinates.",
                "ST_MakePointM(-71.104, 42.315, 10)",
            )
            .with_argument("x", "x value")
            .with_argument("y", "y value")
            .with_argument("m", "m value")
            .with_related_udf("st_point")
            .with_related_udf("st_pointz")
            .with_related_udf("st_makepoint")
            .build()
        }))
    }
}

fn point_type_with_srid(
    mut typ: PointType,
    srid: Option<Option<ScalarValue>>,
    function_name: &str,
) -> Result<PointType> {
    match srid {
        None | Some(None) | Some(Some(ScalarValue::Int64(None))) => {}
        Some(Some(ScalarValue::Int64(Some(srid)))) => {
            let crs = Crs::from_authority_code(format!("EPSG:{srid}"));
            typ = typ.with_metadata(Arc::new(Metadata::new(crs, None)));
        }
        _ => return Err(invalid_srid_error(function_name)),
    }
    Ok(typ)
}

fn create_point_array_from_args(
    args: &ScalarFunctionArgs,
    coord_count: usize,
) -> Result<PointArray> {
    if srid_is_null(
        args,
        coord_count,
        function_name_from_dimension(args.return_field.extension_type::<PointType>().dimension()),
    )? {
        return Ok(create_null_point_array(
            &args.return_field,
            args.number_rows,
        )?);
    }

    let arrays = ColumnarValue::values_to_arrays(&args.args[..coord_count])?;
    Ok(create_point_array(arrays, &args.return_field)?)
}

fn function_name_from_dimension(dimension: Dimension) -> &'static str {
    match dimension {
        Dimension::XY => "ST_Point",
        Dimension::XYZ => "ST_PointZ",
        Dimension::XYM => "ST_PointM",
        Dimension::XYZM => "ST_PointZM",
    }
}

fn srid_is_null(args: &ScalarFunctionArgs, srid_index: usize, function_name: &str) -> Result<bool> {
    match args.args.get(srid_index) {
        None | Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(_)))) => Ok(false),
        Some(ColumnarValue::Scalar(ScalarValue::Int64(None))) => Ok(true),
        Some(ColumnarValue::Array(array)) => {
            // FFI materializes scalar arguments as arrays. Null arrays represent a
            // scalar NULL; non-null arrays must be uniform and match the field CRS.
            if array.data_type() != &DataType::Int64 {
                return Err(invalid_srid_error(function_name));
            }

            let srid = array.as_primitive::<Int64Type>();
            if srid.null_count() == srid.len() {
                return Ok(true);
            }
            if srid.null_count() != 0 {
                return Err(invalid_srid_error(function_name));
            }

            let Some(first) = srid.values().first() else {
                return Ok(false);
            };
            if srid.values().iter().any(|value| value != first) {
                return Err(invalid_srid_error(function_name));
            }

            let expected_crs = Crs::from_authority_code(format!("EPSG:{first}"));
            let point_type = args.return_field.extension_type::<PointType>();
            if point_type.metadata().crs() == &expected_crs {
                Ok(false)
            } else {
                Err(invalid_srid_error(function_name))
            }
        }
        _ => Err(invalid_srid_error(function_name)),
    }
}

fn invalid_srid_error(function_name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "{function_name} only supports SRID as a scalar integer"
    ))
}

fn create_null_point_array(
    return_field: &Field,
    number_rows: usize,
) -> GeoDataFusionResult<PointArray> {
    let typ = return_field.extension_type::<PointType>();
    let arrays = (0..typ.dimension().size())
        .map(|_| new_null_array(&DataType::Float64, number_rows))
        .collect();
    create_point_array(arrays, return_field)
}

fn create_point_array(
    arrays: Vec<ArrayRef>,
    return_field: &Field,
) -> GeoDataFusionResult<PointArray> {
    let typ = return_field.extension_type::<PointType>();
    let dimension = typ.dimension();
    let required_coords = dimension.size();
    if arrays.len() != required_coords {
        return Err(DataFusionError::Execution(format!(
            "ST_Point expects {required_coords} coordinate arguments for {dimension:?}, got {}",
            arrays.len()
        ))
        .into());
    }

    let x = arrays[0].as_primitive::<Float64Type>();
    let y = arrays[1].as_primitive::<Float64Type>();
    let z = match dimension {
        Dimension::XYZ | Dimension::XYZM => Some(arrays[2].as_primitive::<Float64Type>()),
        Dimension::XY | Dimension::XYM => None,
    };
    let m = match dimension {
        Dimension::XYM => Some(arrays[2].as_primitive::<Float64Type>()),
        Dimension::XYZM => Some(arrays[3].as_primitive::<Float64Type>()),
        Dimension::XY | Dimension::XYZ => None,
    };

    let point_arr = match typ.coord_type() {
        CoordType::Interleaved => {
            let mut builder = PointBuilder::with_capacity(typ.clone(), x.len());

            for i in 0..x.len() {
                // Null buffers can mark a slot null even when its backing values are finite.
                if x.is_null(i)
                    || y.is_null(i)
                    || z.is_some_and(|z| z.is_null(i))
                    || m.is_some_and(|m| m.is_null(i))
                {
                    builder.push_null();
                } else {
                    let coord = wkt::types::Coord {
                        x: x.value(i),
                        y: y.value(i),
                        z: z.map(|z| z.value(i)),
                        m: m.map(|m| m.value(i)),
                    };
                    builder.try_push_coord(Some(&coord))?;
                }
            }

            builder.finish()
        }
        CoordType::Separated => {
            let (_, _, metadata) = typ.into_inner();
            let mut coord_buffers = vec![x.values().clone(), y.values().clone()];
            if let Some(z) = z {
                coord_buffers.push(z.values().clone());
            }
            if let Some(m) = m {
                coord_buffers.push(m.values().clone());
            }

            let mut nulls =
                NullBuffer::union(x.logical_nulls().as_ref(), y.logical_nulls().as_ref());
            if let Some(z) = z {
                nulls = NullBuffer::union(nulls.as_ref(), z.logical_nulls().as_ref());
            }
            if let Some(m) = m {
                nulls = NullBuffer::union(nulls.as_ref(), m.logical_nulls().as_ref());
            }

            let coords = SeparatedCoordBuffer::from_vec(coord_buffers, dimension)?;
            PointArray::new(coords.into(), nulls, metadata)
        }
    };

    Ok(point_arr)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use approx::relative_eq;
    use arrow_array::{RecordBatch, create_array};
    use arrow_schema::Schema;
    use datafusion::prelude::SessionContext;
    use geo_traits::{CoordTrait, PointTrait};
    use geoarrow_array::GeoArrowArrayAccessor;

    use super::*;

    #[tokio::test]
    async fn test_st_point() {
        let ctx = SessionContext::new();

        ctx.register_udf(Point::new(CoordType::Separated).into());

        let sql_df = ctx
            .sql(r#"SELECT ST_Point(-71.104, 42.315);"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);

        let output_column = output_batch.column(0);
        let point_arr = PointArray::try_from((output_column.as_ref(), output_field)).unwrap();

        assert_eq!(point_arr.len(), 1);
        let (x, y) = point_arr.value(0).unwrap().coord().unwrap().x_y();

        assert!(relative_eq!(x, -71.104));
        assert!(relative_eq!(y, 42.315));
    }

    #[tokio::test]
    async fn test_st_point_from_table() {
        let ctx = SessionContext::new();

        ctx.register_udf(Point::new(CoordType::Separated).into());

        let x = create_array!(Float64, [-71.104]);
        let y = create_array!(Float64, [42.315]);

        let schema = Schema::new([
            Arc::new(Field::new("x", x.data_type().clone(), true)),
            Arc::new(Field::new("y", y.data_type().clone(), true)),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![x, y]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let sql_df = ctx.sql(r#"SELECT ST_Point(x, y) from t;"#).await.unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);

        // This succeeds
        assert_eq!(output_field.extension_type_name(), Some("geoarrow.point"));

        let output_column = output_batch.column(0);
        let point_arr = PointArray::try_from((output_column.as_ref(), output_field)).unwrap();

        assert_eq!(point_arr.len(), 1);
        let (x, y) = point_arr.value(0).unwrap().coord().unwrap().x_y();

        assert!(relative_eq!(x, -71.104));
        assert!(relative_eq!(y, 42.315));
    }

    #[tokio::test]
    async fn test_st_point_srid() {
        let ctx = SessionContext::new();

        ctx.register_udf(Point::new(CoordType::Separated).into());

        let x = create_array!(Float64, [-71.104]);
        let y = create_array!(Float64, [42.315]);

        let schema = Schema::new([
            Arc::new(Field::new("x", x.data_type().clone(), true)),
            Arc::new(Field::new("y", y.data_type().clone(), true)),
        ]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![x, y]).unwrap();

        ctx.register_batch("t", batch).unwrap();

        let sql_df = ctx
            .sql(r#"SELECT ST_Point(x, y, 4326) as geometry from t;"#)
            .await
            .unwrap();

        let output_batches = sql_df.collect().await.unwrap();
        assert_eq!(output_batches.len(), 1);
        let output_batch = &output_batches[0];
        let output_schema = output_batch.schema();
        let output_field = output_schema.field(0);
        let point_type = output_field.extension_type::<PointType>();
        assert_eq!(
            point_type.metadata().crs(),
            &Crs::from_authority_code("EPSG:4326".to_string())
        );
    }
}
