use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{CoordTrait, Dimensions};
use geoarrow_array::{GeoArrowArray, downcast_geoarrow_array};
use geoarrow_schema::error::GeoArrowResult;
use geoarrow_schema::{BoxType, CoordType, Dimension, GeoArrowType, GeometryType};

use super::geometry::{AffineTransform, geometry_value, null_array, transform_array};
use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionError;
use crate::udf::native::geometry::geometry_array_from_arrow;
use crate::udf::native::io::validate_wkb_array;
use crate::udf::native::owned_geometry::OwnedCoord;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(super) enum AffineFunction {
    Affine,
    Rotate,
    RotateX,
    RotateY,
    RotateZ,
    Scale,
    Translate,
    TransScale,
}

impl AffineFunction {
    fn name(self) -> &'static str {
        match self {
            Self::Affine => "st_affine",
            Self::Rotate => "st_rotate",
            Self::RotateX => "st_rotatex",
            Self::RotateY => "st_rotatey",
            Self::RotateZ => "st_rotatez",
            Self::Scale => "st_scale",
            Self::Translate => "st_translate",
            Self::TransScale => "st_transscale",
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct AffineTransformation {
    signature: Signature,
    function: AffineFunction,
    coord_type: CoordType,
}

impl AffineTransformation {
    pub(super) fn new(function: AffineFunction, coord_type: CoordType) -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            function,
            coord_type,
        }
    }

    fn invoke(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match self.function {
            AffineFunction::Affine => affine_impl(args, self.coord_type),
            AffineFunction::Rotate => rotate_impl(args, self.coord_type),
            AffineFunction::RotateX => rotate_axis_impl(args, self.coord_type, Axis::X),
            AffineFunction::RotateY => rotate_axis_impl(args, self.coord_type, Axis::Y),
            AffineFunction::RotateZ => rotate_axis_impl(args, self.coord_type, Axis::Z),
            AffineFunction::Scale => scale_impl(args, self.coord_type),
            AffineFunction::Translate => translate_impl(args, self.coord_type),
            AffineFunction::TransScale => trans_scale_impl(args, self.coord_type),
        }
    }
}

static DOCUMENTATION: [OnceLock<Documentation>; 8] = [const { OnceLock::new() }; 8];

impl ScalarUDFImpl for AffineTransformation {
    fn name(&self) -> &str {
        self.function.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        coerce_types(self.function, arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal(
            "ST_Affine-family return types require input field metadata".to_string(),
        ))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let output_type = output_type(args.arg_fields[0].as_ref(), self.coord_type)?;
        Ok(Arc::new(output_type.to_field("", true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.invoke(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION[self.function as usize].get_or_init(|| documentation(self.function)))
    }
}

fn documentation(function: AffineFunction) -> Documentation {
    let suffix = "Supports XY, XYZ, XYM, and XYZM coordinates. 3D matrices use Z when present, and M is preserved unless a scale factor point includes M. GeoArrow Box/Rect inputs and nested GeometryCollections are not supported.";
    match function {
        AffineFunction::Affine => Documentation::builder(
            DOC_SECTION_OTHER,
            format!("Applies a 2D or 3D affine transformation to a geometry. The 13-argument form applies the full 3x3 matrix plus XYZ offsets; the 7-argument form uses the upper-left 2x2 matrix plus XY offsets. {suffix}"),
            "ST_Affine(geom, a, b, d, e, xoff, yoff) or ST_Affine(geom, a, b, c, d, e, f, g, h, i, xoff, yoff, zoff)",
        )
        .with_argument("geom", "geometry")
        .with_argument("matrix and offsets", "affine coefficients")
        .build(),
        AffineFunction::Rotate => Documentation::builder(
            DOC_SECTION_OTHER,
            format!("Rotates a geometry counter-clockwise about an XY origin. The optional origin may be an XY point or x/y scalars. {suffix}"),
            "ST_Rotate(geom, radians), ST_Rotate(geom, radians, x0, y0), or ST_Rotate(geom, radians, origin)",
        )
        .with_argument("geom", "geometry")
        .with_argument("radians", "counter-clockwise angle in radians")
        .with_argument("origin", "optional XY origin")
        .build(),
        AffineFunction::RotateX => Documentation::builder(
            DOC_SECTION_OTHER,
            format!("Rotates a geometry about the X axis. {suffix}"),
            "ST_RotateX(geom, radians)",
        )
        .with_argument("geom", "geometry")
        .with_argument("radians", "rotation angle in radians")
        .build(),
        AffineFunction::RotateY => Documentation::builder(
            DOC_SECTION_OTHER,
            format!("Rotates a geometry about the Y axis. {suffix}"),
            "ST_RotateY(geom, radians)",
        )
        .with_argument("geom", "geometry")
        .with_argument("radians", "rotation angle in radians")
        .build(),
        AffineFunction::RotateZ => Documentation::builder(
            DOC_SECTION_OTHER,
            format!("Rotates a geometry about the Z axis. {suffix}"),
            "ST_RotateZ(geom, radians)",
        )
        .with_argument("geom", "geometry")
        .with_argument("radians", "counter-clockwise angle in radians")
        .build(),
        AffineFunction::Scale => Documentation::builder(
            DOC_SECTION_OTHER,
            format!("Scales a geometry by X/Y, X/Y/Z, or point-defined X/Y/Z/M factors, optionally around an origin. A geometry factor must be a non-empty point. {suffix}"),
            "ST_Scale(geom, x, y), ST_Scale(geom, x, y, z), ST_Scale(geom, factor_point), or ST_Scale(geom, factor_point, origin)",
        )
        .with_argument("geom", "geometry")
        .with_argument("factors", "numeric factors or an XY/XYZ/XYM/XYZM point")
        .with_argument("origin", "optional scale origin")
        .build(),
        AffineFunction::Translate => Documentation::builder(
            DOC_SECTION_OTHER,
            format!("Translates a geometry by XY or XYZ offsets. {suffix}"),
            "ST_Translate(geom, dx, dy) or ST_Translate(geom, dx, dy, dz)",
        )
        .with_argument("geom", "geometry")
        .with_argument("offsets", "translation offsets")
        .build(),
        AffineFunction::TransScale => Documentation::builder(
            DOC_SECTION_OTHER,
            format!("Translates by deltaX/deltaY and then scales by X/Y, matching the PostGIS 2D ST_TransScale formula. Z and M are preserved. {suffix}"),
            "ST_TransScale(geom, deltaX, deltaY, xFactor, yFactor)",
        )
        .with_argument("geom", "geometry")
        .with_argument("deltaX", "X translation before scaling")
        .with_argument("deltaY", "Y translation before scaling")
        .with_argument("xFactor", "X scale factor")
        .with_argument("yFactor", "Y scale factor")
        .build(),
    }
}

#[derive(Debug, Clone, Copy)]
enum ArgKind {
    Geometry,
    Float,
}

fn coerce_types(function: AffineFunction, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    let roles = argument_roles(function, arg_types)?;
    roles
        .into_iter()
        .zip(arg_types)
        .map(|(role, data_type)| match role {
            ArgKind::Geometry if *data_type == DataType::Null => Ok(DataType::Null),
            ArgKind::Geometry if is_supported_geometry_type(data_type) => Ok(data_type.clone()),
            ArgKind::Geometry => Err(DataFusionError::Plan(format!(
                "{} does not support argument type {data_type}",
                function.name()
            ))),
            ArgKind::Float if data_type.is_numeric() || *data_type == DataType::Null => {
                Ok(DataType::Float64)
            }
            ArgKind::Float => Err(DataFusionError::Plan(format!(
                "{} numeric arguments must be numeric, got {data_type}",
                function.name()
            ))),
        })
        .collect()
}

fn argument_roles(function: AffineFunction, arg_types: &[DataType]) -> Result<Vec<ArgKind>> {
    use ArgKind::{Float, Geometry};

    let arity_error = || {
        DataFusionError::Plan(format!(
            "{} does not accept {} arguments",
            function.name(),
            arg_types.len()
        ))
    };

    let roles = match (function, arg_types.len()) {
        (AffineFunction::Affine, 7) => {
            vec![Geometry, Float, Float, Float, Float, Float, Float]
        }
        (AffineFunction::Affine, 13) => vec![
            Geometry, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float, Float,
            Float,
        ],
        (AffineFunction::Rotate, 2) => vec![Geometry, Float],
        (AffineFunction::Rotate, 3) => vec![Geometry, Float, Geometry],
        (AffineFunction::Rotate, 4) => vec![Geometry, Float, Float, Float],
        (AffineFunction::RotateX | AffineFunction::RotateY | AffineFunction::RotateZ, 2) => {
            vec![Geometry, Float]
        }
        (AffineFunction::Scale, 2) => vec![Geometry, Geometry],
        (AffineFunction::Scale, 3)
            if is_numeric_or_null(&arg_types[1]) && is_numeric_or_null(&arg_types[2]) =>
        {
            vec![Geometry, Float, Float]
        }
        (AffineFunction::Scale, 3)
            if is_geometry_or_null(&arg_types[1]) && is_geometry_or_null(&arg_types[2]) =>
        {
            vec![Geometry, Geometry, Geometry]
        }
        (AffineFunction::Scale, 4) => vec![Geometry, Float, Float, Float],
        (AffineFunction::Translate, 3) => vec![Geometry, Float, Float],
        (AffineFunction::Translate, 4) => vec![Geometry, Float, Float, Float],
        (AffineFunction::TransScale, 5) => vec![Geometry, Float, Float, Float, Float],
        _ => return Err(arity_error()),
    };
    Ok(roles)
}

fn is_numeric_or_null(data_type: &DataType) -> bool {
    data_type.is_numeric() || *data_type == DataType::Null
}

fn is_geometry_or_null(data_type: &DataType) -> bool {
    *data_type == DataType::Null || is_supported_geometry_type(data_type)
}

static AFFINE_GEOMETRY_TYPES: LazyLock<Vec<DataType>> = LazyLock::new(|| {
    let boxes = [
        Dimension::XY,
        Dimension::XYZ,
        Dimension::XYM,
        Dimension::XYZM,
    ]
    .map(|dimension| BoxType::new(dimension, Default::default()).data_type());

    any_geometry_type()
        .into_iter()
        .filter(|data_type| !boxes.contains(data_type))
        .collect()
});

fn is_supported_geometry_type(data_type: &DataType) -> bool {
    AFFINE_GEOMETRY_TYPES.contains(data_type)
}

fn output_type(input_field: &arrow_schema::Field, coord_type: CoordType) -> Result<GeoArrowType> {
    if input_field.data_type() == &DataType::Null {
        return Ok(GeoArrowType::Geometry(
            GeometryType::new(Default::default()).with_coord_type(coord_type),
        ));
    }

    let input_type = lift_geo(GeoArrowType::from_arrow_field(input_field))?;
    let output_type = match input_type {
        GeoArrowType::Point(typ) => GeoArrowType::Point(typ),
        GeoArrowType::LineString(typ) => GeoArrowType::LineString(typ),
        GeoArrowType::Polygon(typ) => GeoArrowType::Polygon(typ),
        GeoArrowType::MultiPoint(typ) => GeoArrowType::MultiPoint(typ),
        GeoArrowType::MultiLineString(typ) => GeoArrowType::MultiLineString(typ),
        GeoArrowType::MultiPolygon(typ) => GeoArrowType::MultiPolygon(typ),
        GeoArrowType::GeometryCollection(typ) => GeoArrowType::GeometryCollection(typ),
        GeoArrowType::Geometry(typ) => GeoArrowType::Geometry(typ),
        GeoArrowType::Wkb(typ) => GeoArrowType::Geometry(
            GeometryType::new(typ.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::LargeWkb(typ) => GeoArrowType::Geometry(
            GeometryType::new(typ.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::WkbView(typ) => GeoArrowType::Geometry(
            GeometryType::new(typ.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::Wkt(typ) => GeoArrowType::Geometry(
            GeometryType::new(typ.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::LargeWkt(typ) => GeoArrowType::Geometry(
            GeometryType::new(typ.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::WktView(typ) => GeoArrowType::Geometry(
            GeometryType::new(typ.metadata().clone()).with_coord_type(coord_type),
        ),
        GeoArrowType::Rect(_) => {
            return Err(DataFusionError::Plan(
                "affine transformations do not support GeoArrow Box/Rect arrays".to_string(),
            ));
        }
    };
    Ok(output_type)
}

fn affine_impl(args: ScalarFunctionArgs, coord_type: CoordType) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() != 7 && arrays.len() != 13 {
        return Err(DataFusionError::Plan(format!(
            "st_affine expects 7 or 13 arguments, got {}",
            arrays.len()
        )));
    }

    let values = arrays[1..]
        .iter()
        .map(|array| float_array(array.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    let mut transforms = Vec::with_capacity(arrays[0].len());
    for index in 0..arrays[0].len() {
        if let Some(values) = float_values(&values, index) {
            let transform = if values.len() == 6 {
                AffineTransform {
                    a: values[0],
                    b: values[1],
                    c: 0.0,
                    d: values[2],
                    e: values[3],
                    f: 0.0,
                    g: 0.0,
                    h: 0.0,
                    i: 1.0,
                    xoff: values[4],
                    yoff: values[5],
                    zoff: 0.0,
                    m_factor: 1.0,
                }
            } else {
                AffineTransform {
                    a: values[0],
                    b: values[1],
                    c: values[2],
                    d: values[3],
                    e: values[4],
                    f: values[5],
                    g: values[6],
                    h: values[7],
                    i: values[8],
                    xoff: values[9],
                    yoff: values[10],
                    zoff: values[11],
                    m_factor: 1.0,
                }
            };
            transforms.push(Some(transform));
        } else {
            transforms.push(None);
        }
    }
    execute(
        &arrays[0],
        &args.arg_fields[0],
        &args.return_field,
        &transforms,
        coord_type,
    )
}

fn rotate_impl(args: ScalarFunctionArgs, coord_type: CoordType) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let angle = float_array(arrays[1].as_ref())?;
    let mut transforms = Vec::with_capacity(arrays[0].len());

    match arrays.len() {
        2 => {
            for index in 0..arrays[0].len() {
                transforms.push(float_value(angle, index).map(|angle| AffineTransform {
                    a: angle.cos(),
                    b: -angle.sin(),
                    c: 0.0,
                    d: angle.sin(),
                    e: angle.cos(),
                    f: 0.0,
                    g: 0.0,
                    h: 0.0,
                    i: 1.0,
                    xoff: 0.0,
                    yoff: 0.0,
                    zoff: 0.0,
                    m_factor: 1.0,
                }));
            }
        }
        3 => {
            if arrays[2].data_type() == &DataType::Null {
                transforms.resize(arrays[0].len(), None);
                return execute(
                    &arrays[0],
                    &args.arg_fields[0],
                    &args.return_field,
                    &transforms,
                    coord_type,
                );
            }
            let origins = geometry_array_from_arrow(arrays[2].as_ref(), &args.arg_fields[2])?;
            for index in 0..arrays[0].len() {
                if row_has_null(&arrays, index) {
                    transforms.push(None);
                    continue;
                }
                let Some(angle) = float_value(angle, index) else {
                    transforms.push(None);
                    continue;
                };
                let origin = lift_geo(geometry_value(origins.as_ref(), index))?;
                let transform = match origin {
                    None => None,
                    Some(origin) => match origin.as_point() {
                        None => {
                            return Err(DataFusionError::Execution(
                                "ST_Rotate origin must be a POINT".to_string(),
                            ));
                        }
                        Some(None) => None,
                        Some(Some(coord)) => Some(rotation(angle, Axis::Z, coord.x(), coord.y())),
                    },
                };
                transforms.push(transform);
            }
        }
        4 => {
            let x0 = float_array(arrays[2].as_ref())?;
            let y0 = float_array(arrays[3].as_ref())?;
            for index in 0..arrays[0].len() {
                transforms.push(
                    float_value(angle, index)
                        .zip(float_value(x0, index))
                        .zip(float_value(y0, index))
                        .map(|((angle, x0), y0)| rotation(angle, Axis::Z, x0, y0)),
                );
            }
        }
        _ => {
            return Err(DataFusionError::Plan(format!(
                "st_rotate expects 2, 3, or 4 arguments, got {}",
                arrays.len()
            )));
        }
    }

    execute(
        &arrays[0],
        &args.arg_fields[0],
        &args.return_field,
        &transforms,
        coord_type,
    )
}

#[derive(Debug, Clone, Copy)]
enum Axis {
    X,
    Y,
    Z,
}

fn rotate_axis_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
    axis: Axis,
) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let angle = float_array(arrays[1].as_ref())?;
    let transforms = (0..arrays[0].len())
        .map(|index| float_value(angle, index).map(|angle| rotation(angle, axis, 0.0, 0.0)))
        .collect::<Vec<_>>();
    execute(
        &arrays[0],
        &args.arg_fields[0],
        &args.return_field,
        &transforms,
        coord_type,
    )
}

fn rotation(angle: f64, axis: Axis, x0: f64, y0: f64) -> AffineTransform {
    let cos = angle.cos();
    let sin = angle.sin();
    match axis {
        Axis::X => AffineTransform {
            a: 1.0,
            b: 0.0,
            c: 0.0,
            d: 0.0,
            e: cos,
            f: -sin,
            g: 0.0,
            h: sin,
            i: cos,
            xoff: 0.0,
            yoff: 0.0,
            zoff: 0.0,
            m_factor: 1.0,
        },
        Axis::Y => AffineTransform {
            a: cos,
            b: 0.0,
            c: sin,
            d: 0.0,
            e: 1.0,
            f: 0.0,
            g: -sin,
            h: 0.0,
            i: cos,
            xoff: 0.0,
            yoff: 0.0,
            zoff: 0.0,
            m_factor: 1.0,
        },
        Axis::Z => AffineTransform {
            a: cos,
            b: -sin,
            c: 0.0,
            d: sin,
            e: cos,
            f: 0.0,
            g: 0.0,
            h: 0.0,
            i: 1.0,
            xoff: x0 - cos * x0 + sin * y0,
            yoff: y0 - sin * x0 - cos * y0,
            zoff: 0.0,
            m_factor: 1.0,
        },
    }
}

fn scale_impl(args: ScalarFunctionArgs, coord_type: CoordType) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let scale_from_numeric = match arrays.len() {
        3 => is_numeric_or_null(arrays[1].data_type()) && is_numeric_or_null(arrays[2].data_type()),
        4 => true,
        _ => false,
    };

    let transforms = if scale_from_numeric {
        let factors = arrays[1..]
            .iter()
            .map(|array| float_array(array.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        (0..arrays[0].len())
            .map(|index| {
                float_values(&factors, index).map(|factors| {
                    scale_transform(
                        factors[0],
                        factors[1],
                        factors.get(2).copied().unwrap_or(1.0),
                        1.0,
                        None,
                    )
                })
            })
            .collect::<Vec<_>>()
    } else {
        if arrays[1].data_type() == &DataType::Null
            || (arrays.len() == 3 && arrays[2].data_type() == &DataType::Null)
        {
            return execute(
                &arrays[0],
                &args.arg_fields[0],
                &args.return_field,
                &vec![None; arrays[0].len()],
                coord_type,
            );
        }
        let factors = geometry_array_from_arrow(arrays[1].as_ref(), &args.arg_fields[1])?;
        let origins = (arrays.len() == 3)
            .then(|| geometry_array_from_arrow(arrays[2].as_ref(), &args.arg_fields[2]))
            .transpose()?;
        let mut transforms = Vec::with_capacity(arrays[0].len());
        for index in 0..arrays[0].len() {
            if row_has_null(&arrays, index) {
                transforms.push(None);
                continue;
            }
            let factor = lift_geo(geometry_value(factors.as_ref(), index))?;
            let factor = match factor {
                None => {
                    transforms.push(None);
                    continue;
                }
                Some(factor) => match factor.as_point() {
                    Some(Some(coord)) => coord,
                    Some(None) => {
                        return Err(DataFusionError::Execution(
                            "ST_Scale factor point must not be empty".to_string(),
                        ));
                    }
                    None => {
                        return Err(DataFusionError::Execution(
                            "ST_Scale factor geometry parameter must be a POINT".to_string(),
                        ));
                    }
                },
            };
            let z_factor = match factor.dim() {
                Dimensions::Xyz | Dimensions::Xyzm => factor.nth(2).unwrap(),
                Dimensions::Xy | Dimensions::Xym => 1.0,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unsupported ST_Scale factor dimensions: {other:?}"
                    )));
                }
            };
            let m_factor = match factor.dim() {
                Dimensions::Xym => factor.nth(2).unwrap(),
                Dimensions::Xyzm => factor.nth(3).unwrap(),
                Dimensions::Xy | Dimensions::Xyz => 1.0,
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "unsupported ST_Scale factor dimensions: {other:?}"
                    )));
                }
            };
            let origin = match origins.as_ref() {
                Some(origins) => match lift_geo(geometry_value(origins.as_ref(), index))?
                    .and_then(|origin| origin.as_point())
                {
                    Some(Some(origin)) => Some(origin),
                    _ => None,
                },
                None => None,
            };
            transforms.push(Some(scale_transform(
                factor.x(),
                factor.y(),
                z_factor,
                m_factor,
                origin,
            )));
        }
        transforms
    };

    execute(
        &arrays[0],
        &args.arg_fields[0],
        &args.return_field,
        &transforms,
        coord_type,
    )
}

fn scale_transform(
    x_factor: f64,
    y_factor: f64,
    z_factor: f64,
    m_factor: f64,
    origin: Option<OwnedCoord>,
) -> AffineTransform {
    let (xoff, yoff, zoff) = origin
        .map(|origin| {
            let z = if matches!(origin.dim(), Dimensions::Xyz | Dimensions::Xyzm) {
                origin.nth(2).unwrap()
            } else {
                0.0
            };
            (
                origin.x() * (1.0 - x_factor),
                origin.y() * (1.0 - y_factor),
                z * (1.0 - z_factor),
            )
        })
        .unwrap_or((0.0, 0.0, 0.0));

    AffineTransform {
        a: x_factor,
        b: 0.0,
        c: 0.0,
        d: 0.0,
        e: y_factor,
        f: 0.0,
        g: 0.0,
        h: 0.0,
        i: z_factor,
        xoff,
        yoff,
        zoff,
        m_factor,
    }
}

fn translate_impl(args: ScalarFunctionArgs, coord_type: CoordType) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let offsets = arrays[1..]
        .iter()
        .map(|array| float_array(array.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    let transforms = (0..arrays[0].len())
        .map(|index| {
            float_values(&offsets, index).map(|offsets| AffineTransform {
                xoff: offsets[0],
                yoff: offsets[1],
                zoff: offsets.get(2).copied().unwrap_or(0.0),
                ..AffineTransform::identity()
            })
        })
        .collect::<Vec<_>>();
    execute(
        &arrays[0],
        &args.arg_fields[0],
        &args.return_field,
        &transforms,
        coord_type,
    )
}

fn trans_scale_impl(args: ScalarFunctionArgs, coord_type: CoordType) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let values = arrays[1..]
        .iter()
        .map(|array| float_array(array.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    let transforms = (0..arrays[0].len())
        .map(|index| {
            float_values(&values, index).map(|values| AffineTransform {
                a: values[2],
                b: 0.0,
                c: 0.0,
                d: 0.0,
                e: values[3],
                f: 0.0,
                g: 0.0,
                h: 0.0,
                i: 1.0,
                xoff: values[0] * values[2],
                yoff: values[1] * values[3],
                zoff: 0.0,
                m_factor: 1.0,
            })
        })
        .collect::<Vec<_>>();
    execute(
        &arrays[0],
        &args.arg_fields[0],
        &args.return_field,
        &transforms,
        coord_type,
    )
}

fn execute(
    source: &ArrayRef,
    source_field: &FieldRef,
    return_field: &FieldRef,
    transforms: &[Option<AffineTransform>],
    coord_type: CoordType,
) -> Result<ColumnarValue> {
    validate_wkb_array(source.as_ref(), source_field.as_ref())?;
    let output_type = output_type(return_field.as_ref(), coord_type)?;
    let output = if source.data_type() == &DataType::Null {
        lift_geo(null_array(&output_type, source.len()))?
    } else {
        let geo_array = geometry_array_from_arrow(source.as_ref(), source_field)?;
        let array = geo_array.as_ref();
        lift_geo(downcast_geoarrow_array!(
            array,
            transform_array,
            transforms,
            output_type
        ))?
    };
    Ok(ColumnarValue::Array(output.to_array_ref()))
}

fn float_array(array: &dyn Array) -> Result<&arrow_array::PrimitiveArray<Float64Type>> {
    array.as_any().downcast_ref().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "affine numeric argument was not coerced to Float64: {}",
            array.data_type()
        ))
    })
}

fn float_values(
    arrays: &[&arrow_array::PrimitiveArray<Float64Type>],
    index: usize,
) -> Option<Vec<f64>> {
    arrays
        .iter()
        .map(|array| float_value(array, index))
        .collect::<Option<Vec<_>>>()
}

fn float_value(array: &arrow_array::PrimitiveArray<Float64Type>, index: usize) -> Option<f64> {
    (!array.is_null(index)).then(|| array.value(index))
}

fn row_has_null(arrays: &[ArrayRef], index: usize) -> bool {
    arrays.iter().any(|array| array.is_null(index))
}

fn lift_geo<T>(result: GeoArrowResult<T>) -> Result<T> {
    result.map_err(|err| {
        let err: DataFusionError = GeoDataFusionError::from(err).into();
        err
    })
}
