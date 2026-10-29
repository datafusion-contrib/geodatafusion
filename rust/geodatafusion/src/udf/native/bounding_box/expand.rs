use std::sync::{Arc, LazyLock, OnceLock};

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{ArrayRef, PrimitiveArray};
use arrow_schema::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use geo_traits::{CoordTrait, Dimensions, RectTrait};
use geoarrow_array::array::{GeometryArray, RectArray};
use geoarrow_array::builder::RectBuilder;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_schema::{BoxType, CoordType, Dimension, GeometryType};
use wkt::types::{
    Coord as WktCoord, Dimension as WktDimension, LineString as WktLineString,
    Polygon as WktPolygon,
};

use crate::data_types::any_geometry_type;
use crate::error::GeoDataFusionResult;
use crate::udf::native::bounding_box::util::bounds::{
    ExtentBounds, GeometryExtent, geometry_extent,
};
use crate::udf::native::geometry::{
    StrictGeometryBuilder, field_metadata, field_metadata_or_default, geometry_array_from_arrow,
    is_null_geometry_input, null_geometry_array,
};

fn box_data_type(dimension: Dimension) -> DataType {
    BoxType::new(dimension, Default::default()).data_type()
}

fn is_box_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Struct(fields)
            if fields.first().is_some_and(|field| field.name() == "xmin")
    )
}

static SIGNATURE: LazyLock<Signature> =
    LazyLock::new(|| Signature::user_defined(Volatility::Immutable));

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct Expand {
    coord_type: CoordType,
}

impl Expand {
    pub fn new(coord_type: CoordType) -> Self {
        Self { coord_type }
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for Expand {
    fn name(&self) -> &str {
        "st_expand"
    }

    fn signature(&self) -> &Signature {
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(return_field_impl(args, self.coord_type)?)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(expand_impl(args, self.coord_type)?)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=5).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(format!(
                "ST_Expand expects between 2 and 5 arguments, got {}",
                arg_types.len()
            )));
        }

        let input_type = &arg_types[0];
        let box_types = [box_data_type(Dimension::XY), box_data_type(Dimension::XYZ)];
        let is_box = box_types.contains(input_type);
        if is_box {
            // PostGIS has box overloads for box2d + 1/2 distances and box3d +
            // 1/2/3 distances. Larger calls resolve through its geometry
            // overload, which returns a polygon; this typed entry point keeps
            // its box return type and rejects those shapes explicitly.
            let max_distances = if input_type == &box_types[0] { 2 } else { 3 };
            let distances = arg_types.len() - 1;
            if distances > max_distances {
                return Err(DataFusionError::Plan(format!(
                    "ST_Expand box inputs accept at most {max_distances} distances, got {distances}; \
                     PostGIS resolves larger box calls through its geometry overload"
                )));
            }
        }
        let is_geometry = any_geometry_type()
            .iter()
            .any(|data_type| data_type == input_type && !is_box_type(data_type));
        if !is_box && !is_geometry && input_type != &DataType::Null {
            return Err(DataFusionError::Plan(format!(
                "ST_Expand does not support input type {input_type}"
            )));
        }

        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(input_type.clone());
        for data_type in &arg_types[1..] {
            if data_type.is_numeric() || *data_type == DataType::Null {
                coerced.push(DataType::Float64);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "ST_Expand expects numeric distances, got {data_type}"
                )));
            }
        }
        Ok(coerced)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a bounding box or polygonal geometry expanded by the given distance in each axis. Empty geometry inputs are returned unchanged.",
                "ST_Expand(geometry, dx, dy[, dz[, dm]])",
            )
            .with_argument("geometry", "geometry or box")
            .with_argument("dx", "float")
            .with_argument("dy", "float")
            .with_argument("dz", "float")
            .with_argument("dm", "float")
            .build()
        }))
    }
}

fn return_field_impl(
    args: ReturnFieldArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<FieldRef> {
    let input_field = args.arg_fields[0].as_ref();
    if let Ok(box_type) = input_field.try_extension_type::<BoxType>() {
        return Ok(Arc::new(box_type.to_field("", true)));
    }

    let metadata = field_metadata(input_field)?;
    let output_type = GeometryType::new(metadata).with_coord_type(coord_type);
    Ok(Arc::new(output_type.to_field("", true)))
}

fn expand_impl(
    args: ScalarFunctionArgs,
    coord_type: CoordType,
) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if let Ok(box_type) = args.arg_fields[0].try_extension_type::<BoxType>() {
        expand_box(&arrays, box_type)
    } else {
        expand_geometry(&arrays, coord_type, &args.arg_fields[0])
    }
}

fn expand_box(arrays: &[ArrayRef], box_type: BoxType) -> GeoDataFusionResult<ColumnarValue> {
    let rects = RectArray::try_from((arrays[0].as_ref(), box_type.clone()))?;
    let dx = arrays[1].as_primitive::<Float64Type>();
    let dy = arrays
        .get(2)
        .map(|array| array.as_primitive::<Float64Type>());
    let dz = arrays
        .get(3)
        .map(|array| array.as_primitive::<Float64Type>());
    let mut builder = RectBuilder::with_capacity(
        BoxType::new(box_type.dimension(), box_type.metadata().clone()),
        rects.len(),
    );

    for (index, rect) in rects.iter().enumerate() {
        let Some(rect) = rect.transpose()? else {
            builder.push_null();
            continue;
        };
        let Some(dx) = dx.iter().nth(index).flatten() else {
            builder.push_null();
            continue;
        };
        let dy = optional_value(dy, index);
        let dz = optional_value(dz, index);
        if arrays.len() >= 3 && dy.is_none() {
            builder.push_null();
            continue;
        }
        if arrays.len() == 4 && dz.is_none() {
            builder.push_null();
            continue;
        }

        let (dx, dy, dz) = match arrays.len() {
            2 => (dx, dx, dx),
            3 => (dx, dy.unwrap_or_default(), 0.0),
            4 => (dx, dy.unwrap_or_default(), dz.unwrap_or_default()),
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "ST_Expand received {} arguments for a box input",
                    arrays.len()
                ))
                .into());
            }
        };

        let min = rect.min();
        let max = rect.max();
        let xmin = min.x() - dx;
        let xmax = max.x() + dx;
        let ymin = min.y() - dy;
        let ymax = max.y() + dy;
        let (zmin, zmax) = if box_type.dimension() == Dimension::XYZ {
            let zmin = min.nth_or_panic(2) - dz;
            let zmax = max.nth_or_panic(2) + dz;
            (Some(zmin), Some(zmax))
        } else {
            (None, None)
        };

        // PostGIS normalizes box bounds after a negative expansion, while the
        // geometry overload preserves the inverted polygon.
        let lower = WktCoord {
            x: xmin.min(xmax),
            y: ymin.min(ymax),
            z: zmin.zip(zmax).map(|(zmin, zmax)| zmin.min(zmax)),
            m: None,
        };
        let upper = WktCoord {
            x: xmin.max(xmax),
            y: ymin.max(ymax),
            z: zmin.zip(zmax).map(|(zmin, zmax)| zmin.max(zmax)),
            m: None,
        };
        builder.push_min_max(&lower, &upper);
    }

    Ok(builder.finish().into_array_ref().into())
}

fn optional_value(
    array: Option<&arrow_array::PrimitiveArray<Float64Type>>,
    index: usize,
) -> Option<f64> {
    array.and_then(|array| array.iter().nth(index).flatten())
}

fn expand_geometry(
    arrays: &[ArrayRef],
    coord_type: CoordType,
    input_field: &FieldRef,
) -> GeoDataFusionResult<ColumnarValue> {
    if is_null_geometry_input(input_field.as_ref(), arrays[0].as_ref()) {
        let output_type = GeometryType::new(field_metadata_or_default(input_field.as_ref())?)
            .with_coord_type(coord_type);
        return Ok(null_geometry_array(output_type, arrays[0].len())?.into());
    }
    let geo_array = geometry_array_from_arrow(&arrays[0], input_field)?;
    let output_type =
        GeometryType::new(geo_array.data_type().metadata().clone()).with_coord_type(coord_type);
    let factors = arrays[1..]
        .iter()
        .map(|array| array.as_primitive::<Float64Type>())
        .collect::<Vec<_>>();
    let geo_array_ref = geo_array.as_ref();
    let output =
        downcast_geoarrow_array!(geo_array_ref, expand_geometry_array, &output_type, &factors)?;
    Ok(output.into_array_ref().into())
}

fn expand_geometry_array<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    output_type: &GeometryType,
    factors: &[&PrimitiveArray<Float64Type>],
) -> GeoDataFusionResult<GeometryArray> {
    let mut builder = StrictGeometryBuilder::new(output_type.clone());

    for (index, geometry) in array.iter().enumerate() {
        let factor_values = factors
            .iter()
            .map(|factor| factor.iter().nth(index).flatten())
            .collect::<Option<Vec<_>>>();
        let Some(factor_values) = factor_values else {
            builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            continue;
        };
        let Some(geometry) = geometry.transpose()? else {
            builder.push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            continue;
        };

        match geometry_extent(&geometry) {
            GeometryExtent::Empty => builder.push_geometry(Some(&geometry))?,
            GeometryExtent::NonFinite => {
                return Err(DataFusionError::Execution(
                    "ST_Expand does not support non-finite coordinates".to_string(),
                )
                .into());
            }
            GeometryExtent::Finite(bounds) => {
                let expanded = expanded_polygon(&bounds, &factor_values)?;
                builder.push_geometry(Some(&expanded))?;
            }
        }
    }

    Ok(builder.finish())
}

fn expanded_polygon(
    bounds: &ExtentBounds,
    factors: &[f64],
) -> GeoDataFusionResult<WktPolygon<f64>> {
    let (dx, dy, dz, dm) = match factors {
        [distance] => (*distance, *distance, *distance, *distance),
        [dx, dy] => (*dx, *dy, 0.0, 0.0),
        [dx, dy, dz] => (*dx, *dy, *dz, 0.0),
        [dx, dy, dz, dm] => (*dx, *dy, *dz, *dm),
        _ => {
            return Err(DataFusionError::Internal(format!(
                "ST_Expand received {} numeric arguments",
                factors.len()
            ))
            .into());
        }
    };

    let wkt_dimension = match bounds.dimensions() {
        Dimensions::Xy => WktDimension::XY,
        Dimensions::Xyz => WktDimension::XYZ,
        Dimensions::Xym => WktDimension::XYM,
        Dimensions::Xyzm => WktDimension::XYZM,
        Dimensions::Unknown(size) => {
            return Err(DataFusionError::NotImplemented(format!(
                "ST_Expand does not support geometries with {size} physical dimensions"
            ))
            .into());
        }
    };
    let has_z = matches!(wkt_dimension, WktDimension::XYZ | WktDimension::XYZM);
    let has_m = matches!(wkt_dimension, WktDimension::XYM | WktDimension::XYZM);
    let min = bounds.min();
    let max = bounds.max();
    let (xmin, ymin, zmin, mmin) = (min[0] - dx, min[1] - dy, min[2] - dz, min[3] - dm);
    let (xmax, ymax, zmax, mmax) = (max[0] + dx, max[1] + dy, max[2] + dz, max[3] + dm);
    let coord = |x, y, z, m| WktCoord {
        x,
        y,
        z: has_z.then_some(z),
        m: has_m.then_some(m),
    };
    let ring = WktLineString::new(
        vec![
            coord(xmin, ymin, zmin, mmin),
            coord(xmin, ymax, zmin, mmin),
            coord(xmax, ymax, zmax, mmax),
            coord(xmax, ymin, zmax, mmax),
            coord(xmin, ymin, zmin, mmin),
        ],
        wkt_dimension,
    );
    Ok(WktPolygon::new(vec![ring], wkt_dimension))
}

#[cfg(test)]
mod test {
    use datafusion::prelude::SessionContext;

    use super::*;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register(&ctx);
        ctx
    }

    #[tokio::test]
    async fn expands_2d_and_3d_boxes() {
        let ctx = ctx();
        let batches = ctx
            .sql(
                "SELECT \
                    ST_Expand(ST_MakeBox2D(ST_Point(0, 5), ST_Point(10, 20)), 10, 20) AS box2d, \
                    ST_Expand(ST_3DMakeBox(ST_PointZ(-1, 3, 5), ST_PointZ(-1, 6, 8)), 1, 2, 3) AS box3d",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &batches[0];

        let box2d =
            RectArray::try_from((batch.column(0).as_ref(), batch.schema().field(0))).unwrap();
        let rect = box2d.value(0).unwrap();
        assert_eq!(
            (
                rect.min().x(),
                rect.min().y(),
                rect.max().x(),
                rect.max().y()
            ),
            (-10.0, -15.0, 20.0, 40.0)
        );

        let box3d =
            RectArray::try_from((batch.column(1).as_ref(), batch.schema().field(1))).unwrap();
        let rect = box3d.value(0).unwrap();
        assert_eq!(
            (
                rect.min().x(),
                rect.min().y(),
                rect.min().nth_or_panic(2),
                rect.max().x(),
                rect.max().y(),
                rect.max().nth_or_panic(2)
            ),
            (-2.0, 1.0, 2.0, 0.0, 8.0, 11.0)
        );
    }

    #[tokio::test]
    async fn expands_geometry_dimensions_and_empty_inputs() {
        let ctx = ctx();
        let batches = ctx
            .sql(
                "SELECT \
                    ST_AsText(ST_Expand(ST_GeomFromText('LINESTRING M (1 2 3, 10 20 30)'), 1, 4, 2, 7)) AS line_m, \
                    ST_AsText(ST_Expand(ST_GeomFromText('POLYGON EMPTY'), 4)) AS empty",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = &batches[0];
        assert_eq!(
            batch.column(0).as_string::<i32>().value(0),
            "POLYGON M((0 -2 -4,0 24 -4,11 24 37,11 -2 37,0 -2 -4))"
        );
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "POLYGON EMPTY");
    }
}
