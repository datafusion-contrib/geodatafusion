#![cfg_attr(not(feature = "geos-3_11"), allow(dead_code))]

use std::sync::Arc;

use geo_traits::{CoordTrait, Dimensions};
use geoarrow_array::array::{GeometryCollectionArray, MultiPolygonArray};
use geoarrow_array::builder::{
    GeometryCollectionBuilder, LineStringBuilder, MultiLineStringBuilder, MultiPointBuilder,
    MultiPolygonBuilder, PointBuilder, PolygonBuilder,
};
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor};
use geoarrow_schema::GeoArrowType;
use geoarrow_schema::error::GeoArrowResult;

use crate::udf::native::geometry::{
    StrictGeometryBuilder, repair_geometry_collection_array, repair_multi_polygon_array,
};
pub(crate) use crate::udf::native::owned_geometry::OwnedGeometry;
use crate::udf::native::owned_geometry::{CoordTransform, IdentityCoordTransform, OwnedCoord};

#[derive(Debug, Clone, Copy)]
pub(crate) struct AffineTransform {
    pub a: f64,
    pub b: f64,
    pub c: f64,
    pub d: f64,
    pub e: f64,
    pub f: f64,
    pub g: f64,
    pub h: f64,
    pub i: f64,
    pub xoff: f64,
    pub yoff: f64,
    pub zoff: f64,
    pub m_factor: f64,
}

impl AffineTransform {
    pub fn identity() -> Self {
        Self {
            a: 1.0,
            b: 0.0,
            c: 0.0,
            d: 0.0,
            e: 1.0,
            f: 0.0,
            g: 0.0,
            h: 0.0,
            i: 1.0,
            xoff: 0.0,
            yoff: 0.0,
            zoff: 0.0,
            m_factor: 1.0,
        }
    }

    fn apply(&self, coord: &impl CoordTrait<T = f64>) -> OwnedCoord {
        let dim = coord.dim();
        let x = coord.x();
        let y = coord.y();
        let z = matches!(dim, Dimensions::Xyz | Dimensions::Xyzm).then(|| coord.nth(2).unwrap());
        let m = match dim {
            Dimensions::Xym => Some(coord.nth(2).unwrap()),
            Dimensions::Xyzm => Some(coord.nth(3).unwrap()),
            _ => None,
        };

        let (x_out, y_out, z_out) = if let Some(z) = z {
            (
                self.a * x + self.b * y + self.c * z + self.xoff,
                self.d * x + self.e * y + self.f * z + self.yoff,
                self.g * x + self.h * y + self.i * z + self.zoff,
            )
        } else {
            (
                self.a * x + self.b * y + self.xoff,
                self.d * x + self.e * y + self.yoff,
                0.0,
            )
        };

        OwnedCoord::new(dim, x_out, y_out, z_out, m.unwrap_or(0.0) * self.m_factor)
    }
}

struct AffineCoordTransform(AffineTransform);

impl CoordTransform for AffineCoordTransform {
    fn apply(&mut self, coord: &impl CoordTrait<T = f64>) -> OwnedCoord {
        self.0.apply(coord)
    }
}

impl OwnedGeometry {
    /// Apply the MVT tile transform and round coordinates to integers.
    pub(crate) fn mvt_transform(&self, bounds: [f64; 4], extent: f64) -> Self {
        let width = bounds[2] - bounds[0];
        let height = bounds[3] - bounds[1];
        let x_scale = extent / width;
        let y_scale = -extent / height;
        let mut transformed = Self::from_geometry(
            self,
            &mut AffineCoordTransform(AffineTransform {
                a: x_scale,
                b: 0.0,
                c: 0.0,
                d: 0.0,
                e: y_scale,
                f: 0.0,
                g: 0.0,
                h: 0.0,
                i: 1.0,
                xoff: -bounds[0] * x_scale,
                yoff: -bounds[3] * y_scale,
                zoff: 0.0,
                m_factor: 1.0,
            }),
        );
        transformed.round_xy();
        transformed
    }
}

trait OutputBuilder {
    fn push_geometry(&mut self, geometry: Option<&OwnedGeometry>) -> GeoArrowResult<()>;
    fn finish(self) -> Arc<dyn GeoArrowArray>;
}

macro_rules! impl_output_builder {
    ($ty:ty) => {
        impl OutputBuilder for $ty {
            fn push_geometry(&mut self, geometry: Option<&OwnedGeometry>) -> GeoArrowResult<()> {
                <$ty>::push_geometry(self, geometry)
            }

            fn finish(self) -> Arc<dyn GeoArrowArray> {
                Arc::new(<$ty>::finish(self))
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

impl OutputBuilder for StrictGeometryBuilder {
    fn push_geometry(&mut self, geometry: Option<&OwnedGeometry>) -> GeoArrowResult<()> {
        StrictGeometryBuilder::push_geometry(self, geometry)
    }

    fn finish(self) -> Arc<dyn GeoArrowArray> {
        Arc::new(StrictGeometryBuilder::finish(self))
    }
}

pub(super) fn transform_array<'a, A>(
    array: &'a A,
    transforms: &[Option<AffineTransform>],
    output_type: GeoArrowType,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>>
where
    A: GeoArrowArrayAccessor<'a>,
{
    match output_type {
        GeoArrowType::Point(typ) => {
            transform_with_builder(array, transforms, PointBuilder::new(typ))
        }
        GeoArrowType::LineString(typ) => {
            transform_with_builder(array, transforms, LineStringBuilder::new(typ))
        }
        GeoArrowType::Polygon(typ) => {
            transform_with_builder(array, transforms, PolygonBuilder::new(typ))
        }
        GeoArrowType::MultiPoint(typ) => {
            transform_with_builder(array, transforms, MultiPointBuilder::new(typ))
        }
        GeoArrowType::MultiLineString(typ) => {
            transform_with_builder(array, transforms, MultiLineStringBuilder::new(typ))
        }
        GeoArrowType::MultiPolygon(typ) => {
            let output = transform_with_builder(array, transforms, MultiPolygonBuilder::new(typ))?;
            let array = output
                .as_any()
                .downcast_ref::<MultiPolygonArray>()
                .expect("MultiPolygonBuilder returned a different array type");
            Ok(Arc::new(repair_multi_polygon_array(array)?))
        }
        GeoArrowType::GeometryCollection(typ) => {
            let output =
                transform_with_builder(array, transforms, GeometryCollectionBuilder::new(typ))?;
            let array = output
                .as_any()
                .downcast_ref::<GeometryCollectionArray>()
                .expect("GeometryCollectionBuilder returned a different array type");
            Ok(Arc::new(repair_geometry_collection_array(array.clone())?))
        }
        GeoArrowType::Geometry(typ) => {
            transform_with_builder(array, transforms, StrictGeometryBuilder::new(typ))
        }
        other => Err(geoarrow_schema::error::GeoArrowError::InvalidGeoArrow(
            format!("unsupported affine output type: {other:?}"),
        )),
    }
}

fn transform_with_builder<'a, A, B>(
    array: &'a A,
    transforms: &[Option<AffineTransform>],
    mut builder: B,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>>
where
    A: GeoArrowArrayAccessor<'a>,
    B: OutputBuilder,
{
    for (index, geometry) in array.iter().enumerate() {
        let transform = transforms.get(index).copied().flatten();
        match (geometry, transform) {
            (Some(geometry), Some(transform)) => {
                let geometry =
                    OwnedGeometry::from_geometry(&geometry?, &mut AffineCoordTransform(transform));
                builder.push_geometry(Some(&geometry))?;
            }
            _ => builder.push_geometry(None)?,
        }
    }
    Ok(builder.finish())
}

/// Build an all-NULL GeoArrow array matching an already-resolved return type.
///
/// Shared by the affine functions and scalar simplify functions so NULL
/// arguments keep the resolved output type instead of falling back to an
/// untyped array.
pub(crate) fn null_array(
    output_type: &GeoArrowType,
    length: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    macro_rules! empty {
        ($builder:expr) => {
            push_nulls($builder, length)
        };
    }

    match output_type {
        GeoArrowType::Point(typ) => empty!(PointBuilder::new(typ.clone())),
        GeoArrowType::LineString(typ) => empty!(LineStringBuilder::new(typ.clone())),
        GeoArrowType::Polygon(typ) => empty!(PolygonBuilder::new(typ.clone())),
        GeoArrowType::MultiPoint(typ) => empty!(MultiPointBuilder::new(typ.clone())),
        GeoArrowType::MultiLineString(typ) => {
            empty!(MultiLineStringBuilder::new(typ.clone()))
        }
        GeoArrowType::MultiPolygon(typ) => empty!(MultiPolygonBuilder::new(typ.clone())),
        GeoArrowType::GeometryCollection(typ) => {
            empty!(GeometryCollectionBuilder::new(typ.clone()))
        }
        GeoArrowType::Geometry(typ) => empty!(StrictGeometryBuilder::new(typ.clone())),
        other => Err(geoarrow_schema::error::GeoArrowError::InvalidGeoArrow(
            format!("unsupported affine output type: {other:?}"),
        )),
    }
}

fn push_nulls<B: OutputBuilder>(
    mut builder: B,
    length: usize,
) -> GeoArrowResult<Arc<dyn GeoArrowArray>> {
    for _ in 0..length {
        builder.push_geometry(None)?;
    }
    Ok(builder.finish())
}

pub(super) fn geometry_value(
    array: &dyn GeoArrowArray,
    index: usize,
) -> GeoArrowResult<Option<OwnedGeometry>> {
    geoarrow_array::downcast_geoarrow_array!(array, geometry_value_impl, index)
}

fn geometry_value_impl<'a>(
    array: &'a impl GeoArrowArrayAccessor<'a>,
    index: usize,
) -> GeoArrowResult<Option<OwnedGeometry>> {
    if array.is_null(index) {
        return Ok(None);
    }
    let geometry = array.value(index)?;
    Ok(Some(OwnedGeometry::from_geometry(
        &geometry,
        &mut IdentityCoordTransform,
    )))
}
