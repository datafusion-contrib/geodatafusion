use geo_traits::{Dimensions, GeometryTrait};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::GeometryArray;
use geoarrow_array::builder::{GeometryBuilder, GeometryCollectionBuilder};
use geoarrow_schema::error::{GeoArrowError, GeoArrowResult};
use geoarrow_schema::type_id::GeometryTypeId;
use geoarrow_schema::{Dimension, GeometryCollectionType, GeometryType};

/// Builds a generic GeoArrow geometry array without flattening collections.
///
/// GeoArrow 0.9's `GeometryBuilder` canonicalizes a one-member
/// `GeometryCollection` to its child geometry. Collections use a dedicated
/// child builder here, while all other geometries and nulls use the regular
/// builder.
#[derive(Debug)]
pub(super) struct GeometryArrayBuilder {
    inner: GeometryBuilder,
    collections: [GeometryCollectionBuilder; 4],
    collection_lengths: [usize; 4],
    collection_rows: Vec<(usize, usize, i32)>,
    len: usize,
}

impl GeometryArrayBuilder {
    pub(super) fn new(typ: GeometryType) -> Self {
        let coord_type = typ.coord_type();
        let metadata = typ.metadata().clone();
        let collections = std::array::from_fn(|index| {
            GeometryCollectionBuilder::new(
                GeometryCollectionType::new(dimension_from_index(index), Default::default())
                    .with_coord_type(coord_type),
            )
        });

        Self {
            inner: GeometryBuilder::new(GeometryType::new(metadata).with_coord_type(coord_type)),
            collections,
            collection_lengths: [0; 4],
            collection_rows: Vec::new(),
            len: 0,
        }
    }

    pub(super) fn push_geometry(
        &mut self,
        geometry: Option<&impl GeometryTrait<T = f64>>,
    ) -> GeoArrowResult<()> {
        let Some(geometry) = geometry else {
            self.inner
                .push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
            self.len += 1;
            return Ok(());
        };

        let geo_traits::GeometryType::GeometryCollection(collection) = geometry.as_type() else {
            self.inner.push_geometry(Some(geometry))?;
            self.len += 1;
            return Ok(());
        };

        let dimension_index = dimension_index(collection.dim())?;
        let collection_index = self.collection_lengths[dimension_index];

        // Keep the regular union row count aligned, then retarget this row to the collection
        // child in finish().
        self.inner
            .push_geometry(None::<&geo_traits::UnimplementedGeometry<f64>>)?;
        self.collections[dimension_index].push_geometry(Some(collection))?;
        self.collection_lengths[dimension_index] += 1;
        self.collection_rows.push((
            self.len,
            dimension_index,
            i32::try_from(collection_index).expect("geometry collection count fits in i32"),
        ));
        self.len += 1;
        Ok(())
    }

    pub(super) fn finish(self) -> GeometryArray {
        let inner = self.inner.finish();
        let mut type_ids = inner.type_ids().to_vec();
        let mut offsets = inner.offsets().to_vec();

        for (row, dimension_index, collection_index) in self.collection_rows {
            type_ids[row] = self.collections[dimension_index].geometry_type_id();
            offsets[row] = collection_index;
        }

        let dimensions = [
            Dimension::XY,
            Dimension::XYZ,
            Dimension::XYM,
            Dimension::XYZM,
        ];
        GeometryArray::new(
            type_ids.into(),
            offsets.into(),
            dimensions.map(|dim| inner.point_child(dim)),
            dimensions.map(|dim| inner.line_string_child(dim)),
            dimensions.map(|dim| inner.polygon_child(dim)),
            dimensions.map(|dim| inner.multi_point_child(dim)),
            dimensions.map(|dim| inner.multi_line_string_child(dim)),
            dimensions.map(|dim| inner.multi_polygon_child(dim)),
            self.collections.map(GeometryCollectionBuilder::finish),
            inner.data_type().metadata().clone(),
        )
    }
}

fn dimension_index(dim: Dimensions) -> GeoArrowResult<usize> {
    match dim {
        Dimensions::Xy => Ok(0),
        Dimensions::Xyz => Ok(1),
        Dimensions::Xym => Ok(2),
        Dimensions::Xyzm => Ok(3),
        Dimensions::Unknown(_) => Err(GeoArrowError::InvalidGeoArrow(format!(
            "unsupported geometry dimensions: {dim:?}"
        ))),
    }
}

fn dimension_from_index(index: usize) -> Dimension {
    match index {
        0 => Dimension::XY,
        1 => Dimension::XYZ,
        2 => Dimension::XYM,
        3 => Dimension::XYZM,
        _ => unreachable!("GeoArrow has four dimensions"),
    }
}
