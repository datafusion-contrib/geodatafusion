//! Assemble `ST_Collect_Agg` results as a GeoArrow [`GeometryArray`][geoarrow_array::array::GeometryArray].

use std::sync::Arc;

use arrow_array::ArrayRef;
use datafusion::error::DataFusionError;
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::WkbArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::capacity::{
    GeometryCapacity, GeometryCollectionCapacity, MultiLineStringCapacity, MultiPointCapacity,
    MultiPolygonCapacity,
};
use geoarrow_schema::{Dimension, GeometryType};
use wkb::Endianness;
use wkb::reader::Wkb;

use super::super::container::{CollectedGeometry, CollectedKind, classify_members};
use crate::error::GeoDataFusionResult;

/// Borrowed WKB source batches with reusable storage for one aggregate result's member readers.
struct WkbGroupReader<'a> {
    batches: &'a [Arc<WkbArray>],
    members: Vec<Wkb<'a>>,
}

impl<'a> WkbGroupReader<'a> {
    /// Read the non-null WKB rows contributing to one aggregate result, reusing the member vector.
    /// Individual readers **may** still allocate internally; see [`read_wkb_member`].
    fn read(&mut self, member_rows: &[(usize, usize)]) -> GeoDataFusionResult<&[Wkb<'a>]> {
        self.members.clear();
        for &(batch_index, row_index) in member_rows {
            let batch = &self.batches[batch_index];
            if batch.is_null(row_index) {
                continue;
            }
            let bytes = batch.inner().value(row_index);
            self.members.push(read_wkb_member(bytes)?);
        }
        Ok(&self.members)
    }
}

/// GeoArrow output prepared from WKB members for a fixed set of aggregate groups
/// (where "group" is defined by a SQL `GROUP BY` or similar).
pub(super) struct PreparedWkbOutput<'a, 'g> {
    reader: WkbGroupReader<'a>,
    aggregate_groups: &'g [&'g [(usize, usize)]],
    capacity: GeometryCapacity,
}

impl<'a, 'g> PreparedWkbOutput<'a, 'g> {
    /// Size the GeoArrow buffers needed for the supplied SQL aggregate results.
    /// Each slice lists `(batch_index, row_index)` entries for one result; indices must be valid.
    ///
    /// # Errors
    ///
    /// Returns an error if a member cannot be read or an aggregate group mixes coordinate dimensions.
    pub(super) fn prepare(
        batches: &'a [Arc<WkbArray>],
        aggregate_groups: &'g [&'g [(usize, usize)]],
    ) -> GeoDataFusionResult<Self> {
        let mut reader = WkbGroupReader {
            batches,
            members: Vec::new(),
        };

        // All aggregate result rows share the builder's output buffers.
        // Appending a later result can outgrow a buffer holding earlier results, temporarily
        // retaining both the old allocation and its replacement while copying their contents.
        // We size the complete output first to avoid that peak in memory use.
        let mut nulls = 0;

        // GeometryCapacity tracks separate child arrays for each coordinate dimension.
        // Per the GeometryCapacity::new docs, these are slices by dimension:
        // XY, XYZ, XYM, and XYZM in order.
        let mut multi_points = [MultiPointCapacity::new_empty(); 4];
        let mut multi_lines = [MultiLineStringCapacity::new_empty(); 4];
        let mut multi_polygons = [MultiPolygonCapacity::new_empty(); 4];
        let mut collections = [GeometryCollectionCapacity::new_empty(); 4];

        for member_rows in aggregate_groups {
            let members = reader.read(member_rows)?;
            if members.is_empty() {
                nulls += 1;
                continue;
            }

            let (kind, dim) = classify_members(members)?;
            let index = match Dimension::try_from(dim)? {
                Dimension::XY => 0,
                Dimension::XYZ => 1,
                Dimension::XYM => 2,
                Dimension::XYZM => 3,
            };
            let collected = CollectedGeometry::new(members, kind, dim);

            match kind {
                CollectedKind::MultiPoint => multi_points[index].add_multi_point(Some(&collected)),
                CollectedKind::MultiLineString => {
                    multi_lines[index].add_multi_line_string(Some(&collected))
                }
                CollectedKind::MultiPolygon => {
                    multi_polygons[index].add_multi_polygon(Some(&collected))
                }
                CollectedKind::GeometryCollection => {
                    collections[index].add_geometry_collection(Some(&collected))?;
                }
            }
        }

        // ST_Collect_Agg emits only MULTI* or GeometryCollection, even for a single atomic member.
        // The top-level Point, LineString, and Polygon child arrays therefore need no capacity.
        // Atomic members inside collections are counted by GeometryCollectionCapacity itself.
        // If collection classification ever emits atomic results, their capacities must be added here.
        let capacity = GeometryCapacity::new(
            nulls,
            Default::default(),
            Default::default(),
            Default::default(),
            multi_points,
            multi_lines,
            multi_polygons,
            collections,
        );
        Ok(Self {
            reader,
            aggregate_groups,
            capacity,
        })
    }

    /// Decode the WKB members into one GeoArrow geometry per SQL aggregate result, in supplied order.
    ///
    /// Aggregate groups with no non-null members yield NULL; others yield MULTI* or GeometryCollection.
    /// The output uses `out_type`'s metadata and coordinate layout.
    ///
    /// # Errors
    ///
    /// Propagates member parsing, classification, and GeoArrow builder errors.
    pub(super) fn assemble(mut self, out_type: GeometryType) -> GeoDataFusionResult<ArrayRef> {
        let mut builder = GeometryBuilder::with_capacity(out_type, self.capacity);
        for member_rows in self.aggregate_groups {
            let members = self.reader.read(member_rows)?;
            if members.is_empty() {
                builder.push_null();
                continue;
            }
            let (kind, dim) = classify_members(members)?;
            builder.push_geometry(Some(&CollectedGeometry::new(members, kind, dim)))?;
        }
        Ok(builder.finish().into_array_ref())
    }
}

// WKB byte-order markers: XDR selects big-endian, NDR selects little-endian.
const WKB_XDR: u8 = Endianness::BigEndian as u8;
const WKB_NDR: u8 = Endianness::LittleEndian as u8;
// Mask for EWKB's SRID-present bit: a four-byte SRID follows the type code when set.
const EWKB_SRID_MASK: u32 = 0x2000_0000;

/// Read WKB in either ISO or EWKB format.
///
/// Both big and little endian are supported.
/// Embedded SRIDs are rejected because GeoArrow output uses array-level CRS metadata;
/// the WKB reader would otherwise skip those SRIDs without propagating them.
///
/// Coordinate bytes are borrowed and in many successful cases
/// no allocations are needed (dependent on WKB crate behavior).
///
/// # Errors
///
/// Returns an appropriate error if the bytes are invalid or for an unsupported configuration:
///
/// - An execution error for a truncated header or invalid byte-order marker
/// - `NotImplemented` for an SRID flag in the outer header
/// - The underlying WKB reader error for other invalid bytes
fn read_wkb_member(bytes: &[u8]) -> GeoDataFusionResult<Wkb<'_>> {
    let code = match bytes {
        [WKB_XDR, a, b, c, d, ..] => u32::from_be_bytes([*a, *b, *c, *d]),
        [WKB_NDR, a, b, c, d, ..] => u32::from_le_bytes([*a, *b, *c, *d]),
        // All other prefixes are malformed, regardless of the geometry type.
        _ => {
            return Err(
                DataFusionError::Execution("Invalid ST_Collect_Agg WKB header".into()).into(),
            );
        }
    };
    if code & EWKB_SRID_MASK != 0 {
        return Err(DataFusionError::NotImplemented(
            "ST_Collect_Agg does not support embedded WKB SRIDs; use array-level CRS metadata"
                .into(),
        )
        .into());
    }
    Wkb::try_new(bytes).map_err(|err| DataFusionError::External(Box::new(err)).into())
}

#[cfg(test)]
mod test {
    use arrow_array::BinaryArray;
    use geo_traits::{
        CoordTrait, GeometryTrait, GeometryType as TraitGeometryType, MultiPointTrait, PointTrait,
    };
    use geoarrow_array::GeoArrowArrayAccessor;
    use geoarrow_array::array::GeometryArray;
    use geoarrow_array::builder::WkbBuilder;
    use geoarrow_schema::{CoordType, Metadata, WkbType};

    use super::*;

    fn point_wkb(byte_order: Endianness, code: u32, coords: &[f64]) -> Vec<u8> {
        let mut bytes = vec![byte_order as u8];
        bytes.extend_from_slice(&match byte_order {
            Endianness::BigEndian => code.to_be_bytes(),
            Endianness::LittleEndian => code.to_le_bytes(),
        });
        if code & EWKB_SRID_MASK != 0 {
            bytes.extend_from_slice(&match byte_order {
                Endianness::BigEndian => 4326u32.to_be_bytes(),
                Endianness::LittleEndian => 4326u32.to_le_bytes(),
            });
        }
        for coord in coords {
            bytes.extend_from_slice(&match byte_order {
                Endianness::BigEndian => coord.to_be_bytes(),
                Endianness::LittleEndian => coord.to_le_bytes(),
            });
        }
        bytes
    }

    fn assemble_members(
        members: &[Vec<u8>],
        coord_type: CoordType,
    ) -> GeoDataFusionResult<ArrayRef> {
        // Preserve externally supplied bytes, including malformed input, as state normalization does.
        let source = WkbArray::new(
            BinaryArray::from_iter_values(members.iter().map(Vec::as_slice)),
            Arc::new(Metadata::default()),
        );
        let entries: Vec<_> = (0..members.len()).map(|i| (0, i)).collect();
        PreparedWkbOutput::prepare(&[Arc::new(source)], &[entries.as_slice()])?
            .assemble(GeometryType::default().with_coord_type(coord_type))
    }

    #[test]
    fn wkb_output_capacity_counts_all_collected_lines() {
        // Size multiple aggregate results, counting each occurrence of a retained input row.
        // The single source row is ISO little-endian LINESTRING(0 0,1 1).
        let mut line = vec![WKB_NDR];
        line.extend_from_slice(&2u32.to_le_bytes());
        line.extend_from_slice(&2u32.to_le_bytes());
        for val in [0f64, 0., 1., 1.] {
            line.extend_from_slice(&val.to_le_bytes());
        }

        let mut source = WkbBuilder::<i32>::new(WkbType::default());
        source.push_wkb(Some(&line)).unwrap();
        let batches = [Arc::new(source.finish())];

        // The first SQL aggregate result collects the row twice; the second collects it once.
        let aggregate_groups: &[&[(usize, usize)]] = &[&[(0, 0), (0, 0)], &[(0, 0)]];
        let prepared = PreparedWkbOutput::prepare(&batches, aggregate_groups).unwrap();

        let lines = prepared.capacity.multi_line_string(Dimension::XY);
        // All three line members need their two coordinates, including repeated source rows.
        assert_eq!(
            lines.coord_capacity(),
            6,
            "three lines need two XY coordinates each"
        );
        // MultiLineStringCapacity calls its line-entry count "ring_capacity".
        assert_eq!(
            lines.ring_capacity(),
            3,
            "every line occurrence needs an entry"
        );
        // There are two output geometries, although only one source row was retained.
        assert_eq!(
            lines.geom_capacity(),
            2,
            "each SQL aggregate result produces one MultiLineString"
        );
    }

    #[test]
    fn assemble_wkb_preserves_coordinates_across_encodings() {
        // Each SQL aggregate result mixes input rows with both byte orders and ISO/EWKB encodings.
        for (iso, ewkb, dim) in [
            (1, 1, Dimension::XY),
            (1001, 0x8000_0001, Dimension::XYZ),
            (2001, 0x4000_0001, Dimension::XYM),
            (3001, 0xc000_0001, Dimension::XYZM),
        ] {
            let coords = &[1.25, -2.5, 3.75, -4.5][..dim.size()];
            let members: Vec<_> = [iso, ewkb]
                .into_iter()
                .flat_map(|code| {
                    [Endianness::LittleEndian, Endianness::BigEndian]
                        .map(|order| point_wkb(order, code, coords))
                })
                .collect();
            for coord_type in [CoordType::Interleaved, CoordType::Separated] {
                let output = assemble_members(&members, coord_type).unwrap();
                let geometry = GeometryArray::try_from((
                    output.as_ref(),
                    GeometryType::default().with_coord_type(coord_type),
                ))
                .unwrap();
                let value = geometry.value(0).unwrap();
                let TraitGeometryType::MultiPoint(points) = value.as_type() else {
                    panic!("expected MultiPoint for {dim:?} with {coord_type:?} coordinates");
                };
                assert_eq!(
                    Dimension::try_from(points.dim()).unwrap(),
                    dim,
                    "dimension preserved with {coord_type:?} coordinates"
                );
                assert_eq!(
                    points.num_points(),
                    members.len(),
                    "all {dim:?} members retained"
                );
                for (point_index, point) in points.points().enumerate() {
                    let coord = point.coord().unwrap();
                    for (i, expected) in coords.iter().enumerate() {
                        assert_eq!(
                            coord.nth(i),
                            Some(*expected),
                            "{dim:?}/{coord_type:?}: point {point_index}, ordinate {i} decoded correctly"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn assemble_wkb_rejects_embedded_srid_in_either_byte_order() {
        for order in [Endianness::LittleEndian, Endianness::BigEndian] {
            let member = point_wkb(order, 0x2000_0001, &[1., 2.]);
            let err = assemble_members(&[member], CoordType::Interleaved).unwrap_err();
            assert!(
                err.to_string().contains("SRID"),
                "{order:?} input must be rejected for its embedded SRID: {err}"
            );
        }
    }

    #[test]
    fn assemble_wkb_rejects_malformed_input() {
        for (member, reason) in [
            (vec![], "missing header"),
            (vec![1, 1], "truncated header"),
            (vec![2, 1, 0, 0, 0], "invalid byte order"),
            (vec![1, 1, 0, 0, 0], "missing coordinates"),
        ] {
            assert!(
                assemble_members(&[member], CoordType::Interleaved).is_err(),
                "malformed WKB with {reason} must return an error"
            );
        }
    }

    #[test]
    fn assemble_wkb_rejects_mixed_dimensions_across_encodings() {
        let members = [
            point_wkb(Endianness::LittleEndian, 1, &[1., 2.]),
            point_wkb(Endianness::BigEndian, 0x8000_0001, &[1., 2., 3.]),
        ];
        let err = assemble_members(&members, CoordType::Interleaved).unwrap_err();
        assert!(
            err.to_string().contains("differing coordinate dimensions"),
            "XY ISO and XYZ EWKB must be rejected for mixed dimensions: {err}"
        );
    }
}
