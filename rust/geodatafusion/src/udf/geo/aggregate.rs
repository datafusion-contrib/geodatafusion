//! Spatial aggregate functions: ST_Union_Agg, ST_Collect_Agg.

use std::any::Any;
use std::sync::OnceLock;

use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::error::Result;
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion::prelude::SessionContext;
use geo::BooleanOps;
use geoarrow_array::array::from_arrow_array;
use geoarrow_array::{GeoArrowArray, GeoArrowArrayAccessor, downcast_geoarrow_array};
use geoarrow_expr_geo::util::to_geo::geometry_to_geo;
use geoarrow_schema::GeometryType;

/// Register all aggregate spatial functions.
pub fn register(session_context: &SessionContext) {
    session_context.register_udaf(datafusion::logical_expr::AggregateUDF::from(
        UnionAgg::default(),
    ));
    session_context.register_udaf(datafusion::logical_expr::AggregateUDF::from(
        CollectAgg::default(),
    ));
}

// =========================================================================
// ST_Union_Agg
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct UnionAgg {
    signature: Signature,
}

impl UnionAgg {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for UnionAgg {
    fn default() -> Self {
        Self::new()
    }
}

static UNION_AGG_DOC: OnceLock<Documentation> = OnceLock::new();

impl AggregateUDFImpl for UnionAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_union_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Return a generic geometry type
        let geom_type = GeometryType::new(Default::default());
        Ok(geom_type.data_type())
    }

    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(UnionAccumulator { result: None }))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(UNION_AGG_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Computes the union of a set of geometries.",
                "ST_Union_Agg(geometry)",
            )
            .with_argument("geom", "geometry column")
            .build()
        }))
    }
}

#[derive(Debug)]
struct UnionAccumulator {
    result: Option<geo::MultiPolygon>,
}

impl Accumulator for UnionAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        let geom_type = GeometryType::new(Default::default());
        let field = Field::new("", geom_type.data_type(), true);

        if let Ok(geo_array) = from_arrow_array(array, &field) {
            self.process_array(geo_array.as_ref())?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        match &self.result {
            Some(mp) => {
                use wkt::ToWkt;
                let wkt_str = geo::Geometry::MultiPolygon(mp.clone()).wkt_string();
                Ok(ScalarValue::Utf8(Some(wkt_str)))
            }
            None => Ok(ScalarValue::Utf8(None)),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .result
                .as_ref()
                .map(|mp| mp.0.iter().map(|p| p.exterior().0.len() * 16).sum::<usize>())
                .unwrap_or(0)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        match &self.result {
            Some(mp) => {
                use wkt::ToWkt;
                let wkt_str = geo::Geometry::MultiPolygon(mp.clone()).wkt_string();
                Ok(vec![ScalarValue::Utf8(Some(wkt_str))])
            }
            None => Ok(vec![ScalarValue::Utf8(None)]),
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let string_array = states[0]
            .as_any()
            .downcast_ref::<arrow_array::StringArray>();
        if let Some(arr) = string_array {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let wkt_str = arr.value(i);
                    if let Ok(geom) = wkt::TryFromWkt::try_from_wkt_str(wkt_str) {
                        let geom: geo::Geometry = geom;
                        self.union_geometry(&geom);
                    }
                }
            }
        }
        Ok(())
    }
}

impl UnionAccumulator {
    fn process_array(&mut self, array: &dyn GeoArrowArray) -> Result<()> {
        fn _process_typed<'a>(
            array: &'a impl GeoArrowArrayAccessor<'a>,
        ) -> geoarrow_schema::error::GeoArrowResult<Vec<geo::Geometry>> {
            let mut geoms = Vec::new();
            for item in array.iter() {
                if let Some(geom) = item {
                    let geo_geom = geometry_to_geo(&geom?)?;
                    geoms.push(geo_geom);
                }
            }
            Ok(geoms)
        }

        let geoms: Vec<geo::Geometry> = downcast_geoarrow_array!(array, _process_typed)
            .map_err(|e| datafusion::error::DataFusionError::Internal(e.to_string()))?;

        for geom in &geoms {
            self.union_geometry(geom);
        }
        Ok(())
    }

    fn union_geometry(&mut self, geom: &geo::Geometry) {
        let mp = match geom {
            geo::Geometry::Polygon(p) => geo::MultiPolygon(vec![p.clone()]),
            geo::Geometry::MultiPolygon(mp) => mp.clone(),
            _ => return,
        };

        self.result = Some(match &self.result {
            Some(existing) => existing.union(&mp),
            None => mp,
        });
    }
}

// =========================================================================
// ST_Collect_Agg
// =========================================================================
#[derive(Debug, Eq, PartialEq, Hash)]
pub struct CollectAgg {
    signature: Signature,
}

impl CollectAgg {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl Default for CollectAgg {
    fn default() -> Self {
        Self::new()
    }
}

static COLLECT_AGG_DOC: OnceLock<Documentation> = OnceLock::new();

impl AggregateUDFImpl for CollectAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_collect"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // TODO: Return native geometry type instead of WKT. Changing to native geometry
        // requires significant refactoring of the aggregate accumulator interface
        // (state serialization, merge).
        Ok(DataType::Utf8)
    }

    fn accumulator(
        &self,
        _acc_args: datafusion::logical_expr::function::AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CollectAccumulator {
            geometries: vec![],
        }))
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(COLLECT_AGG_DOC.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Collects geometries into a GeometryCollection or appropriate Multi* type.",
                "ST_Collect(geometry)",
            )
            .with_argument("geom", "geometry column")
            .build()
        }))
    }
}

#[derive(Debug)]
struct CollectAccumulator {
    geometries: Vec<geo::Geometry>,
}

impl Accumulator for CollectAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        let geom_type = GeometryType::new(Default::default());
        let field = Field::new("", geom_type.data_type(), true);

        if let Ok(geo_array) = from_arrow_array(array, &field) {
            fn _collect_typed<'a>(
                array: &'a impl GeoArrowArrayAccessor<'a>,
            ) -> geoarrow_schema::error::GeoArrowResult<Vec<geo::Geometry>> {
                let mut geoms = Vec::new();
                for item in array.iter() {
                    if let Some(geom) = item {
                        let geo_geom = geometry_to_geo(&geom?)?;
                        geoms.push(geo_geom);
                    }
                }
                Ok(geoms)
            }

            let geo_array_ref = geo_array.as_ref();
            if let Ok(geoms) = downcast_geoarrow_array!(geo_array_ref, _collect_typed) {
                self.geometries.extend(geoms);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        use wkt::ToWkt;

        if self.geometries.is_empty() {
            return Ok(ScalarValue::Utf8(None));
        }

        let gc = geo::GeometryCollection(self.geometries.clone());
        let wkt_str = geo::Geometry::GeometryCollection(gc).wkt_string();
        Ok(ScalarValue::Utf8(Some(wkt_str)))
    }

    fn size(&self) -> usize {
        use geo::CoordsIter;
        std::mem::size_of::<Self>()
            + self.geometries.len() * std::mem::size_of::<geo::Geometry>()
            + self
                .geometries
                .iter()
                .map(|g| g.coords_count() * 16)
                .sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.evaluate().map(|v| vec![v])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let string_array = states[0]
            .as_any()
            .downcast_ref::<arrow_array::StringArray>();
        if let Some(arr) = string_array {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    let wkt_str = arr.value(i);
                    if let Ok(geom) = wkt::TryFromWkt::try_from_wkt_str(wkt_str) {
                        let geom: geo::Geometry = geom;
                        self.geometries.push(geom);
                    }
                }
            }
        }
        Ok(())
    }
}
