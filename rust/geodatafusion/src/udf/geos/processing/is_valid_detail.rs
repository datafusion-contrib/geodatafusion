use std::os::raw::c_int;
use std::ptr;
use std::sync::{Arc, OnceLock};

use arrow_array::builder::{BooleanBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::{DataType, Field, FieldRef, Fields};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::scalar_doc_sections::DOC_SECTION_OTHER;
use datafusion::logical_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::builder::GeometryBuilder;
use geoarrow_array::cast::to_wkb;
use geoarrow_schema::{GeoArrowType, GeometryType, Metadata};
use geos::sys;
use wkb::reader::Wkb;

use super::super::{adapter, raw};
use crate::error::GeoDataFusionResult;
use crate::udf::native::geometry::geometry_array_from_arrow;

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct IsValidDetail {
    signature: Signature,
}

impl IsValidDetail {
    pub fn new() -> Self {
        Self {
            signature: adapter::signature(),
        }
    }
}

impl Default for IsValidDetail {
    fn default() -> Self {
        Self::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

impl ScalarUDFImpl for IsValidDetail {
    fn name(&self) -> &str {
        "st_isvaliddetail"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        adapter::check_arg_count(arg_types, &[1, 2], self.name())?;
        adapter::check_geometry_type(&arg_types[0], 0, self.name())?;
        let mut coerced = arg_types.to_vec();
        if arg_types.len() == 2 {
            coerced[1] = adapter::coerce_int64(&arg_types[1], 1, self.name())?;
        }
        Ok(coerced)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Internal("return_type".to_string()))
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let location = adapter::geometry_return_field(&args, &[0], self.name())?;
        let location = Arc::new(
            location
                .as_ref()
                .clone()
                .with_name("location")
                .with_nullable(true),
        );
        let fields = Fields::from(vec![
            Arc::new(Field::new("valid", DataType::Boolean, false)),
            Arc::new(Field::new("reason", DataType::Utf8, true)),
            location,
        ]);
        Ok(Arc::new(Field::new("", DataType::Struct(fields), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(is_valid_detail_impl(&args)?)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(DOCUMENTATION.get_or_init(|| {
            Documentation::builder(
                DOC_SECTION_OTHER,
                "Returns a validity record with `valid`, `reason`, and `location` fields. The optional flags argument controls GEOS validity checking; flag 1 permits self-touching rings to form a hole. The location is a 2D point when GEOS can report one.",
                "ST_IsValidDetail(geom, flags)",
            )
            .with_argument("geom", "geometry")
            .with_argument("flags", "optional integer GEOS validity flags")
            .build()
        }))
    }
}

fn is_valid_detail_impl(args: &ScalarFunctionArgs) -> GeoDataFusionResult<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    let row_count = arrays.first().map_or(0, |array| array.len());
    let (fields, location_type) = detail_fields(&args.arg_fields[0])?;
    let mut valid = BooleanBuilder::with_capacity(row_count);
    let mut reason = StringBuilder::with_capacity(row_count, 0);
    let mut location = GeometryBuilder::new(location_type);
    let mut validity = NullBufferBuilder::new(row_count);

    if matches!(args.arg_fields[0].data_type(), DataType::Null) {
        for _ in 0..row_count {
            valid.append_null();
            reason.append_null();
            location.push_null();
            validity.append_null();
        }
    } else {
        let geo_array = geometry_array_from_arrow(arrays[0].as_ref(), args.arg_fields[0].as_ref())?;
        let wkb_array = to_wkb::<i32>(geo_array.as_ref())?;
        for row in 0..row_count {
            if wkb_array.inner().is_null(row) {
                valid.append_null();
                reason.append_null();
                location.push_null();
                validity.append_null();
                continue;
            }

            let flags = if arrays.len() > 1 {
                match adapter::int64_value(&arrays, 1, row, "ST_IsValidDetail")? {
                    Some(flags) => flags,
                    None => {
                        valid.append_null();
                        reason.append_null();
                        location.push_null();
                        validity.append_null();
                        continue;
                    }
                }
            } else {
                0
            };

            let wkb = wkb_array.inner().value(row);
            let prepared = adapter::prepare_geos_wkb(wkb)?;
            let (is_valid, detail_reason, detail_location) =
                raw::with_context("ST_IsValidDetail", |handle| unsafe {
                    let geometry = raw::read_wkb(handle, prepared.as_ref(), "ST_IsValidDetail")?;
                    let mut reason_ptr = ptr::null_mut();
                    let mut location_ptr = ptr::null_mut();
                    let result = sys::GEOSisValidDetail_r(
                        handle,
                        geometry,
                        flags as c_int,
                        &mut reason_ptr,
                        &mut location_ptr,
                    );
                    raw::destroy_geometry(handle, geometry);
                    if result == 2 {
                        raw::destroy_geometry(handle, location_ptr);
                        if !reason_ptr.is_null() {
                            sys::GEOSFree_r(handle, reason_ptr.cast());
                        }
                        return Err(DataFusionError::Execution(
                            "ST_IsValidDetail: GEOS validity check failed".to_string(),
                        )
                        .into());
                    }

                    let reason = if reason_ptr.is_null() {
                        None
                    } else {
                        Some(raw::take_string(handle, reason_ptr, "ST_IsValidDetail")?)
                    };
                    let location = if location_ptr.is_null() {
                        None
                    } else {
                        let location = raw::write_wkb(handle, location_ptr, "ST_IsValidDetail")?;
                        raw::destroy_geometry(handle, location_ptr);
                        Some(location)
                    };
                    Ok((result == 1, reason, location))
                })?;

            valid.append_value(is_valid);
            reason.append_option(detail_reason.as_deref());
            match detail_location {
                Some(wkb) => {
                    let parsed = Wkb::try_new(&wkb).map_err(|error| {
                        DataFusionError::Execution(format!(
                            "ST_IsValidDetail result WKB is invalid: {error}"
                        ))
                    })?;
                    location.push_geometry(Some(&parsed))?;
                }
                None => location.push_null(),
            }
            validity.append_non_null();
        }
    }

    let location = location.finish().into_array_ref();
    let struct_array = StructArray::new(
        fields,
        vec![
            Arc::new(valid.finish()) as ArrayRef,
            Arc::new(reason.finish()) as ArrayRef,
            location,
        ],
        validity.finish(),
    );
    Ok(ColumnarValue::Array(Arc::new(struct_array)))
}

fn detail_fields(input: &FieldRef) -> GeoDataFusionResult<(Fields, GeometryType)> {
    let location_type = if matches!(input.data_type(), DataType::Null) {
        GeometryType::new(Default::default())
    } else {
        let geo_type = GeoArrowType::from_arrow_field(input)?;
        let metadata = geo_type.metadata();
        GeometryType::new(Arc::new(Metadata::new(metadata.crs().clone(), None)))
            .with_coord_type(geo_type.coord_type().unwrap_or_default())
    };
    let location = location_type.to_field("location", true);
    let fields = Fields::from(vec![
        Arc::new(Field::new("valid", DataType::Boolean, false)),
        Arc::new(Field::new("reason", DataType::Utf8, true)),
        Arc::new(location),
    ]);
    Ok((fields, location_type))
}
