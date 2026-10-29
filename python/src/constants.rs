use std::ffi::CStr;

pub const AGGREGATE_UDF_CAPSULE_NAME: &CStr = cr"datafusion_aggregate_udf";
pub const SCALAR_UDF_CAPSULE_NAME: &CStr = cr"datafusion_scalar_udf";
pub const TABLE_FUNCTION_CAPSULE_NAME: &CStr = cr"datafusion_table_function";
pub const WINDOW_UDF_CAPSULE_NAME: &CStr = cr"datafusion_window_udf";
