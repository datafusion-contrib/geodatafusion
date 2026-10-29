use std::sync::{Arc, OnceLock};

use arrow_array::{BinaryArray, StringArray};
use arrow_schema::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF};
use datafusion::prelude::SessionContext;
use geoarrow_schema::{Metadata, WkbType, WktType};

struct FuzzUdfs {
    from_text: ScalarUDF,
    from_wkb: ScalarUDF,
}

static UDFS: OnceLock<FuzzUdfs> = OnceLock::new();

fn udfs() -> &'static FuzzUdfs {
    UDFS.get_or_init(|| {
        let ctx = SessionContext::new();
        geodatafusion::register(&ctx);
        let state = ctx.state();
        let function = |name: &str| {
            state
                .scalar_functions()
                .get(name)
                .unwrap_or_else(|| panic!("missing UDF {name}"))
                .as_ref()
                .clone()
        };
        FuzzUdfs {
            from_text: function("st_geomfromtext"),
            from_wkb: function("st_geomfromwkb"),
        }
    })
}

/// Invoke the public WKT constructor. Invalid UTF-8 is rejected at the
/// Arrow string boundary before reaching the parser.
pub fn invoke_wkt(data: &[u8]) -> datafusion::error::Result<()> {
    let Some(text) = std::str::from_utf8(data).ok().map(str::to_string) else {
        return Ok(());
    };
    let array = StringArray::from(vec![text]);
    let field: FieldRef = Arc::new(
        Field::new("input", DataType::Utf8, true)
            .with_extension_type(WktType::new(Arc::new(Metadata::default()))),
    );
    std::hint::black_box(invoke_single_argument(
        &udfs().from_text,
        ColumnarValue::Array(Arc::new(array)),
        field,
    )?);
    Ok(())
}

/// Invoke the public WKB constructor with arbitrary bytes.
pub fn invoke_wkb(data: &[u8]) -> datafusion::error::Result<()> {
    let array = BinaryArray::from(vec![Some(data)]);
    let field: FieldRef = Arc::new(
        Field::new("input", DataType::Binary, true)
            .with_extension_type(WkbType::new(Arc::new(Metadata::default()))),
    );
    std::hint::black_box(invoke_single_argument(
        &udfs().from_wkb,
        ColumnarValue::Array(Arc::new(array)),
        field,
    )?);
    Ok(())
}

fn invoke_single_argument(
    udf: &ScalarUDF,
    argument: ColumnarValue,
    argument_field: FieldRef,
) -> datafusion::error::Result<ColumnarValue> {
    let scalar_arguments = [None];
    let return_field = udf.return_field_from_args(ReturnFieldArgs {
        arg_fields: std::slice::from_ref(&argument_field),
        scalar_arguments: &scalar_arguments,
    })?;
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![argument],
        arg_fields: vec![argument_field],
        number_rows: 1,
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    })
}
