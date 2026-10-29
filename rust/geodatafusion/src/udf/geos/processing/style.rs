use datafusion::error::{DataFusionError, Result};

pub(super) struct StyleParameter<'a> {
    pub(super) key: &'a str,
    pub(super) value: &'a str,
}

/// Parse the space-separated `key=value` parameters shared by the buffer-style
/// functions.
pub(super) fn style_parameters<'a>(
    style: &'a str,
    function: &str,
    subject: &str,
) -> Result<Vec<StyleParameter<'a>>> {
    let mut parameters = Vec::new();
    for parameter in style.split_whitespace() {
        let (key, value) = parameter.split_once('=').ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{function} {subject} parameter '{parameter}' is missing '=value'"
            ))
        })?;
        if value.is_empty() {
            return Err(DataFusionError::Execution(format!(
                "{function} {subject} parameter '{key}' is missing a value"
            )));
        }
        parameters.push(StyleParameter { key, value });
    }
    Ok(parameters)
}
