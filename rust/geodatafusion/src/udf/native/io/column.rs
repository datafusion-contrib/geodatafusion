//! Small typed-column accessors shared by text-formatting UDFs.

use arrow_array::{
    Array, ArrayRef, Int32Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
};
use arrow_schema::DataType;
use datafusion::error::{DataFusionError, Result};

#[derive(Clone, Copy)]
pub(crate) enum IntColumn<'a> {
    Default(Option<i64>),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
}

impl<'a> IntColumn<'a> {
    pub(crate) fn from_array(array: Option<&'a ArrayRef>, default: Option<i64>) -> Result<Self> {
        let Some(array) = array else {
            return Ok(Self::Default(default));
        };
        match array.data_type() {
            DataType::Int32 => Ok(Self::Int32(array.as_any().downcast_ref().unwrap())),
            DataType::Int64 => Ok(Self::Int64(array.as_any().downcast_ref().unwrap())),
            other => Err(DataFusionError::Internal(format!(
                "unexpected integer argument type {other}"
            ))),
        }
    }

    pub(crate) fn value(&self, row: usize) -> Option<i64> {
        match self {
            Self::Default(value) => *value,
            Self::Int32(array) if array.is_null(row) => None,
            Self::Int32(array) => Some(i64::from(array.value(row))),
            Self::Int64(array) if array.is_null(row) => None,
            Self::Int64(array) => Some(array.value(row)),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum TextColumn<'a> {
    Default(Option<&'a str>),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
}

impl<'a> TextColumn<'a> {
    pub(crate) fn from_array(
        array: Option<&'a ArrayRef>,
        default: Option<&'a str>,
    ) -> Result<Self> {
        let Some(array) = array else {
            return Ok(Self::Default(default));
        };
        match array.data_type() {
            DataType::Utf8 => Ok(Self::Utf8(array.as_any().downcast_ref().unwrap())),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8(array.as_any().downcast_ref().unwrap())),
            DataType::Utf8View => Ok(Self::Utf8View(array.as_any().downcast_ref().unwrap())),
            other => Err(DataFusionError::Internal(format!(
                "unexpected text argument type {other}"
            ))),
        }
    }

    pub(crate) fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::Default(value) => *value,
            Self::Utf8(array) if array.is_null(row) => None,
            Self::Utf8(array) => Some(array.value(row)),
            Self::LargeUtf8(array) if array.is_null(row) => None,
            Self::LargeUtf8(array) => Some(array.value(row)),
            Self::Utf8View(array) if array.is_null(row) => None,
            Self::Utf8View(array) => Some(array.value(row)),
        }
    }
}
