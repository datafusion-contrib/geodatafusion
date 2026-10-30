mod accessors;
mod bounding_box;
mod constructors;
mod editors;
mod io;

use pyo3::prelude::*;

#[pymodule]
pub(crate) fn native(m: &Bound<PyModule>) -> PyResult<()> {
    // accessors
    m.add_class::<accessors::PyCoordDim>()?;
    m.add_class::<accessors::PyDimension>()?;
    m.add_class::<accessors::PyDump>()?;
    m.add_class::<accessors::PyEndPoint>()?;
    m.add_class::<accessors::PyExteriorRing>()?;
    m.add_class::<accessors::PyHasM>()?;
    m.add_class::<accessors::PyHasZ>()?;
    m.add_class::<accessors::PyInteriorRingN>()?;
    m.add_class::<accessors::PyIsClosed>()?;
    m.add_class::<accessors::PyIsCollection>()?;
    m.add_class::<accessors::PyIsEmpty>()?;
    m.add_class::<accessors::PyM>()?;
    m.add_class::<accessors::PyGeometryType>()?;
    m.add_class::<accessors::PyNDims>()?;
    m.add_class::<accessors::PyNPoints>()?;
    m.add_class::<accessors::PyNRings>()?;
    m.add_class::<accessors::PyNumGeometries>()?;
    m.add_class::<accessors::PyNumInteriorRings>()?;
    m.add_class::<accessors::PyPointN>()?;
    m.add_class::<accessors::PyStartPoint>()?;
    m.add_class::<accessors::PySTGeometryType>()?;
    m.add_class::<accessors::PyX>()?;
    m.add_class::<accessors::PyY>()?;
    m.add_class::<accessors::PyZ>()?;
    m.add_class::<accessors::PyZmFlag>()?;

    // bounding_box
    m.add_class::<bounding_box::PyBox2D>()?;
    m.add_class::<bounding_box::PyBox3D>()?;
    m.add_class::<bounding_box::PyExtent>()?;
    m.add_class::<bounding_box::PyMakeBox2D>()?;
    m.add_class::<bounding_box::PyMakeBox3D>()?;
    m.add_class::<bounding_box::PyXMax>()?;
    m.add_class::<bounding_box::PyXMin>()?;
    m.add_class::<bounding_box::PyYMax>()?;
    m.add_class::<bounding_box::PyYMin>()?;
    m.add_class::<bounding_box::PyZMax>()?;
    m.add_class::<bounding_box::PyZMin>()?;

    // constructors
    m.add_class::<constructors::PyLineFromMultiPoint>()?;
    m.add_class::<constructors::PyMakeEnvelope>()?;
    m.add_class::<constructors::PyMakeLine>()?;
    m.add_class::<constructors::PyPoint>()?;
    m.add_class::<constructors::PyPointZ>()?;
    m.add_class::<constructors::PyPointM>()?;
    m.add_class::<constructors::PyPointZM>()?;
    m.add_class::<constructors::PyMakePoint>()?;
    m.add_class::<constructors::PyMakePointM>()?;

    // editors
    m.add_class::<editors::PyForce2D>()?;
    m.add_class::<editors::PyForce3D>()?;
    m.add_class::<editors::PyForce3DZ>()?;
    m.add_class::<editors::PyForce3DM>()?;
    m.add_class::<editors::PyForce4D>()?;

    // io
    m.add_class::<io::PyAsBinary>()?;
    m.add_class::<io::PyAsText>()?;
    m.add_class::<io::PyGeomFromText>()?;
    m.add_class::<io::PyGeomFromWKB>()?;

    Ok(())
}
