use pyo3::prelude::*;

trait ToPyErr {
    fn to_pyerr(self) -> PyErr;
}

impl ToPyErr for String {
    fn to_pyerr(self) -> PyErr {
        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(self)
    }
}

pub trait ToPyResult<T> {
    fn to_pyresult(self) -> PyResult<T>;
}

impl<T> ToPyResult<T> for Result<T, String> {
    fn to_pyresult(self) -> PyResult<T> {
        match self {
            Ok(res) => Ok(res),
            Err(err) => Err(err.to_pyerr()),
        }
    }
}
