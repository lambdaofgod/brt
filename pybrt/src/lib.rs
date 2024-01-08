mod util;
use brt_core::index_registry::IndexRegistry;
use brt_core::wrappers::TantivyIndexWrapper;
use lazy_static::lazy_static;
use polars::prelude::DataFrame;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use pyo3_polars::PyDataFrame;
use std::collections::HashMap;
use util::ToPyResult;

lazy_static! {
    static ref INDEX_REGISTRY: IndexRegistry = IndexRegistry::new();
}

#[pyfunction]
fn initialize_index(name: &PyString, name_field: &PyString, fields: &PyList) -> PyResult<()> {
    let fields_list = fields.extract::<Vec<String>>()?;
    INDEX_REGISTRY
        .initialize_index(name.to_string(), name_field.to_string(), fields_list)
        .to_pyresult()?;
    Ok(())
}

#[pyfunction]
fn index_document<'a>(name: &'a PyString, document_dict: &'a PyDict) -> PyResult<()> {
    let document_map = document_dict.extract::<HashMap<&str, String>>()?;

    INDEX_REGISTRY
        .index_document(name.to_string(), document_map)
        .to_pyresult()
}

#[pyfunction]
fn search<'a>(py: Python<'a>, name: &'a PyString, query: &'a PyString) -> PyResult<&'a PyList> {
    let vec_results = INDEX_REGISTRY
        .search(name.to_string(), query.to_string())
        .to_pyresult()?;
    Ok(PyList::new(py, vec_results))
}

#[pyfunction]
fn get_index_names<'a>(py: Python<'a>) -> PyResult<&'a PyList> {
    let index_names = INDEX_REGISTRY.get_index_names().to_pyresult();
    Ok(PyList::new(py, index_names))
}

// #[pyfunction]
// fn get_field_names<'a>(py: Python<'a>, name: &'a PyString) -> PyResult<&'a PyList> {
//     let index = INDEX_REGISTRY
//         .indices
//         .read()
//         .unwrap()
//         .get(&name.to_string())
//         .unwrap();

//     Ok(PyList::new(py, index.field_names()))
// }

#[pyfunction]
fn index_polars_dataframe(name: &PyString, pydf: PyDataFrame) -> PyResult<()> {
    let df: DataFrame = pydf.into();
    INDEX_REGISTRY.index_df(name.to_string(), &df).to_pyresult()
}

#[pymodule]
#[pyo3(name = "pybrt")]
fn brt(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(initialize_index, m)?)?;
    m.add_function(wrap_pyfunction!(index_document, m)?)?;
    m.add_function(wrap_pyfunction!(search, m)?)?;
    m.add_function(wrap_pyfunction!(get_index_names, m)?)?;
    m.add_function(wrap_pyfunction!(index_polars_dataframe, m)?)?;
    // m.add_function(wrap_pyfunction!(get_field_names, m)?)?;

    Ok(())
}
