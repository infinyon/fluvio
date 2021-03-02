use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

#[pyfunction]
fn rust_func() -> usize {
    14
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(rust_func))?;

    Ok(())
}
