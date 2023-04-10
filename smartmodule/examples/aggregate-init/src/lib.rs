use std::sync::atomic::{AtomicBool, Ordering};

use fluvio_smartmodule::{
    dataplane::smartmodule::SmartModuleExtraParams, smartmodule, Record, RecordData, Result,
};

use once_cell::sync::OnceCell;

static INITIAL_VALUE: OnceCell<UseOnce<RecordData>> = OnceCell::new();

const PARAM_NAME: &str = "initial_value";

#[smartmodule(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &Record) -> Result<RecordData> {
    let accumulator = if let Some(initial_value) = INITIAL_VALUE.get() {
        initial_value.get_or(&accumulator)
    } else {
        &accumulator
    };
    // Parse the accumulator and current record as strings
    let accumulator_string = std::str::from_utf8(accumulator.as_ref())?;
    let current_string = std::str::from_utf8(current.value.as_ref())?;

    // Parse the strings into integers
    let accumulator_int = accumulator_string.trim().parse::<i32>().unwrap_or(0);
    let current_int = current_string.trim().parse::<i32>()?;

    // Take the sum of the two integers and return it as a string
    let sum = accumulator_int + current_int;
    Ok(sum.to_string().into())
}

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    if let Some(raw_spec) = params.get(PARAM_NAME) {
        INITIAL_VALUE
            .set(UseOnce::new(raw_spec.as_str().into()))
            .expect("initial value is already initialized");
    };
    Ok(())
}

#[derive(Debug)]
struct UseOnce<T> {
    value: T,
    used: AtomicBool,
}

impl<T> UseOnce<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            used: AtomicBool::new(false),
        }
    }

    fn get_or<'a: 'b, 'b>(&'a self, default: &'b T) -> &'b T {
        if self.used.load(Ordering::SeqCst) {
            default
        } else {
            self.used.store(true, Ordering::SeqCst);
            &self.value
        }
    }
}
