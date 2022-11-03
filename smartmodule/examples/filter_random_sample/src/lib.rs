use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleInitError};

use fluvio_smartmodule::eyre;
use once_cell::sync::OnceCell;

use fluvio_smartmodule::{smartmodule, Record, Result};

#[smartmodule(filter)]
pub fn filter(_: &Record) -> Result<bool> {
    // fastrand is NOT cryptographically secure
    Ok(fastrand::u64(..CRITERIA.get().unwrap()) == 0)
}

static CRITERIA: OnceCell<u64> = OnceCell::new();

const SAMPLING_FREQUENCY: &str = "sampling_frequency";
#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    let sampling_frequency = expect_key(SAMPLING_FREQUENCY, &params)?.parse()?;

    if sampling_frequency < 1 {
        return Err(
            SmartModuleInitError::MissingParam("TODO change error type".to_string()).into(),
        );
    }

    CRITERIA
        .set(sampling_frequency)
        .map_err(|err| eyre!("failed setting key: {:#?}", err))
}

fn expect_key<'a>(key: &str, params: &'a SmartModuleExtraParams) -> Result<&'a String> {
    params
        .get("sampling_frequency")
        .ok_or_else(|| SmartModuleInitError::MissingParam(key.to_string()).into())
}
