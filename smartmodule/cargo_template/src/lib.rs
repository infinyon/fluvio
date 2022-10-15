{% if smart-module-init %}
use fluvio_smartmodule::dataplane::smartmodule::{SmartModuleExtraParams, SmartModuleInitError};
{% if smart-module-type == "filter" %}
use once_cell::sync::OnceCell;
use fluvio_smartmodule::eyre;
{% endif %}
{% endif %}
{% if smart-module-type == "filter" %}
use fluvio_smartmodule::{smartmodule, Result, Record};

#[smartmodule(filter{%if smart-module-params  %}, params{% endif %})]
pub fn filter(record: &Record{%if smart-module-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(string.contains('a'))
}
{% elsif smart-module-type == "map" %}
use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};

#[smartmodule(map{%if smart-module-params  %}, params{% endif %})]
pub fn map(record: &Record{%if smart-module-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<(Option<RecordData>, RecordData)> {
    let key = record.key.clone();

    let string = std::str::from_utf8(record.value.as_ref())?;
    let int = string.parse::<i32>()?;
    let value = (int * 2).to_string();

    Ok((key, value.into()))
}
{% elsif smart-module-type == "filter-map" %}
use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(filter_map{% if smart-module-params  %}, params{% endif %})]
pub fn filter_map(record: &Record{%if smart-module-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<Option<(Option<RecordData>, RecordData)>> {
    let key = record.key.clone();
    let string = String::from_utf8_lossy(record.value.as_ref()).to_string();
    let int: i32 = string.parse()?;

    if int % 2 == 0 {
        let output = int / 2;
        Ok(Some((key.clone(), RecordData::from(output.to_string()))))
    } else {
        Ok(None)
    }
}

{% elsif smart-module-type == "array-map" %}
use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};

#[smartmodule(array_map{% if smart-module-params  %}, params{% endif %})]
pub fn array_map(record: &Record{% if smart-module-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    // Deserialize a JSON array with any kind of values inside
    let array = serde_json::from_slice::<Vec<serde_json::Value>>(record.value.as_ref())?;

    // Convert each JSON value from the array back into a JSON string
    let strings: Vec<String> = array
        .into_iter()
        .map(|value| serde_json::to_string(&value))
        .collect::<core::result::Result<_, _>>()?;

    // Create one record from each JSON string to send
    let kvs: Vec<(Option<RecordData>, RecordData)> = strings
        .into_iter()
        .map(|s| (None, RecordData::from(s)))
        .collect();
    Ok(kvs)
}
{% elsif smart-module-type == "aggregate" %}
use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};

#[smartmodule(aggregate{% if smart-module-params  %}, params{% endif %})]
pub fn aggregate(accumulator: RecordData, current: &Record{% if smart-module-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<RecordData> {
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
{% endif %}
{% if smart-module-params  %}
#[derive(fluvio_smartmodule::SmartOpt, Default)]
pub struct SmartModuleOpt;
{% endif %}
{% if smart-module-init %}
{% if smart-module-type == "filter" %}
static CRITERIA: OnceCell<String> = OnceCell::new();

#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    // You can refer to the example SmartModules in Fluvio's GitHub Repository
    // https://github.com/infinyon/fluvio/tree/master/smartmodule
    if let Some(key) = params.get("key") {
        CRITERIA.set(key.clone()).map_err(|err| eyre!("failed setting key: {:#?}", err))
    } else {
        Err(SmartModuleInitError::MissingParam("key".to_string()).into())
    }
}
{% else %}
#[smartmodule(init)]
fn init(params: SmartModuleExtraParams) -> Result<()> {
    // You can refer to the example SmartModules in Fluvio's GitHub Repository
    // https://github.com/infinyon/fluvio/tree/master/smartmodule
    todo!("Provide initialization logic for your SmartModule")
}
{% endif %}
{% endif %}
