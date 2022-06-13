{% if smartmodule-type == "filter" %}
use fluvio_smartmodule::{smartmodule, Result, Record};

#[smartmodule(filter{%if smartmodule-params  %}, params{% endif %})]
pub fn filter(record: &Record{%if smartmodule-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(string.contains('a'))
}
{% elsif smartmodule-type == "map" %}
use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};

#[smartmodule(map{%if smartmodule-params  %}, params{% endif %})]
pub fn map(record: &Record{%if smartmodule-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<(Option<RecordData>, RecordData)> {
    let key = record.key.clone();

    let string = std::str::from_utf8(record.value.as_ref())?;
    let int = string.parse::<i32>()?;
    let value = (int * 2).to_string();

    Ok((key, value.into()))
}
{% elsif smartmodule-type == "filter-map" %}
use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(filter_map{% if smartmodule-params  %}, params{% endif %})]
pub fn filter_map(record: &Record{%if smartmodule-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<Option<(Option<RecordData>, RecordData)>> {
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

{% elsif smartmodule-type == "array-map" %}
use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};

#[smartmodule(array_map{% if smartmodule-params  %}, params{% endif %})]
pub fn array_map(record: &Record{% if smartmodule-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<Vec<(Option<RecordData>, RecordData)>> {
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
{% elsif smartmodule-type == "aggregate" %}
use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};

#[smartmodule(aggregate{% if smartmodule-params  %}, params{% endif %})]
pub fn aggregate(accumulator: RecordData, current: &Record{% if smartmodule-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<RecordData> {
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
{% if smartmodule-params  %}
#[derive(fluvio_smartmodule::SmartOpt, Default)]
pub struct SmartModuleOpt;
{% endif %}
