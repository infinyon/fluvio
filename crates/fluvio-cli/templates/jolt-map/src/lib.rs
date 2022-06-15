use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};

#[smartmodule(map{%if smartmodule-params  %}, params{% endif %})]
pub fn map(record: &Record{%if smartmodule-params  %}, _params: &SmartModuleOpt{% endif %}) -> Result<(Option<RecordData>, RecordData)> {
    use serde_json::{json,from_value};
    use fluvio_jolt::{transform,TransformSpec};

    let spec: TransformSpec = from_value(json!(    
            {{ jolt }}
    ))
    .expect("parsed spec");

    let input = serde_json::from_slice(record.value.as_ref())?;
    let result = transform(input, &spec);
    let serialized_output = serde_json::to_vec(&result)?;
    Ok((None, RecordData::from(serialized_output)))
}
