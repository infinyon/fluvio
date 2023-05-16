# JSON to JSON transformation SmartModule

This is a `map` type SmartModule that transforms JSON records using [Fluvio Jolt][1] library.

The transformation is defined in the configuration of the SmartModule. It is set up once during the SmartModule initialization
and is re-used in the processing.

## Usage
Typically (but not required), Jolt SmartModule is used in chain with other smartmodules to enrich or reduce the data in records.
Here is an example of SQL connector configuration with a chain of two smartmodules:

```yaml
name: fluvio-sql-connector
type: sql-sink
version: latest
topic: mqtt-topic
create-topic: true
parameters:
  database-url: '{DB_CONNECTION_STRING}'
  rust_log: 'sql_sink=INFO,sqlx=WARN'
transforms:
  - uses: infinyon/jolt@0.1.0
    with:
      spec:
        - operation: shift
          spec:
            payload:
              device: "device"       #everything under '$.payload.device.*' goes to '$.device.*'
        - operation: default
          spec:
            device:
              type: "mobile"         #if not present, adds '$device.type' = 'mobile'
  - uses: infinyon/json-sql@0.1.0
    with:
      mapping:
        table: "topic_message"
        map-columns:
          "device_id":
            json-key: "device.device_id"
            value:
              type: "int"
              default: "0"
              required: true
          "record":
            json-key: "$"
            value:
              type: "jsonb"
              required: true
```

In this example we have a chain with two smartmodules:  
    1. `jolt` - changes the structure of the JSON record.  
    2. `json-sql` - generates a SQL record from JSON record resulted from the previous step.

The `jolt` step removes from the incoming records everything except `$.payload.device` objects and moves this object up to
the root level object. After that, it adds `$.device.type` field with default value `mobile`, if not present.

### Jolt SmartModule transformation example
For the above configuration the `jolt` will process the record:
```json
{
  "mqtt_topic": "ag-mqtt-topic",
  "payload": {
    "device": {
      "device_id": 0,
      "name": "device0"
    }
  }
}
```
into:
```json
{
  "device": {
    "device_id": 0,
    "name": "device0",
    "type": "mobile"
  }
}
```

[1]: https://github.com/infinyon/fluvio-jolt
