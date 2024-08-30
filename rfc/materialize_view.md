Fluvio & InfinyOn Cloud users need the ability to perform operations on a time-bound window and generate a materialized view.

## Motivation 

Today, Fluvio supports record-by-record processing with the ability to apply transformation one record at a time. When a multi-record stream-based operation is required, Fluvio users create a Microservice that reads the records, applies an operation, and returns the result to a new stream. Unfortunately, these Microservices are managed independently of Fluvio, significantly increasing the complexity of building simple Real-Time Apps. 

This PRD is a proposal to add the ability to compute aggregates inside Fluvio. This solution should eliminate the need to deploy and operate separate Microservices to perform stream-based computations. 

In a larger context, time-based computations bring Fluvio closer to Flink and Spark, where our users won’t need to run multiple stacks to perform sums, averages, anomaly detections, etc.


## Requirements

Fluvio's data streaming layer (aka topic/partitions) will continue operating as before. The stream processing component is an additional layer that runs on top. This stream-processing engine is defined as a separate object, as described below.


## Example Use Case

We'll begin by describing a data streaming use case and a data set that we'll use to implement it.

### Use Case

We want to build a data pipeline that captures the usage of cloud servers in terms of network storage and compute. In addition, we want to apply the price per unit and calculate the overall cost. The cost is calculated every minute and resets at each month's end.

### Data Sets

The data sets are two data streams: metrics and pricing.

##### Metrics

Each metrics event has a key and a value. The key is the server name, and the value store the metric type, value, and timestamp:

```
$ cat metrics.json
[
  { "key":"server-a", "value": { "metric":"storage", "count":1,  "ts":"2023-02-17T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"compute", "count":2,  "ts":"2023-02-17T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"network", "count":3,  "ts":"2023-02-17T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"storage", "count":4,  "ts":"2023-02-17T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"compute", "count":5,  "ts":"2023-02-17T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"network", "count":6,  "ts":"2023-02-17T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"storage", "count":7,  "ts":"2023-02-18T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"compute", "count":8,  "ts":"2023-02-18T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"network", "count":9,  "ts":"2023-02-18T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"storage", "count":10, "ts":"2023-02-18T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"compute", "count":11, "ts":"2023-02-18T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"network", "count":12, "ts":"2023-02-18T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"storage", "count":13, "ts":"2023-03-02T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"compute", "count":14, "ts":"2023-03-02T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"network", "count":15, "ts":"2023-03-02T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"storage", "count":16, "ts":"2023-03-02T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"compute", "count":17, "ts":"2023-03-02T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"network", "count":18, "ts":"2023-03-02T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"storage", "count":19, "ts":"2023-03-10T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"compute", "count":20, "ts":"2023-03-10T06:41:48.000Z" }},
  { "key":"server-a", "value": { "metric":"network", "count":21, "ts":"2023-03-10T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"storage", "count":22, "ts":"2023-03-10T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"compute", "count":23, "ts":"2023-03-10T06:41:48.000Z" }},
  { "key":"server-b", "value": { "metric":"network", "count":24, "ts":"2023-03-10T06:41:48.000Z" }}
]
```

##### Pricing

The Pricing data set stores the price per metric and the timestamp when it was updated:

```
$ cat pricing.json
[
  {"value": { "metric":"storage", "ts":"1970-01-01T00:00:00.000Z", "cost":0.01}},
  {"value": { "metric":"compute", "ts":"1970-01-01T00:00:00.000Z", "cost":0.02}},
  {"value": { "metric":"network", "ts":"1970-01-01T00:00:00.000Z", "cost":0.03}},
  {"value": { "metric":"storage", "ts":"2023-02-18T01:00:00.000Z", "cost":0.04}},
  {"value": { "metric":"compute", "ts":"2023-02-18T01:00:00.000Z", "cost":0.05}},
  {"value": { "metric":"network", "ts":"2023-02-18T01:00:00.000Z", "cost":0.06}},
  {"value": { "metric":"storage", "ts":"2023-03-02T01:00:00.000Z", "cost":0.07}},
  {"value": { "metric":"compute", "ts":"2023-03-02T01:00:00.000Z", "cost":0.08}},
  {"value": { "metric":"network", "ts":"2023-03-02T01:00:00.000Z", "cost":0.09}},
  {"value": { "metric":"storage", "ts":"2023-03-10T01:00:00.000Z", "cost":0.10}},
  {"value": { "metric":"compute", "ts":"2023-03-10T01:00:00.000Z", "cost":0.11}},
  {"value": { "metric":"network", "ts":"2023-03-10T01:00:00.000Z", "cost":0.12}}
]
```

Next, we'll use the data sets to create the materialized views.


<hr />


## Materialized Views

Fluvio operates on binary records, where the data interpretation is opaque to the system. However, with stream-based computation, the system must understand the data it operates on.

Defining a materialized view in Fluvio requires the following steps:

1. Define a column schema yaml definition.
2. Create a topic and apply the column schema.
3. Define a materialized view yaml file.
4. Create a view and apply the materialized view definition.

Joining materialized views is a derivative of the operations above, where a materialized view references another to derive a composite result.

Let's create the `metrics` materialized view that computes an aggregate for each server and metric for the current month.

### 1. Define a Column Schema Definition

The column schema definition file has a dual purpose: to validate and map records from the data stream into a memory representation.

```
$cat metrics_columns.yaml
- name: server
  key: true
  type: string
  validate:
  - uses: infinyon/regex@0.1.0
    with: 
      regex: ^[a-zA-Z]+([a-zA-Z0-9]+)*$
- name: metric
  type: string
  map: 
  - uses: john/lowercase@0.1.0
- name: count
  type: double
- name: ts
  type: timestamp
- name: description
	type: string
	optional: true
```

The expected input format is `json,` and the data mapping is performed based on `name.` The order of the items in the file defines the order in the resulting table.

*Definitions*

- `key` reads this field from the key of the record.
    - there can only be up to one `key` column per schema file
- `optional` allows records without this field to be parsed successfully.
    - `optional` columns are internally represented as `rust options`
- `validate` invokes a smartmodule that ensures the record is compliant
- `map` invokes a smartmodule to convert an item into the desired output


### 2. Create a Topic and Apply the Column Schema

Create a topic and apply the column schema. These topics are `columnar topics.`


#### Create a Columnar Topic

Create a topic a `columnar topic` called `metrics` as follows:

```
$ fluvio topic create metrics --columns metrics_columns.yaml
```

Use `metrics.json` file we defined above to load events into the topic:

```
$ fluvio produce --json-file metrics.json metrics
parsed successfully
```

*Parsing behavior*

- for mandatory field, if column is not found, **display error**, **skip record & continue**.
- for type parsing, **display error**, **skip record & continue**.
- for validation error, **display error**, **skip record & continue**.
- for undefined field, ignore field, **display warning**, **build records** & **continue.**


#### Inspect the topic

Inspect the uploaded data. While `columnar topics` can natively produce tables, they require `--output table` for backward compatibility:

```
$ fluvio consume metrics -Bd --output table

server      metric  count             ts               description
---------  -------  -----   ------------------------   -----------
server-a   storage      1   2023-02-17 06:41:48.000Z   
server-a   compute      2   2023-02-17 06:41:48.000Z
server-a   network      3   2023-02-17 06:41:48.000Z
server-b   storage      4   2023-02-17 06:41:48.000Z
...
```

Columnar topics are identified by the `COLUMNS` flag:

```
$ fluvio topic list
NAME          COLUMNS    TYPE      PARTITIONS  REPLICAS  RETENTION  COMPRESSION  STATUS                   REASON 
metrics         Y       computed      1           1        7days       any      provisioned
topic-1                 computed      1           1        7days       any      provisioned
```

### 3. Create a Materialized View Configuration file

Create a materialized view definition file `usage-view.yaml`, to describe the target topic, operation, and output of the materialized view:

```
$ cat usage-view.yaml
topic: metrics
window:
  from: `datetime.today().replace(day=1, minute=0, second=0, microsecond=0).timestamp()`
  interval: 60 sec
groupBy: 
  - server
  - metric
conditions:
  - metric = 'storage' OR metric = 'compute' OR metric = 'network'  
output:
  - field: server
  - field: metric
  - operation: sum(count)
    label: quantity
```

*Definitions*

- `topic` - target topic for the materialized view (restricted to columnar topics)
- `window` - defines range (`from` until now) and refresh interval:
    - `from` - `python code` to compute `first day of month` - “2023-03-01T00:00:00.000Z” and convert to millisecond timestamp: 1679035308000.
    - `interval` - the time interval after which a new window is recomputed (humanized).
- `groupBy` - groups records for the operation specified in the “output”.
- `conditions` *(optional)* - allows for additional query refinement
    - common operations: `=`, `<` , `>`, `and`, `or`, `not`
- `field` - represents the column to be displayed in the output
    - use `operation` to perform a computation
        - apis: `sum`, `aggregate`, `min`, `max`, `count`
        - arithmetic operations: `+`, `-` , `*`, `/`
        - for e*xamples*: `sum(count * price)`
    - use `label` to rename the column

_Note_: A columnar topic may have as many materialized views as desired.


### 4. Create a View and Apply the Materialized View Definition.

We are introducing a new object called `view` to manage materialized views. 


#### Create a View object

Apply configuration file to create a materialized view object:

```
$ fluvio view create usage usage-view.yaml
view created
```

The materialized begins stream processing as soon as it's applied.


#### Inspect the View

List views:

```
$ fluvio view list

NAME      FROM      INTERVAL
-----  ----------  ----------
usage  2023-03-01  60 seconds  
```


#### Consume from  View

Consuming from a view is similar to consuming from a topic, except that the output is in table format.

*Streaming (default)*

The table is automatically updated at refresh interval

```
$ fluvio view consume usage

SERVER     METRIC   QUANTITY
--------   -------  --------
server-a   storage      32.0 
server-a   compute      34.0 
server-a   network      36.0 
server-b   storage      38.0 
server-b   compute      40.0 
server-b   network      42.0
```

*Snapshot*

The table is retrieve, and the command exists. Reading the view multiple times, retrieves the same values until the next refresh interval.

```
$ fluvio view consume usage -d
```

#### View Commands

The view object has the following commands:

```
$ fluvio view -h

  create     Provision a view
  consume    Read the table produced by the view
  eval       invoke an API from the view
  list       List all views
  describe   Show configuration parameters
  delete     Delete a view

```

In summary, to create a materialized view, we need to:
* Build a column definition schema
* Create a columnar topic
* Build a materialized view definition file
* Create the view

<hr />


## Join Materialized Views

Join is the most requested operation for materialized views. In this section, we'll add a `pricing` and join it with `usage` to compute the usage cost.

Joining materialized views has two steps:

1. Create the view providing the data
2. Create the view consuming the data

Let's create get started.


### 1. Create the View Providing the Data

The view providing the data is a `pricing` view. We'll go through the same steps as above to create this view.

#### Create `pricing` Columnar Topic

Create column schema definition `pricing-columns.yaml` file:

```
$ cat pricing_columns.yaml
- name: metric
  type: string
- name: ts
  type: timestamp
- name: cost
  type: decimal(10,2)
  ```

Create  `pricing` columnar topic:

```
$ fluvio topic create pricing --columns pricing_columns.yaml
```

Add pricing data from `JSON` file defined above to the topic:

```
 $ fluvio produce --json-file pricing.json pricing
parsed successfully
```

Consume from the pricing view:

```
$ fluvio consume pricing -Bd --output table

METRIC             TS              COST  
-------  ------------------------  ----  
storage  1970-01-01 00:00:00.000Z  0.01 
compute  1970-01-01 00:00:00.000Z  0.02 
network  1970-01-01 00:00:00.000Z  0.03
storage  2023-02-18 01:00:00.000Z  0.04 
...
```

#### Create `pricing` View

Create `pricing-view.yaml` definition file:

```
$ cat pricing-view.yaml
topic: pricing
smartmodules:
  - name: getPrice
    uses: john/getPrice@0.1.0
    with:
      params:
        - name: ts
          type: timestamp
        - name: metric
          type: string
    output: 
      - decimal(10,2)
output:
  - field: metric
  - field: cost
  - field: ts
  ```

In addition to the `output`, this materialized view also offers a `smartmodule` called `getPrice`. The smartmodule takes 2 parameters, and returns the prices for the specific `timestamp`. The smartmodule was built by `john` and he published it in the `smartmodule hub`.

Create the `pricing` view:

```
$ fluvio view create pricing pricing-view.yaml
view created
```

Consume from `pricing` view:

```
$ fluvio view consume pricing -d

METRIC             TS         COST  
-------  -------------------  ----  
storage  1970-01-01 00:00:00  0.01 
compute  1970-01-01 00:00:00  0.02 
network  1970-01-01 00:00:00  0.03 
storage  2023-02-18 01:00:00  0.04
...
```

Test `getPrice` API:

```
$ fluvio view eval pricing `getPrice("network", "2023-02-18 06:41:48")`
0.06
```

### 1. Create the View Consuming the Data

In our use case the consumer of the `pricing` view is the `usage` view, with a new column for the price.

Let's define a new `usage-pricing-view.yaml` view:

```
$ cat usage-pricing-view.yaml
topic: metrics
window:
  from: `datetime.today().replace(day=1, minute=0, second=0, microsecond=0).timestamp()`
  interval: 60 sec
derivedColumns:
  - field: cost
    eval: `pricing.getPrice($.metric, $.ts)`
groupBy: 
  - cluster
  - metric
output:
  - field: cluster
  - field: metric
  - operation: sum(count)
    label: quantity
  - operation: sum(cost * count)
    label: cost
```

This view defines a new `derivedColumn` that evaluates `pricing.getPrice` with the metric and timestamp values from `metrics` topic and returns the reult in `cost`. The `cost` is used in the output to compute the final result.

*Definitions*

- `derivedColumns` allows a view to reference `smartmodules` from other views.
    - `field` is the name of the new column.
    - `eval`  is the routine to be invoke
        - before `.` is the `view` name: **pricing**
        - after `.` is the API to evaluate: **getPrice**
        - parameters reference values in the metrics table:
            - `$.metric` - metric value in the current row
            - `$.ts` - timestamp in the current row
            - The result is stored in the column, for ts="2023-02-18 06:41:48" & metric="network" it would be  `0.6` .
- operation `sum(cost * count)`  takes the `cost` from derived column and multiplies with count.


Create `usage-pricing` view:

```
$ fluvio view create usage-pricing usage-pricing-view.yaml
view created
```

Consume from `usage-pricing`: 

```
SERVER     METRIC   QUANTITY  COST
--------   -------  --------  ----
server-a   storage      32.0  2.81 
server-a   compute      34.0  3.32
server-a   network      36.0  3.87
server-b   storage      38.0  3.32
server-b   compute      40.0  3.89
server-b   network      42.0  4.50
```

In summary, to create a join, we need to:
* Create a provider materialized view and expose an API
* Create a consumer materialized view with derived columns that evaluate the provider API.
