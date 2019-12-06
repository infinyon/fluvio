# KF-Protocol-Build Code Generator

 The motivation for building generator for **Kafka Spec** to **Rust Code** is meant to reduce the burden of integrating various Kafka releases with Fluvio project.
 
 Code generator takes an *input* directory of **JSON** files and an *output* file or directory to store resulting **RUST** code. The conversion may be augmented with custom defined field translations through the use of map files.

Input directory must consist of file **Request/Response** file-pairs (as published by Kafka):

*  __[API-name]Request.json__ 
*  __[API-name]Response.json__ 

Each file-pair is compiled together into a corresponding **kf_[api_name].rs** file. Resulting **.rs** files are placed in the output parameter provided at the command line.

## Usage

Install Kafka in a parallel directory

#### Generate code

Code generator has the ability to covert files with or without modifications and place output in a file or directory.

```
USAGE:
    kfspec2code generate [OPTIONS] --input <string> --output-directory <string>

FLAGS:
    -h, --help    Prints help information

OPTIONS:
    -i, --input <string>               Input directory
    -m, --map-file <string>            Json file for field translations
    -o, --output-directory <string>    Output directory
    -f, --output-file <string>         Output file
```

Example code generation with directory output:

```
./kfspec2code  generate -i ../kafka/clients/src/main/resources/common/message/ -o ~/output
```


Use **Map Files** to translate individual fields during during code generation, Maps are **json** files that match fields in Kafka spec and translate them with custom definitions. Map Files have the following keys:

* kind - {'header', 'primitiveField', 'structField'} 
* match - { 'name', 'type' }
* translate - { 'type' }
* add - [ array of strings to augment 'structFiled']

Translate ErrorCode from type **int16** to **ErrorCode** which is represented by an enum in the Rust code base:

```
> vi error_code_map.json
{
    "translations": [
        {
            "field": {
                "match": {
                    "name": "ErrorCode",
                    "type": "int16"
                },
                "translate": {
                    "type": "ErrorCode"
                }
            }
        }
    ]
}
:wq

./kfspec2code  generate -i ../kafka/clients/src/main/resources/common/message/ -m error_code_map.json -o ~/output
```

Map files may have multiple sections. In this example, **Record** is translated to an **R** trait:

```
{
    "translations": [
        {
            "header": {
                "match": {
                    "name": "FetchResponse",
                    "type": "response"
                },
                "translate": {
                    "name": "FetchResponse<R>"
                }
            }
        },
        {
            "field": {
                "match": {
                    "name": "Topics",
                    "type": "[]FetchableTopicResponse"
                },
                "translate": {
                    "type": "[]FetchableTopicResponse<R>"
                }
            }
        },
        {
            "field": {
                "match": {
                    "name": "Partitions",
                    "type": "[]FetchablePartitionResponse"
                },
                "translate": {
                    "type": "[]FetchablePartitionResponse<R>"
                }
            }
        },
        {
            "field": {
                "match": {
                    "name": "Records",
                    "type": "bytes"
                },
                "translate": {
                    "type": "R"
                }
            }
        }
    ],
    "additions": [
        {
            "struct": {
                "names": [
                    "FetchResponse<R>",
                    "FetchableTopicResponse<R>",
                    "FetchablePartitionResponse<R>"
                ]
            },
            "add": {
                "lines": [
                    {
                        "text": "where",
                        "indent_level": 1
                    },
                    {
                        "text": "R: Encoder + Decoder + Default + Debug,",
                        "indent_level": 2
                    }
                ]
            }
        }
    ]
}
```

Finally, multiple map files may be chained together in one code generation request

```
./kfspec2code  generate -i ../kafka/clients/src/main/resources/common/message/ -m error_code_map.json -m fetch_responses_map.json -o ~/output
```

#### Check Keys

Check if any new keys have been introduced

```
USAGE:
    kfspec2code check-keys --input <string>

FLAGS:
    -h, --help    Prints help information

OPTIONS:
    -i, --input <string>    Input directory
```

For example 

```
./kfspec2code check-keys -i ../kafka/clients/src/main/resources/common/message/
```



## Code Generator

Fluvio code generator interprets **Kafka Json** parameters and generates **Fluvio RS** template files that that are compiled into code by Rust template generator.


## Derive Templates

Fluvio uses **#[derive(FluvioKf)]** to tell Rust to convert the API template to various API versions. Kafka derive parameters such as **#[fluvio_kf-(topic)]** informs compiler of the topics that are targeteed by the conversion.

#### API Versions

Each **Request** and **Response** has a mandatory ***validVersions*** parameter that details all versions supported by the API:

``` 
"validVersions": "0-3"
...
"validVersions": "0"
```

In Fluvio, validVersions are represented by **#fluvio_kf(...)]**:

```
#[fluvio_kf(api_min_version = 0, api_max_version = 3)]

#[fluvio_kf(api_min_version = 0, api_max_version = 0)]
```

#### Field Versions

Each field has a mandatory has ***versions*** parameter that defines the versions of the API that support this field:

```
"versions": "2-4"
...
"versions": "3+"
```

In Fluvio, they are represented by **#fluvio_kf(...)]**:

```
#[fluvio_kf(min_version = 2, max_version = 4)]

#[fluvio_kf(min_version = 3)]
```

Code generator skips elements with "0+" versions.


#### Nullable Versions

Kafka uses an optional parameter ***nullableVersions*** for strings and arrays. Nullable strings are also represented in documentation as:

```
transactional_id => NULLABLE_STRING
```

Nullable pameters represented in ***"(version)+"*** format

```
"nullableVersions": "2+
```


In Fluvio ***nullable*** is representd through ***Option*** 

```
#[fluvio_kf(versions="1-3")]
trancation_id: Option<String>

#[fluvio_kf(versions="2")]
trandactions: Option<Vec<TranactionObj>>

```

#### Ignorable

Ignorable are flags that are translated into annotations

```
#[fluvio_kf(min_version = 2, max_version = 4, ignorable)]
```


#### Example

Code generator for for Fetch Request (only top struct displayed)

```
kf_api!(
    #[fluvio_kf(api_min_version = 0, api_max_version = 10, api_key = 1 response = "FetchResponse")]
    pub struct KfFetchRequest {
        pub replica_id: i32,
        pub max_wait: i32,
        pub min_bytes: i32,

        #[fluvio_kf(min_version = 3, ignorable)]
        pub max_bytes: i32,

        #[fluvio_kf(min_version = 4)]
        pub isolation_level: i8,

        #[fluvio_kf(min_version = 7)]
        pub session_id: i32,

        #[fluvio_kf(min_version = 7)]
        pub epoch: i32,
        pub topics: Vec<FetchableTopic>,

        #[fluvio_kf(min_version = 7)]
        pub forgotten: Vec<ForgottenTopic>,
    }
}
...
```

## Tips

Grab parameters from **Kafka Json** files:

```
grep -ho "\"versions\": *.\+" input-json/* 

grep -ho "\"nullableVersions\": *.\+" input-json/*

grep -ho "\"ignorable\": *.\+" input-json/*

```




