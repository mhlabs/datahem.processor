# datahem.processor
Process bounded and unbounded data and write to PubSub and BigQuery, currently using Google Dataflow (Apache Beam) and supports processing of Google Analytics hits and AWS Kinesis events

# Version:
## 0.7.2 (2018-09-21): Measurement Protocol Pipeline Test & Google Experiment entity
Beam pipeline tests for all measurement protocol entities (16 tests).
New entity capturing experiments from both Google Optimize and Content Experiments

## 0.7.1 (2018-08-27): Measurement Protocol Site Search Term URL decoding
Fixed URL decoding of site search term.

## 0.7.0 (2018-06-14): Measurement Protocol Camel Case naming and custom dimension/metrics suffixes
Changed the field naming to camel case instead of snake and fixed custom dimension and metrics suffixes to support multiple dimensions and metrics in bigquery.