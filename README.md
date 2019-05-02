# datahem.processor
Process bounded and unbounded data and write to PubSub and BigQuery, currently using Google Dataflow (Apache Beam) and supports processing of Google Analytics hits and AWS Kinesis events

# Version:
## 1.1.2 (2019-04-26): Streaming backfill pipeline
Query BigQuery backup tables and publish to pubsub for backfill purposes

## 1.1.1 (2019-04-24): Hot-fix remove value provider 

## 1.1.0 (2019-04-16):  Measurement Protocol exclude IP-filter & date and time dimensions
Added support for IP-filters
Modified schema for time dimensions to bigquery types

## 1.0.0 (2019-04-10): AGPL and Measurement Protocol version 2
Changed license to AGPL 3.0 or later
Added version 2 of pipeline for measurement protocol with more strictly typed schema.

## 0.9.1 (2018-12-21): Measurement Protocol Pipeline - replace localTimestamp with localDateTime
Replaced the field localTimestamp of type STRING with a field localDateTime with type DATETIME for easier analysis in BigQuery. 

## 0.9.0 (2018-12-20): Measurement Protocol Pipeline restructuring
Reads pubsub messages with request headers stored as pubsub attributes instead of concatenating request querystring body, synced changes with datahem.collector version 0.9.0.
Configure jobs with json (account -> properties -> views) to process multiple properties and views in the same job.
Cleaned up code and updated documentation. See README.md in the measurementprotocol.src.main.java.org.datahem.processor.measurementprotocol folder

## 0.7.2 (2018-09-21): Measurement Protocol Pipeline Test & Google Experiment entity
Beam pipeline tests for all measurement protocol entities (16 tests).
New entity capturing experiments from both Google Optimize and Content Experiments

## 0.7.1 (2018-08-27): Measurement Protocol Site Search Term URL decoding
Fixed URL decoding of site search term.

## 0.7.0 (2018-06-14): Measurement Protocol Camel Case naming and custom dimension/metrics suffixes
Changed the field naming to camel case instead of snake and fixed custom dimension and metrics suffixes to support multiple dimensions and metrics in bigquery.