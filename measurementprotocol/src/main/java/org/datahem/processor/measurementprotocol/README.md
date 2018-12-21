#Process google analytics (measurement protocol) hits and store to bigquery

Description of custom parameters: [MeasurementProtocolPipeline_metadata](./MeasurementProtocolPipeline_metadata)

# 2. Execute jobs

There are two ways to execute the streaming mesurement protocol processor to read pubsub subscription, process payload to enitites and store entities to bigquery and publish to pubsub topic. 

Before excuting job, change worker parameters to desired values (control number of workers and worker configuration) and change pattern parameter values to the java REGEX that meet your requirements.

## 2.1 Streaming Measurement Protocol Pipeline

```shell
#Set variables in linux

#DataHem generic settings
PROJECT_ID='' # Required. Your google project id. Example: 'my-prod-project'
VERSION='' # Required. DataHem version used. Example: '0.5'

#Dataflow settings
DF_REGION= # Optional. Default: us-central1
DF_ZONE= # Optional. Default:  an availability zone from the region set in DF_REGION.
DF_NUM_WORKERS= # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 2
DF_MAX_NUM_WORKERS= # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 5
DF_DISK_SIZE_GB= # Optional. Default: Size defined in your Cloud Platform project. Minimum is 30. Example: 50
DF_WORKER_MACHINE_TYPE='' # Optional. Default: The Dataflow service will choose the machine type based on your job. Example: 'n1-standard-1'

#Measurement Protocol Pipeline settings
CONFIG=''  # Required. JSON. Example: CONFIG='{"name":"<accountName>","properties":[{"id":"ua123456789","views":[{"id":"master","searchEnginesPattern":".*(www.google.|www.bing.|search.yahoo.).*","ignoredReferersPattern":".*(datahem.org|klarna.com).*","socialNetworksPattern":".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*","includedHostnamesPattern":".*(datahem.org).*","excludedBotsPattern":".*(^$|bot|spider|crawler).*","siteSearchPattern":".*q=(([^&#]*)|&|#|$)","timeZone":"Europe/Stockholm", "pubSubTopic":"ua123456789-master"},{"id":"unfiltered","searchEnginesPattern":".*(www.google.|www.bing.|search.yahoo.).*","ignoredReferersPattern":".*(www.datahem.org).*","socialNetworksPattern":".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*","includedHostnamesPattern":".*","excludedBotsPattern":"a^","siteSearchPattern":".*q=(([^&#]*)|&|#|$)","timeZone":"Europe/Stockholm"}]}]}'
# Specify an account with one or multiple properties. Each property can have one or more views.
```

```json
{"name":"abcd",
"properties":[
    {"id":"ua123456789",
    "views":[
        {"id":"master",
        "searchEnginesPattern":".*(www.google.|www.bing.|search.yahoo.).*",
        "ignoredReferersPattern":".*(datahem.org|klarna.com).*",
        "socialNetworksPattern":".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*",
        "includedHostnamesPattern":".*(datahem.org).*",
        "excludedBotsPattern":".*(^$|bot|spider|crawler).*",
        "siteSearchPattern":".*q=(([^&#]*)|&|#|$)",
        "timeZone":"Europe/Stockholm",
        "pubSubTopic":"ua123456789-master"},
        {"id":"unfiltered",
        "searchEnginesPattern":".*(www.google.|www.bing.|search.yahoo.).*",
        "ignoredReferersPattern":".*(www.datahem.org).*",
        "socialNetworksPattern":".*(facebook.|instagram.|pinterest.|youtube.|linkedin.|twitter.).*",
        "includedHostnamesPattern":".*",
        "excludedBotsPattern":"a^",
        "siteSearchPattern":".*q=(([^&#]*)|&|#|$)",
        "timeZone":"Europe/Stockholm"}
    ]}
]}
```

# name -> Account name. [REQUIRED, STRING]
# properties[].id -> Property id [REQUIRED, STRING] Name of the pubsubSubscription to read from.
# properties[].views[].id -> View id. [REQUIRED, STRING]
# properties[].views[].searchEnginesPattern -> Search Engine Regex Pattern [REQUIRED, STRING] Regex-pattern to match traffic from search engines.
# properties[].views[].ignoredReferrersPattern -> Ignored Referrers Regex Pattern [REQUIRED, STRING] Regex-pattern to match referrers that should be ignored as traffic source.
# properties[].views[].socialNetworksPattern -> Social Networks Regex Pattern [REQUIRED, STRING] Regex-pattern to match traffic from social networks.
# properties[].views[].includedHostnamesPattern -> Included Hostnames Regex Pattern [REQUIRED, STRING] Regex-pattern to match hostnames that should be included, rest is excluded.
# properties[].views[].excludedBotsPattern -> Excluded Bots Regex Pattern [REQUIRED, STRING] Regex-pattern to match user-agents that should be excluded.
# properties[].views[].siteSearchPattern -> Site Search Regex Pattern [REQUIRED, STRING] Regex-pattern to match site search and search terms.
# properties[].views[].timeZone -> Timezone [REQUIRED, STRING] Local timezone.
# properties[].views[].pubSubTopic -> PubSubTopic [OPTIONAL, STRING] Name of the PubSub topic to publish enriched entities to.

### 2.1.A. Compile and execute job

```shell
mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.measurementprotocol.MeasurementProtocolPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --jobName=measurementprotocol \
      --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --config='$CONFIG' \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE"
```

### 2.1.B. Create and execute dataflow template

```shell
# create template

mvn compile exec:java \
     -Dexec.mainClass=org.datahem.processor.measurementprotocol.MeasurementProtocolPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=$PROJECT_ID \
                  --zone=$DF_ZONE \
                  --region=$DF_REGION \
                  --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
                  --templateLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/measurementprotocol/MeasurementProtocolPipeline \
                  --workerMachineType=$DF_WORKER_MACHINE_TYPE \
                  --diskSizeGb=$DF_DISK_SIZE_GB"
```

```shell
# execute template

gcloud beta dataflow jobs run $STREAM_ID-processor \
--gcs-location gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/measurementprotocol/MeasurementProtocolPipeline \
--zone=$DF_ZONE \
--region=$DF_REGION \
--max-workers=$DF_MAX_NUM_WORKERS \
--parameters \
--config=$CONFIG
```



## 2.2 Backfill/Replay Measurement Protocol Pipeline (Batch)

```shell
QUERY='' # Required. Query to pull out the records from backup that you want to reprocess. Example: 'SELECT data FROM \`$PROJECT_ID.backup.$STREAM_ID\`'
```

### 2.2.A. Compile and execute job
```shell
mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.measurementprotocol.MeasurementProtocolBackfillPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --tempLocation=gs://$PROJECT_ID-processor/temp/ \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE \
      --query=\"$QUERY\" \
      --bigQueryTableSpec=$STREAM_ID.entities \
      --ignoredReferersPattern=\"$IGNORED_REFERERS_PATTERN\" \
      --searchEnginesPattern=\"$SEARCH_ENGINES_PATTERN\" \
      --socialNetworksPattern=\"$SOCIAL_NETWORKS_PATTERN\" \
      --includedHostnamesPattern=\"$INCLUDED_HOSTNAMES\" \
      --excludedBotsPattern=\"$EXCLUDED_BOTS_PATTERN\" \
      --siteSearchPattern=\"$SITE_SEARCH_PATTERN\" \
      --timeZone=$TIME_ZONE"
```

### 2.1.B. Create and execute dataflow template
```shell
mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.measurementprotocol.MeasurementProtocolBackfillPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --tempLocation=gs://$PROJECT_ID-processor/temp/ \
      --templateLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/measurementprotocol/MeasurementProtocolBackfillPipeline \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE \
```

```shell
gcloud beta dataflow jobs run $STREAM_ID-backfill \
--gcs-location gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/measurementprotocol/MeasurementProtocolBackfillPipeline \
--zone=$DF_ZONE \
--region=$DF_REGION \
--max-workers=$DF_MAX_NUM_WORKERS \
--parameters \
query=\"$QUERY\",\
bigQueryTableSpec=$STREAM_ID.entities,\
ignoredReferersPattern=$IGNORED_REFERERS_PATTERN,\
searchEnginesPattern=$SEARCH_ENGINES_PATTERN,\
socialNetworksPattern=$SOCIAL_NETWORKS_PATTERN,\
includedHostnamesPattern=$INCLUDED_HOSTNAMES,\
excludedBotsPattern=$EXCLUDED_BOTS_PATTERN,\
siteSearchPattern=$SITE_SEARCH_PATTERN,\
timeZone=$TIME_ZONE
```