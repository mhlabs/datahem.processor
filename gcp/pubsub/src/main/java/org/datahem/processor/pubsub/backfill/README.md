# Query bigquery backup table and publish as pubsub messages on a pubsub topic 


## Streaming Backfill Pipeline

```shell
#Set variables in linux

#DataHem generic settings
PROJECT_ID='' # Required. Your google project id. Example: 'my-prod-project'

#Dataflow settings
DF_REGION= # Optional. Default: us-central1
DF_ZONE= # Optional. Default:  an availability zone from the region set in DF_REGION.
DF_NUM_WORKERS= # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 2
DF_MAX_NUM_WORKERS= # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 5
DF_DISK_SIZE_GB= # Optional. Default: Size defined in your Cloud Platform project. Minimum is 30. Example: 50
DF_WORKER_MACHINE_TYPE='' # Optional. Default: The Dataflow service will choose the machine type based on your job. Example: 'n1-standard-1'

#Job settings
PUBSUB_TOPIC=''
QUERY=''

```shell
mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.pubsub.backfill.StreamBackfillPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --jobName=StreamBackfill \
      --stagingLocation=gs://$PROJECT_ID-processor/staging/ \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --tempLocation=gs://$PROJECT_ID-processor/temp/ \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE \
      --pubsubTopic=$PUBSUB_TOPIC \
      --query='$QUERY'"
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