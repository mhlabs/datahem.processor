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