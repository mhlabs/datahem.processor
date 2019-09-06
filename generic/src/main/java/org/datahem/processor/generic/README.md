# GenericStreamPipeline

Read pubsub containing json-payload and write parsed (using protobuf schema) payload to bigquery

# 1. Set variables

```shell
# DataHem generic settings
PROJECT_ID='' # Required. Your google project id. Example: 'my-project'

# Dataflow settings
DF_REGION='europe-west1' # Optional. Default: us-central1
DF_ZONE='europe-west1-b' # Optional. Default:  an availability zone from the region set in DF_REGION.
DF_NUM_WORKERS=1 # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 2
DF_MAX_NUM_WORKERS=1 # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 5
DF_DISK_SIZE_GB=30 # Optional. Default: Size defined in your Cloud Platform project. Minimum is 30. Example: 50
DF_WORKER_MACHINE_TYPE='n1-standard-1' # Optional. Default: The Dataflow service will choose the machine type based on your job. Example: 'n1-standard-1'
JOB_NAME='' # Optional. Default: The Dataflow service will name the job if not provided

# GenericStream Pipeline settings
BUCKET_NAME=${PROJECT_ID}-schema-registry # Required. Example. ${PROJECT_ID}-schema-registry
FILE_DESCRIPTOR_NAME='' # Required. Example: schemas.desc
FILE_DESCRIPTOR_PROTO_NAME='' # Required. Example: datahem/protobuf/order/v1/order.proto'
MESSAGE_TYPE='' # Required. Example: order
PUBSUB_SUBSCRIPTION='' # Required. The name of the stream. Example: 'order-stream'
BIGQUERY_TABLE_SPEC='' # Required. Example: streams.orders
TAXONOMY_RESOURCE_PATTERN='' # Optional. Default: '.*'. Example: '7564324984364324'  (taxonomy id)
```

## compile and run

```shell
mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.generic.GenericStreamPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --jobName=$JOB_NAME \
      --stagingLocation=gs://$PROJECT_ID-processor/org/datahem/processor/generic/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE \
      --bucketName=$BUCKET_NAME \
      --fileDescriptorName=$FILE_DESCRIPTOR_NAME \
      --descriptorFullName=$DESCRIPTOR_FULL_NAME \
      --taxonomyResourcePattern=$TAXONOMY_RESOURCE_PATTERN \
      --pubsubSubscription=projects/$PROJECT_ID/subscriptions/$PUBSUB_SUBSCRIPTION \
      --bigQueryTableSpec=$BIGQUERY_TABLE_SPEC"
```

```shell
mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.generic.GenericStreamPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --jobName=$JOB_NAME \
      --stagingLocation=gs://$PROJECT_ID-processor/org/datahem/processor/generic/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE \
      --bucketName=$BUCKET_NAME \
      --fileDescriptorName=$FILE_DESCRIPTOR_NAME \
      --fileDescriptorProtoName=$FILE_DESCRIPTOR_PROTO_NAME \
      --messageType=$MESSAGE_TYPE \
      --pubsubSubscription=projects/$PROJECT_ID/subscriptions/$PUBSUB_SUBSCRIPTION \
      --bigQueryTableSpec=$BIGQUERY_TABLE_SPEC"
```



## Create template

```shell
   mvn compile exec:java \
      -Dexec.mainClass=com.datahem.processor.kinesis.order.OrderStreamPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/com/datahem/processor/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --templateLocation=gs://$PROJECT_ID-processor/$VERSION/com/datahem/processor/kinesis/order/OrderStreamPipeline \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE"
```

## Run template

```shell
 gcloud beta dataflow jobs run $STREAM_ID-processor \
--gcs-location gs://$PROJECT_ID-processor/$VERSION/com/datahem/processor/kinesis/order/OrderStreamPipeline \
--zone=$DF_ZONE \
--region=$DF_REGION \
--max-workers=$DF_MAX_NUM_WORKERS \
--parameters \
pubsubSubscription=projects/$PROJECT_ID/subscriptions/$STREAM_ID-processor,\
bigQueryPartitionTimeZone=$TIME_ZONE,\
bigQueryTableSpec=$STREAM_ID.entities
```