#AWS Kinesis Order Stream

Read pubsub and write parsed payload to bigquery and pubsub

# 1. Set variables

```shell
#DataHem generic settings
PROJECT_ID='mathem-ml-datahem-test' # Required. Your google project id. Example: 'my-prod-project'
VERSION='0.11.3' # Required. DataHem version used. Example: '0.5'

#Dataflow settings
DF_REGION='europe-west1' # Optional. Default: us-central1
DF_ZONE='europe-west1-b' # Optional. Default:  an availability zone from the region set in DF_REGION.
DF_NUM_WORKERS=1 # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 2
DF_MAX_NUM_WORKERS=1 # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 5
DF_DISK_SIZE_GB=30 # Optional. Default: Size defined in your Cloud Platform project. Minimum is 30. Example: 50
DF_WORKER_MACHINE_TYPE='n1-standard-1' # Optional. Default: The Dataflow service will choose the machine type based on your job. Example: 'n1-standard-1'

#AWS Kinesis Order Stream Pipeline settings
STREAM_ID='member-service-MemberProcessTopic' # Required. The name of the stream. Example: 'order-stream'
TIME_ZONE='Europe/Stockholm' #Optional. Define local time zone (ex. Europe/Stockholm) for date field used for partitioning. Default: 'Etc/UTC'
```

## compile and run

```shell
   mvn compile exec:java \
      -Dexec.mainClass se.mathem.processor.aws.sns.member.MemberStreamPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --jobName=members \
      --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/com/datahem/processor/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE \
      --pubsubSubscription=projects/$PROJECT_ID/subscriptions/$STREAM_ID \
      --bigQueryPartitionTimeZone=$TIME_ZONE \
      --bigQueryTableSpec=streams.members"
```

mvn compile exec:java \
      -Dexec.mainClass=se.mathem.processor.aws.sns.member.MemberStreamPipeline \
      -Dexec.args=" \
      --project=mathem-ml-datahem-test \
      --jobName=members \
      --stagingLocation=gs://mathem-ml-datahem-test-processor/98/com/datahem/processor/staging \
      --gcpTempLocation=gs://mathem-ml-datahem-test-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone='europe-west1-b' \
      --region='europe-west1' \
      --numWorkers=1 \
      --maxNumWorkers=2 \
      --diskSizeGb=30 \
      --workerMachineType='n1-standard-1' \
      --pubsubSubscription=projects/mathem-ml-datahem-test/subscriptions/member-service-MemberProcessTopic \
      --bigQueryPartitionTimeZone='Europe/Stockholm' \
      --bigQueryTableSpec=streams.members2"

mvn compile exec:java \
      -Dexec.mainClass=se.mathem.processor.aws.sns.member.MemberStreamPipeline \
      -Dexec.args=" \
      --project=mathem-ml-datahem-prod \
      --jobName=members \
      --stagingLocation=gs://mathem-ml-datahem-prod-processor/98/com/datahem/processor/staging \
      --gcpTempLocation=gs://mathem-ml-datahem-prod-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone='europe-west1-b' \
      --region='europe-west1' \
      --numWorkers=1 \
      --maxNumWorkers=2 \
      --diskSizeGb=30 \
      --workerMachineType='n1-standard-1' \
      --pubsubSubscription=projects/mathem-ml-datahem-prod/subscriptions/member-service-MemberProcessTopic \
      --bigQueryPartitionTimeZone='Europe/Stockholm' \
      --bigQueryTableSpec=streams.members"

mvn compile exec:java \
      -Dexec.mainClass se.mathem.processor.aws.sns.member.MemberStreamPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/com/datahem/processor/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE \
      --pubsubSubscription=projects/$PROJECT_ID/subscriptions/$STREAM_ID \
      --bigQueryPartitionTimeZone=$TIME_ZONE \
      --bigQueryTableSpec=streams.testing"



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