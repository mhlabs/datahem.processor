#Backup pubsub to bigquery

Description of custom parameters: [MeasurementProtocolPipeline_metadata](./MeasurementProtocolPipeline_metadata)


# 1. Set variables

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

#Pubsub Backup Pipeline settings
STREAM_ID= #Required. The ID (name) used to set up a streaming source with the infrastructor script. Example: 'ua123456789'

```

# 2. Execute jobs

There are two ways to execute the pubsub backup processor pipeline to read pubsub subscription and store payload to bigquery for backup purposes. 

Before excuting job, change worker parameters to desired values (control number of workers and worker configuration).

compile and run

```shell
   mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.pubsub.backup.PubSubBackupPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=$DF_ZONE \
      --region=$DF_REGION \
      --numWorkers=$DF_NUM_WORKERS \
      --maxNumWorkers=$DF_MAX_NUM_WORKERS \
      --diskSizeGb=$DF_DISK_SIZE_GB \
      --workerMachineType=$DF_WORKER_MACHINE_TYPE \
      --pubsubSubscription=projects/$PROJECT_ID/subscriptions/$STREAM_ID-backup \
      --bigQueryTableSpec=backup.$STREAM_ID"
```

Create template

```shell
 mvn compile exec:java \
     -Dexec.mainClass=org.datahem.processor.pubsub.backup.PubSubBackupPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=$PROJECT_ID \
                  --zone=$DF_ZONE \
                  --region=$DF_REGION \
                  --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
                  --templateLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/pubsub/backup/PubSubBackupPipeline \
                  --workerMachineType=$DF_WORKER_MACHINE_TYPE \
                  --diskSizeGb=$DF_DISK_SIZE_GB"
```

Run job

```shell
gcloud beta dataflow jobs run $STREAM_ID-backup \
--gcs-location gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/pubsub/backup/PubSubBackupPipeline \
--zone=$DF_ZONE \
--region=$DF_REGION \
--max-workers=$DF_MAX_NUM_WORKERS \
--parameters pubsubSubscription=projects/$PROJECT_ID/subscriptions/$STREAM_ID-backup,\
bigQueryTableSpec=backup.$STREAM_ID
```