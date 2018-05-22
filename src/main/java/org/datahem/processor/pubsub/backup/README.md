#Backup pubsub to bigquery

Description of custom parameters: [MeasurementProtocolPipeline_metadata](./MeasurementProtocolPipeline_metadata)


# 1. Set variables

**$PROJECT_ID :** your google project id

**$VERSION :** the datahem version used

**$STREAM_ID :** the ID (name) used to set up a streaming source with the infrastructor script

```shell
#Set variables in linux

PROJECT_ID='my-prod-project'
VERSION='0.5'
STREAM_ID='UA-1234567-89'
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
      --zone=europe-west1-b \
      --region=europe-west1 \
      --numWorkers=1 \
      --maxNumWorkers=1 \
      --diskSizeGb=5 \
      --workerMachineType=n1-standard-1 \
      --pubsubSubscription=projects/$PROJECT_ID/subscriptions/$STREAM_ID-backup \
      --bigQueryTableSpec=backup.$STREAM_ID"
```

Create template

```shell
 mvn compile exec:java \
     -Dexec.mainClass=org.datahem.processor.pubsub.backup.PubSubBackupPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=$PROJECT_ID \
                  --zone=europe-west1-b \
                  --region=europe-west1 \
                  --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
                  --templateLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/pubsub/backup/PubSubBackupPipeline \
                  --workerMachineType=n1-standard-1 \
                  --diskSizeGb=10"
```

Run job

```shell
gcloud beta dataflow jobs run mpbackup \
--gcs-location gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/pubsub/backup/PubSubBackupPipeline \
--zone=europe-west1-b \
--region=europe-west1 \
--max-workers=1 \
--parameters pubsubSubscription=projects/$PROJECT_ID/subscriptions/$STREAM_ID-backup,\
bigQueryTableSpec=backup.$STREAM_ID
```

