Description of custom parameters: [MeasurementProtocolPipeline_metadata](./MeasurementProtocolPipeline_metadata)

**<PROJECT ID> :** your google project id

**<VERSION> :** the datahem version used

**<STREAM ID> **: the ID (name) used to set up a streaming source with the infrastructor script

Backup pubsub to bigquery

compile and run

```shell
   mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.pubsub.backup.PubSubBackupPipeline \
      -Dexec.args=" \
      --project=mathem-data \
      --stagingLocation=gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/staging \
      --gcpTempLocation=gs://<PROJECT ID>-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=europe-west1-b \
      --numWorkers=1 \
      --maxNumWorkers=1 \
      --diskSizeGb=5 \
      --workerMachineType=n1-standard-1 \
      --pubsubSubscription=projects/<PROJECT ID>/subscriptions/<STREAM ID> \
      --bigQueryTableSpec=backup.<STREAM ID>"
```

Create template

```shell
 mvn compile exec:java \
     -Dexec.mainClass=org.datahem.processor.pubsub.backup.PubSubBackupPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=mathem-data \
                  --stagingLocation=gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/staging \
                  --templateLocation=gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/pubsub/backup/PubSubBackupPipeline \
                  --workerMachineType=n1-standard-1 \
                  --diskSizeGb=10"    
```

Run job

```shell
gcloud beta dataflow jobs run mpbackup \
        --gcs-location gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/pubsub/backup/PubSubBackupPipeline \
        --zone=europe-west1-b \
        --max-workers=1 \
        --parameters pubsubSubscription=projects/<PROJECT ID>/subscriptions/<STREAM ID>-backup,bigQueryTableSpec=backup.<STREAM ID>
```

