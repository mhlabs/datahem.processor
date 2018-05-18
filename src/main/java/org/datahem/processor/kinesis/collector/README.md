compile and run

```shell
   mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.kinesis.collector.StreamCollectorPipeline \
      -Dexec.args=" \
      --project=<PROJECT ID> \
      --stagingLocation=gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/staging \
      --gcpTempLocation=gs://<PROJECT ID>-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=europe-west1-b \
      --region=europe-west1 \
      --numWorkers=1 \
      --maxNumWorkers=2 \
      --diskSizeGb=25 \
      --workerMachineType=n1-standard-1 \
      --awsKey=<ENCRYPTED KEY> \
      --awsSecret=<ENCRYPTED SECRET> \
      --awsStream=<STREAM> \
      --awsRegion=<REGION> \
      --kmsProjectId=<PROJECT ID> \
      --kmsLocationId=global \
      --kmsKeyRingId=<KEY RING ID> \
      --kmsCryptoKeyId=<CRYPTO KEY ID> \
      --pubsubTopic=projects/<PROJECT ID>/topics/<STREAM> \
      --initialPositionInStream=LATEST"
```

Create template

```shell
mvn compile exec:java \
     -Dexec.mainClass=org.datahem.processor.kinesis.collector.StreamCollectorPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=<PROJECT ID> \
                  --stagingLocation=gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/staging \
                  --templateLocation=gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/kinesis/collector/OrderCollectorPipeline \
                  --workerMachineType=n1-standard-1 \
                  --zone=europe-west1-b \
                  --region=europe-west1 \
                  --diskSizeGb=25 \
                  --awsKey=<KEY> \
                  --awsSecret=<SECRET> \
                  --awsStream=<STREAM> \
                  --awsRegion=<REGION> \
                  --kmsProjectId=<PROJECT ID> \
                  --kmsLocationId=global \
                  --kmsKeyRingId=<KEY RING ID> \
                  --kmsCryptoKeyId=<CRYPTO KEY ID> \
                  --initialPositionInStream=LATEST"   
```

//Run job                  

```shell
gcloud beta dataflow jobs run kinesisToPubsub \
        --gcs-location gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/kinesis/collector/OrderCollectorPipeline \
        --zone=europe-west1-b \
        --region=europe-west1 \
        --max-workers=2 \
        --parameters pubsubTopic=projects/<PROJECT ID>/topics/<TOPIC ID>
```
