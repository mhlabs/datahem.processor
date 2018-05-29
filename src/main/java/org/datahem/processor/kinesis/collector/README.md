#AWS Kinesis stream to pubsub

Encrypt AWS Key and Secret with GCP KMS to keep credentials safe.
First you must create a gcp kms key ring and key. You can do that through the console or with command line.

```shell
KMS_LOCATION_ID='global' # Required.
KMS_KEY_RING_ID='aws' # Required.
KMS_CRYPTO_KEY_ID='gcp_kinesis_consumer' # Required.
AWS_KEY='blabla' # Required. The AWS key you want to encrypt.
AWS_SECRET= # Required. The AWS secret you want to encrypt.

gcloud kms keyrings create $KMS_KEY_RING_ID --location $KMS_LOCATION_ID

gcloud kms keys create $KMS_CRYPTO_KEY_ID --location $KMS_LOCATION_ID --keyring $KMS_KEY_RING_ID --purpose encryption

echo -n $AWS_KEY | base64
echo -n $AWS_SECRET | base64
```
The key and secret must be base64 encoded before being submitted to the KMS encryption API to get an encrypted key. 

Since gcloud only supports file for plaintext and ciphertext it may be easier to use GCP API explorer to encrypt the key and secret https://developers.google.com/apis-explorer/?hl=en_US#p/cloudkms/v1/cloudkms.projects.locations.keyRings.cryptoKeys.encrypt

# 1. Set variables

```shell
#DataHem generic settings
PROJECT_ID='' # Required. Your google project id. Example: 'my-prod-project'
VERSION='' # Required. DataHem version used. Example: '0.5'

#Dataflow settings
DF_REGION='europe-west1' # Optional. Default: us-central1
DF_ZONE='europe-west1-b' # Optional. Default:  an availability zone from the region set in DF_REGION.
DF_NUM_WORKERS=1 # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 2
DF_MAX_NUM_WORKERS=1 # Optional. Default: Dataflow service will determine an appropriate number of workers. Example: 5
DF_DISK_SIZE_GB=30 # Optional. Default: Size defined in your Cloud Platform project. Minimum is 30. Example: 50
DF_WORKER_MACHINE_TYPE='n1-standard-1' # Optional. Default: The Dataflow service will choose the machine type based on your job. Example: 'n1-standard-1'

#AWS Kinesis Stream Collector Pipeline settings
ENCRYPTED_AWS_KEY # Required. The encrypted AWS Key.
ENCRYPTED_AWS_SECRET # Required. The encrypted AWS Secret.
AWS_STREAM # Required. The name of the AWS stream to read from. Example: 'customer-stream'
AWS_REGION # Optional. The region of the AWS stream. Default: 'europe-west-1'
KMS_PROJECT_ID # Required. The project id of the GCP KMS used for encryption. Example: 'my-prod-project'
KMS_LOCATION_ID # Optional. The project id of the GCP KMS used for encryption. Default: 'global'
KMS_KEY_RING_ID # Required. The key ring id in the GCP KMS used for encryption. 
KMS_CRYPTO_KEY_ID # Required. The key id in the GCP KMS used for encryption. 
STREAM_ID= # Required. The name of the stream. Example: 'ua123456789'
AWS_INITIAL_POSITION_IN_STREAM # Optional. Initial Position In AWS Kinesis Stream. Example: 'LATEST' or 'TRIM_HORIZON' or 'AT_TIMESTAMP'

```

compile and run

```shell
   mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.kinesis.collector.StreamCollectorPipeline \
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
      --awsKey=$ENCRYPTED_AWS_KEY \
      --awsSecret=$ENCRYPTED_AWS_SECRET \
      --awsStream=$AWS_STREAM \
      --awsRegion=$AWS_REGION \
      --kmsProjectId=$KMS_PROJECT_ID \
      --kmsLocationId=$KMS_LOCATION_ID \
      --kmsKeyRingId=$KMS_KEY_RING_ID \
      --kmsCryptoKeyId=$KMS_CRYPTO_KEY_ID \
      --pubsubTopic=projects/$PROJECT_ID/topics/$STREAM_ID \
      --initialPositionInStream=$AWS_INITIAL_POSITION_IN_STREAM"
```

Create template

```shell
mvn compile exec:java \
     -Dexec.mainClass=org.datahem.processor.kinesis.collector.StreamCollectorPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=$PROJECT_ID \
                  --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
                  --templateLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/kinesis/collector/$STREAM_ID/CollectorPipeline \
                  --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
                  --zone=$DF_ZONE \
                  --region=$DF_REGION \
                  --numWorkers=$DF_NUM_WORKERS \
                  --maxNumWorkers=$DF_MAX_NUM_WORKERS \
                  --diskSizeGb=$DF_DISK_SIZE_GB \
                  --workerMachineType=$DF_WORKER_MACHINE_TYPE \
                  --awsKey=$ENCRYPTED_AWS_KEY \
                  --awsSecret=$ENCRYPTED_AWS_SECRET \
                  --awsStream=$AWS_STREAM \
                  --awsRegion=$AWS_REGION \
                  --kmsProjectId=$KMS_PROJECT_ID \
                  --kmsLocationId=$KMS_LOCATION_ID \
                  --kmsKeyRingId=$KMS_KEY_RING_ID \
                  --kmsCryptoKeyId=$KMS_CRYPTO_KEY_ID \
                  --pubsubTopic=projects/$PROJECT_ID/topics/$STREAM_ID \
                  --initialPositionInStream=$AWS_INITIAL_POSITION_IN_STREAM"
```

//Run job                  

```shell
gcloud beta dataflow jobs run $STREAM_ID-collector \
        --gcs-location gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/kinesis/collector/$STREAM_ID/CollectorPipeline \
        --zone=$DF_ZONE \
        --region=$DF_REGION \
        --max-workers=$DF_MAX_NUM_WORKERS \
        --parameters pubsubTopic=projects/$PROJECT_ID/topics/$STREAM_ID
```
