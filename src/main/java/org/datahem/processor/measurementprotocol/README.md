Description of custom parameters: [MeasurementProtocolPipeline_metadata](./MeasurementProtocolPipeline_metadata)

**<PROJECT ID> :** your google project id

**<VERSION> :** the datahem version used

**<TRACKING ID> **: the property ID tracked in google analytics/measurement protocol, ex. UA-1234567-89

**<alphanumeric TRACKING ID> :** the property ID without dash, ex. UA123456789

Execute streaming mesurement protocol processor to read pubsub subscription, process payload to enitites and store entities to bigquery and publish to pubsub topic

```shell
mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.measurementprotocol.MeasurementProtocolPipeline \
      -Dexec.args=" \
      --project=<PROJECT ID> \
      --stagingLocation=gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/staging \
      --gcpTempLocation=gs://<PROJECT ID>-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=europe-west1-b \
      --region=europe-west1 \
      --numWorkers=1 \
      --maxNumWorkers=1 \
      --diskSizeGb=20 \
      --workerMachineType=n1-standard-1 \
      --pubsubTopic=projects/<PROJECT ID>/topics/<TRACKING ID>-entities \
      --pubsubSubscription=projects/<PROJECT ID>/subscriptions/<TRACKING ID>-processor \
      --bigQueryTableSpec=<alphanumeric TRACKING ID>.entities \
      --ignoredReferersPattern=\".*mathem\\.se.*\" \
      --searchEnginesPattern=\".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*\" \
      --socialNetworksPattern=\".*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*\" \
      --includedHostnamesPattern=\".*\" \
      --excludedBotsPattern=\".*bot.*|.*spider.*|.*crawler.*\" \
      --siteSearchPattern=\".*q=(([^&#]*)|&|#|$)\" \
      --timeZone=Europe/Stockholm"
```

Create a dataflow template

```shell
mvn compile exec:java \
     -Dexec.mainClass=org.datahem.processor.measurementprotocol.MeasurementProtocolPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=<PROJECT ID> \
                  --zone=europe-west1-b \
                  --region=europe-west1 \
                  --stagingLocation=gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/staging \
                  --templateLocation=gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/measurementprotocol/MeasurementProtocolPipeline \
                  --workerMachineType=n1-standard-1 \
                  --diskSizeGb=20"
```

Execute a job from commandline using the template

```shell
gcloud beta dataflow jobs run ua123456789-processor \
--gcs-location gs://<PROJECT ID>-processor/<VERSION>/org/datahem/processor/templates/measurementprotocol/MeasurementProtocolPipeline \
--zone=europe-west1-b \
--region=europe-west1 \
--max-workers=1 \
--parameters \
pubsubSubscription=projects/<PROJECT ID>/subscriptions/<TRACKING ID>-processor,\
pubsubTopic=projects/<PROJECT ID>/topics/<TRACKING ID>-entities,\
bigQueryTableSpec=<alphanumeric TRACKING ID>.entities,\
ignoredReferersPattern=".*mathem\\.se.*",\
searchEnginesPattern=".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*",\
socialNetworksPattern=".*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*",\
includedHostnamesPattern=".*",\
excludedBotsPattern=".*bot.*|.*spider.*|.*crawler.*",\
siteSearchPattern=".*q=(([^&#]*)|&|#|$)",\
timeZone="Europe/Stockholm"
```