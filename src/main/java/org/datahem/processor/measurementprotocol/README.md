#Process google analytics (measurement protocol) hits and store to bigquery

Description of custom parameters: [MeasurementProtocolPipeline_metadata](./MeasurementProtocolPipeline_metadata)

# 2. Execute jobs

There are two ways to execute the streaming mesurement protocol processor to read pubsub subscription, process payload to enitites and store entities to bigquery and publish to pubsub topic. 

Before excuting job, change worker parameters to desired values (control number of workers and worker configuration) and change pattern parameter values to the java REGEX that meet your requirements.

## 2.1 Streaming Measurement Protocol Pipeline

**$PROJECT_ID :** your google project id

**$VERSION :** the datahem version used

**$TRACKING_ID :** the property ID tracked in google analytics/measurement protocol, ex. UA-1234567-89

```shell
#Set variables in linux

PROJECT_ID='my-prod-project'
VERSION='0.5'
TRACKING_ID='UA-1234567-89'
```

### 2.1.A. Compile and execute job

```shell
mvn compile exec:java \
      -Dexec.mainClass=org.datahem.processor.measurementprotocol.MeasurementProtocolPipeline \
      -Dexec.args=" \
      --project=$PROJECT_ID \
      --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
      --gcpTempLocation=gs://$PROJECT_ID-processor/gcptemp/ \
      --runner=DataflowRunner \
      --zone=europe-west1-b \
      --region=europe-west1 \
      --numWorkers=1 \
      --maxNumWorkers=1 \
      --diskSizeGb=20 \
      --workerMachineType=n1-standard-1 \
      --pubsubTopic=projects/$PROJECT_ID/topics/$TRACKING_ID-entities \
      --pubsubSubscription=projects/$PROJECT_ID/subscriptions/$TRACKING_ID-processor \
      --bigQueryTableSpec=$TRACKING_ID.entities \
      --ignoredReferersPattern=\".*mysite\\.com.*\" \
      --searchEnginesPattern=\".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*\" \
      --socialNetworksPattern=\".*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*\" \
      --includedHostnamesPattern=\".*\" \
      --excludedBotsPattern=\".*bot.*|.*spider.*|.*crawler.*\" \
      --siteSearchPattern=\".*q=(([^&#]*)|&|#|$)\" \
      --timeZone=Europe/Stockholm"
```

### 2.1.B. Create and execute dataflow template

```shell
# create template

mvn compile exec:java \
     -Dexec.mainClass=org.datahem.processor.measurementprotocol.MeasurementProtocolPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=$PROJECT_ID \
                  --zone=europe-west1-b \
                  --region=europe-west1 \
                  --stagingLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/staging \
                  --templateLocation=gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/measurementprotocol/MeasurementProtocolPipeline \
                  --workerMachineType=n1-standard-1 \
                  --diskSizeGb=20"
```

```shell
# execute template

gcloud beta dataflow jobs run ua123456789-processor \
--gcs-location gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/templates/measurementprotocol/MeasurementProtocolPipeline \
--zone=europe-west1-b \
--region=europe-west1 \
--max-workers=1 \
--parameters \
pubsubSubscription=projects/$PROJECT_ID/subscriptions/$TRACKING_ID-processor,\
pubsubTopic=projects/$PROJECT_ID/topics/$TRACKING_ID-entities,\
bigQueryTableSpec=$TRACKING_ID.entities,\
ignoredReferersPattern=".*mathem\\.se.*",\
searchEnginesPattern=".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*",\
socialNetworksPattern=".*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*",\
includedHostnamesPattern=".*",\
excludedBotsPattern=".*bot.*|.*spider.*|.*crawler.*",\
siteSearchPattern=".*q=(([^&#]*)|&|#|$)",\
timeZone="Europe/Stockholm
```

## 2.2 Backfill/Replay Measurement Protocol Pipeline (Batch)

**$PROJECT_ID :** your google project id

**$VERSION :** the datahem version used

**$TRACKING_ID :** the property ID tracked in google analytics/measurement protocol, ex. UA-1234567-89

**$ANUM_TRACKING_ID :** the property ID without dash, ex. UA123456789

```shell
#Set variables in linux

PROJECT_ID='my-prod-project'
VERSION='0.5'
TRACKING_ID='UA-1234567-89'
ANUM_TRACKING_ID='UA123456789'
IGNORED_REFERERS='.*mysite\\.com.*'
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
      --zone=europe-west1-b \
      --region=europe-west1 \
      --numWorkers=1 \
      --maxNumWorkers=1 \
      --diskSizeGb=20 \
      --workerMachineType=n1-standard-1 \
      --bigQueryTableSpec=$TRACKING_ID.entities \
      --query=\"SELECT data FROM \`$PROJECT_ID.backup.$ANUM_TRACKING_ID\` \" \
      --ignoredReferersPattern=\"$IGNORED_REFERERS\" \
      --searchEnginesPattern=\".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*\" \
      --socialNetworksPattern=\".*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*\" \
      --includedHostnamesPattern=\".*mysite\\.com.*\" \
      --excludedBotsPattern=\".*bot.*|.*spider.*|.*crawler.*\" \
      --siteSearchPattern=\".*q=(([^&#]*)|&|#|$)\" \
      --timeZone=Europe/Stockholm"
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
      --zone=europe-west1-b \
      --region=europe-west1 \
      --numWorkers=1 \
      --maxNumWorkers=1 \
      --diskSizeGb=20 \
      --workerMachineType=n1-standard-1 \
      --bigQueryTableSpec=$TRACKING_ID.entities \
      --query=\"SELECT data FROM \`$PROJECT_ID.backup.$ANUM_TRACKING_ID\` \" \
      --ignoredReferersPattern=\".*mysite\\.com.*\" \
      --searchEnginesPattern=\".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*\" \
      --socialNetworksPattern=\".*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*\" \
      --includedHostnamesPattern=\".*\" \
      --excludedBotsPattern=\".*bot.*|.*spider.*|.*crawler.*\" \
      --siteSearchPattern=\".*q=(([^&#]*)|&|#|$)\" \
      --timeZone=Europe/Stockholm"
```

```shell
gcloud beta dataflow jobs run mpbackfill \
--gcs-location gs://$PROJECT_ID-processor/$VERSION/org/datahem/processor/measurementprotocol/MeasurementProtocolBackfillPipeline \
--zone=europe-west1-b \
--region=europe-west1 \
--max-workers=1 \
--parameters "bigQueryTableSpec=$TRACKING_ID.entities,\
query=\"SELECT data FROM \`$PROJECT_ID.backup.$ANUM_TRACKING_ID\` \",\
ignoredReferersPattern=\".*mysite\\.com.*\",\
searchEnginesPattern=\".*www\\.google\\..*|.*www\\.bing\\..*|.*search\\.yahoo\\..*\",\
socialNetworksPattern=\".*facebook\\..*|.*instagram\\..*|.*pinterest\\..*|.*youtube\\..*|.*linkedin\\..*|.*twitter\\..*\",\
includedHostnamesPattern=\".*\",\
excludedBotsPattern=\".*bot.*|.*spider.*|.*crawler.*\",\
siteSearchPattern=\".*q=(([^&#]*)|&|#|$)\",\
timeZone=Europe/Stockholm"
```