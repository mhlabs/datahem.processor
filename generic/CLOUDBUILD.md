cp /home/robert_sahlin/mhlabs/datahem.processor/generic/target/generic-bundled-0.7.2.jar /home/robert_sahlin/datahem-com/docker/processor/generic/dynamodb

# build, tag and push docker image
version=1.0.0
docker build --tag local/patcher --build-arg version=$version .
docker tag local/patcher datahem/patcher:latest
docker tag local/patcher datahem/patcher:$version
docker push datahem/patcher

gcloud builds submit --tag gcr.io/[PROJECT_ID]/dynamodb .

# manually trig build from directory with cloudbuild.yaml and substitutions
gcloud builds submit --config cloudbuild.yaml --no-source --substitutions=^%^_CONFIG='{"name":"accountName","properties":[{"id":"ua1234567","views":[{"id":"master","searchEnginesPattern":".*(www\\.google\\.|www\\.bing\\.|search\\.yahoo\\.).*","ignoredReferersPattern":".*(datahem\\.org|klarna\\.com).*","socialNetworksPattern":".*(facebook\\.|instagram\\.|pinterest\\.|youtube\\.|linkedin\\.|twitter\\.).*","includedHostnamesPattern":".*(datahem\\.org).*","excludedBotsPattern":".*(^$|bot|spider|crawler).*","siteSearchPattern":".*q=(([^&#]*)|&|#|$)","timeZone":"Europe/Stockholm","excludedIpsPattern":"(127\\.0\\.0\\.0|172\\.16\\.0\\.0)", "pubSubTopic":"ua1234567-master", "tableSpec":"streams.ua1234567_master"},{"id":"unfiltered","searchEnginesPattern":".*(www\\.google\\.|www\\.bing\\.|search\\.yahoo\\.).*","ignoredReferersPattern":".*(\\.datahem\\.org).*","socialNetworksPattern":".*(facebook\\.|instagram\\.|pinterest\\.|youtube\\.|linkedin\\.|twitter\\.).*","includedHostnamesPattern":".*","excludedBotsPattern":"\\b\\B","siteSearchPattern":".*q=(([^&#]*)|&|#|$)","timeZone":"Europe/Stockholm", "excludedIpsPattern":"\\b\\B", "tableSpec":"streams.ua1234567_unfiltered"}]}]}' 