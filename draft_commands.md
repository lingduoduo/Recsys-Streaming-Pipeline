=========================
## di-golden-path
=========================
gcloud auth login
gcloud auth list


gcloud config set project scio-playground
gcloud config set project automated-marketing-engagement
gcloud config set project formats-insights
gcloud config set project paradox-mo
gcloud config set project acmacquisition

gcloud config list
gcloud iam roles describe roles/spanner.databaseAdmin --project=myproject


->> Configure your kubectl to talk to the ml-paved-road-training-eu Kubernetes cluster that we've provided
gcloud auth login
gcloud config set project ml-sketchbook
gcloud config set container/cluster ml-paved-road-training-eu
gcloud container clusters get-credentials ml-paved-road-training-eu --zone=europe-west1-b

->> Add credentials/key to the Kubernetes cluster
kubectl create secret generic $(whoami)-paved-road --from-file=key.json=./key.json

sbt pack dockerBuildAndPush
docker run -it \
-v $(pwd)/key.json:/key.json \
-e GOOGLE_APPLICATION_CREDENTIALS=/key.json \
-e SECRET_KEY_NAME=$(whoami)-paved-road \
-e BUILD_ID=$(whoami) \
gcr.io/ml-sketchbook/tf-supervised/$(whoami) \
bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_tasks TrainingGKEJob"


['{"message":"Campaign created","campaign":{"id":"29a11a77-d149-4410-9d4d-e871a31af702","name":"2019q4_acm_lifecycle_viva_latino_personalized_ladron_top_track","optout_type":"notify-recommended-music","active":true,"metadata":{"business_metric":"Engagement - MAU","business_owner":"Growth Opportunities","business_purpose":"Viva Latino Recommendation, broad","type":"Lifecycle Campaigns"},"created_by":"lingh","created_date":"2019-11-14T22:48:49.038334Z","modified_by":"lingh","modified_date":"2019-11-14T22:48:49.038334Z"},"success":true}']

spotify:user:spotify:playlist:37i9dQZF1DX10zKzsJ2jva


sbt
project di-golden-path-pipeline-lingh
runMain com.spotify.data.example.BrownieRecsJob 
--project=scio-playground 
--runner=DataflowRunner  
--region=europe-west1 
--tempLocation=gs://scio-playground/user/<username>/dataflow/tmp 
--output=gs://scio-playground/user/lingh/di_golden_path/output/end_content_fact_example 
--topN=10  
--metricsLocation=gs://scio-playground/user/lingh/di_golden_path/output/_metrics


runMain com.spotify.data.example.WordCount --project=scio-playground --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://scio-playground/user/lingh/dataflow/tmp --output=gs://scio-playground/user/lingh/di_golden_path/output/

runMain com.spotify.scio.examples.WordCount --input=<FILE PATTERN> --output=gs://scio-playground/user/lingh/di_golden_path/output/wordcount

project di-golden-path-pipeline-lingh

sbt 
runMain com.spotify.scio.examples.BeamSqlInterpolatorWordCount --project=scio-playground --runner=DataflowRunner  --region=europe-west1 --input=gs://apache-beam-samples/shakespeare/kinglear.txt --output=gs://scio-playground/user/lingh/di_golden_path/output/wordcount

runMain com.spotify.data.example.BrownieRecsJob --project=scio-playground --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://scio-playground/user/lingh/dataflow/tmp --output=gs://scio-playground/user/lingh/di_golden_path/output/end_content_fact_example

//runMain com.spotify.data.example.TopArtistsJob --project=scio-playground --runner=DataflowRunner --region=europe-west1 --endContentFact=gs://golden-path-sample-data/di.golden.path.EndContentFactXT2/2018-10-31/20181031T075406.854146-12dc225803c3/*.avro --textOutput=gs://scio-playground/user/lingh/di_golden_path/textOutput/ --avroOutput=gs://scio-playground/user/lingh/di_golden_path/avroOutput/ --bqOutput=lingh.bqOutput --topN=10

runMain com.spotify.data.example.TopCollaborationsJob --project=scio-playground --runner=DataflowRunner --region=europe-west1 --avroOutput=gs://scio-playground/user/lingh/di_golden_path/avroOutput2/ --topN=10

PYTHONPATH='di-golden-path-pipeline-lingh/src/main/python/' JAR_DIR='di-golden-path-pipeline-lingh/target/pack/lib' luigi --module top_artists TopArtistsJob --date 2018-10-31 --top-n 100 --local-scheduler


Exercise 1
sbt compile pack
//runMain com.spotify.data.example.TopArtistsJob --project=scio-playground --runner=DataflowRunner --region=europe-west1 --endContentFact=gs://golden-path-sample-data/di.golden.path.EndContentFactXT2/2018-10-31/20181031T075406.854146-12dc225803c3/*.avro --tempLocation=gs://scio-playground/user/lingh/dataflow/staging --textOutput=gs://scio-playground/user/lingh/di_golden_path/textOutput/ --avroOutput=gs://scio-playground/user/lingh/di_golden_path/avroOutput/ --bqOutput=lingh.bqOutput --topN=10

//runMain com.spotify.data.example.TopArtistsJob --runner=DataflowRunner  --project=scio-playground  --region=europe-west1  --endContentFact=gs://golden-path-sample-data/di.golden.path.EndContentFactXT2/2018-10-31/20181031T075406.854146-12dc225803c3/*.avro --textOutput=gs://scio-playground/user/lingh/di_golden_path/top_artists2/ --bqOutput=scio-playground:lingh.bqOutput --avroOutput=gs://scio-playground/user/lingh/di_golden_path/top_artists2/*.avro --topN=10
gsutil ls gs://scio-playground/user/lingh/di_golden_path/top_artists2/
avro-tools tojson gs://<gcs-bucket>/<output-dir>/<file-name>.avro

gcloud config set project formats-insights
git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-~/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting n\u000b
runMain com.spotify.data.example.UserJob --project=formats-insights --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://lingh/dataflow/tmp --output=gs://lingh/ml-testing/output/

https://lexikon.spotify.net/bq-table/bq%3Ausers-protection.ccd.communications_health_all_time_/metadata
https://ghe.spotify.net/nataniaw/message-volume-optimization-prototype

sbt
project ml-testing
runMain com.spotify.data.example.UserJob --project=formats-insights --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://lingh/dataflow/tmp --unsub=gs://lingh/ml-testing/unsub/ --active=gs://lingh/ml-testing/active/

sbt
project ml-testing
runMain com.spotify.data.example.UserJob --project=formats-insights --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://lingh/dataflow/tmp --unsub=gs://lingh/ml-testing/unsub/ --active=gs://lingh/ml-testing/active/
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks UserJob --unsub=gs://lingh/ml-testing/unsub/  --active=gs://lingh/ml-testing/active/ --local-scheduler

sbt
project ml-testing
runMain com.spotify.data.example.UserJob --project=formats-insights --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://lingh/dataflow/tmp --bqUnsub=lingh.unsub_20190715 --bqActive=lingh.active_20190715
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks UserJob --local-scheduler

Scheduled 1 tasks of which:
* 1 ran successfully:
    - 1 UserJob(uri_prefix=gs://lingh/ml-testing/)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====



sbt clean package docker
[info] Step 1/9 : FROM gcr.io/spotify-datainfra/edgedocker-base:2019.3.2
[info]  ---> 20b5d7b51770
[info] Step 2/9 : MAINTAINER squad <edison-acm-engagement-private@spotify.com>
[info]  ---> Using cache
[info]  ---> 28364ecda8be
[info] Step 3/9 : RUN apt-get update && apt-get install -y  libapr1 && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
[info]  ---> Using cache
[info]  ---> 62ec4d4d5e09
[info] Step 4/9 : COPY 0/requirements.txt /usr/share/ml-testing/requirements.txt
[info]  ---> Using cache
[info]  ---> 7308a0f0ffb3
[info] Step 5/9 : RUN ["pip", "install", "-r", "\/usr\/share\/ml-testing\/requirements.txt"]
[info]  ---> Using cache
[info]  ---> d13a2d6fde53
[info] Step 6/9 : COPY 1/python /usr/share/ml-testing/python
[info]  ---> Using cache
[info]  ---> 359e7dbb323a
[info] Step 7/9 : COPY 2/pack /usr/share/ml-testing
[info]  ---> 1b797b42ed77
[info] Step 8/9 : WORKDIR /usr/share/ml-testing
[info]  ---> Running in e653ce5f812c
[info] Removing intermediate container e653ce5f812c
[info]  ---> ec3f7ec48908
[info] Step 9/9 : ENV GIT_COMMIT="0de76b15ed5341c7690c0b19e0cca97ef4c54b7c" PYTHONPATH="/usr/share/ml-testing/python:$PYTHONPATH" STYX_DOCKER_IMAGE="gcr.io/formats-insights/ml-testing:20190716T113017-0de76b1" HADOOP_CONF_DIR="/etc/hadoop/conf" JAR_PATH="/usr/share/ml-testing/lib/*" LUIGI_GKE_IMAGE="gcr.io/formats-insights/ml-testing:20190716T113017-0de76b1"
[info]  ---> Running in a7ee15720eaf
[info] Removing intermediate container a7ee15720eaf
[info]  ---> ae384833c4dd
[info] Successfully built ae384833c4dd
[info] Tagging image ae384833c4dd with name: gcr.io/formats-insights/ml-testing:latest
[info] Tagging image ae384833c4dd with name: gcr.io/formats-insights/ml-testing:20190716T113017-0de76b1
[success] Total time: 8 s, completed Jul 16, 2019 11:30:25 AM

docker images

qstyx run -f data-info.yaml -w ml-testing.UserJob -p 2019-07-16 -i gcr.io/formats-insights/ml-testing


ERROR: Command '['java', '-Xmx256m', '-cp', '/usr/share/ml-testing/lib/*', 'com.spotify.data.example.UserJob', 
'--runner=DataflowRunner', 
'--project=formats-insights', 
'--region=europe-west1', 
'--stagingLocation=gs://lingh/ml-testing/dataflow-staging/', 
'--tempLocation=gs://lingh/ml-testing/dataflow-temp/', 
'--autoscalingAlgorithm=THROUGHPUT_BASED', 
'--maxNumWorkers=25', 
'--labels={"spotify-styx-parameter": "p-2019-07-16", "spotify-styx-component-id": "ml-testing", "spotify-styx-trigger-id": "qstyx-eb1788f0-9492-4f76-bc88-fc637a71668d", "spotify-styx-workflow-id": "ml-testing-userjob", "spotify-styx-execution-id": "styx-run-8b5fb66c-7072-4d67-a88e-8435737fb038"}', 
'--blocking', 
'--bqActive=formats-insights:_incoming_EU._active_20190716_8997914007', '--bqUnsub=formats-insights:_incoming_EU._unsub_20190716_9295646783']' returned non-zero exit status 1
Traceback (most recent call last):
  File "/usr/local/lib/python2.7/dist-packages/spotify_scala_luigi/scio.py", line 182, in run
    run_with_logging(cmd_line, self)
  File "/usr/local/lib/python2.7/dist-packages/spotify_scala_luigi/scio.py", line 564, in run_with_logging
    return _run_with_logging(cmd, task, monitor)
  File "/usr/local/lib/python2.7/dist-packages/spotify_scala_luigi/scio.py", line 590, in _run_with_logging
    raise subprocess.CalledProcessError(exit_code, cmd, output=output)
CalledProcessError: Command '['java', '-Xmx256m', '-cp', '/usr/share/ml-testing/lib/*', 'com.spotify.data.example.UserJob', '--runner=DataflowRunner', '--project=formats-insights', '--region=europe-west1', '--stagingLocation=gs://lingh/ml-testing/dataflow-staging/', '--tempLocation=gs://lingh/ml-testing/dataflow-temp/', '--autoscalingAlgorithm=THROUGHPUT_BASED', '--maxNumWorkers=25', '--labels={"spotify-styx-parameter": "p-2019-07-16", "spotify-styx-component-id": "ml-testing", "spotify-styx-trigger-id": "qstyx-eb1788f0-9492-4f76-bc88-fc637a71668d", "spotify-styx-workflow-id": "ml-testing-userjob", "spotify-styx-execution-id": "styx-run-8b5fb66c-7072-4d67-a88e-8435737fb038"}', '--blocking', '--bqActive=formats-insights:_incoming_EU._active_20190716_8997914007', '--bqUnsub=formats-insights:_incoming_EU._unsub_20190716_9295646783']' returned non-zero exit status 1
INFO: Deleting temporary table BQTable(project_id='formats-insights', dataset_id='_incoming_EU', table_id='_active_20190716_8997914007', location=None) due to job failure
INFO: Deleting temporary table BQTable(project_id='formats-insights', dataset_id='_incoming_EU', table_id='_unsub_20190716_9295646783', location=None) due to job failure
INFO:run: cleaning up json key of service account 504437161930-compute@developer.gserviceaccount.com
INFO:run: cleaning up p12 key of service account 504437161930-compute@developer.gserviceaccount.com
ERROR:qstyx: non-zero exit code (1) from `/usr/local/bin/docker run -it -v /Users/lingh/Git/ml-testing/_qstyx:/etc/_qstyx -e STYX_COMPONENT_ID=ml-testing -e STYX_WORKFLOW_ID=ml-testing.UserJob -e STYX_PARAMETER=2019-07-16 -e STYX_DOCKER_IMAGE=gcr.io/formats-insights/ml-testing -e STYX_DOCKER_ARGS="wrap-luigi --module luigi_tasks UserJob" -e STYX_EXECUTION_ID=styx-run-8b5fb66c-7072-4d67-a88e-8435737fb038 -e STYX_TRIGGER_ID=qstyx-eb1788f0-9492-4f76-bc88-fc637a71668d -e STYX_ENVIRONMENT=qstyx -e STYX_LOGGING=text -e GOOGLE_APPLICATION_CREDENTIALS=/etc/_qstyx/gcp-sa-key.json -e STYX_SERVICE_ACCOUNT=504437161930-compute@developer.gserviceaccount.com gcr.io/formats-insights/ml-testing wrap-luigi --module luigi_tasks UserJob`


sbt -mem 2048 -Dbigquery.secret=/Users/zhongyaoc/keys/bart-features-starlord.json -Dbigquery.project=bart-ml-features docker

sbt clean verify docker
qstyx run -f data-info.yaml -w ml-golden-path-workshop.<job-name> -p <date> -i gcr.io/ml-sketchbook/ml-golden-path-workshop
qstyx run -f data-info.yaml -w ml-testing.UserJob -p 2019-07-16 -i gcr.io/formats-insights/ml-testing


sbt
project message-ranking
runMain com.spotify.data.example. EngagementTrainingJob --project=formats-insights --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://lingh/dataflow/tmp --bqUnsub=lingh.unsub_20190715 --bqActive=lingh.active_20190715
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks UserJob --local-scheduler

Right now, the interactionContext 
https://ghe.spotify.net/white-mouse/message-ranking/blob/master/message-ranking/src/main/scala/com/spotify/features/EngagementInteractionContextJob.scala) 
	which is what gathers training data for the model re-constructs the context the user had at serving.  
But Ladron logs this data now at the time a message is chosen, 
so message-ranker should read this directly and simply join with the user_message_interaction signal


runMain com.spotify.data.example.UserJob --project=formats-insights --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://lingh/dataflow/tmp --unsub=gs://lingh/ml-testing/unsub/ --active=gs://lingh/ml-testing/active/


qstyx run -f data-info.yaml -w ml-testing.UserJob -p 2019-07-16 -i gcr.io/formats-insights/ml-testing


[error] /var/jenkins_home/workspace/tingle.335/workspace/ml-testing/src/main/scala/com/spotify/data/example/UserJob.scala:28:4: exception during macro expansion:
[error] com.google.api.client.googleapis.json.GoogleJsonResponseException: 403 Forbidden
[error] {
[error] "code" : 403,
[error] "errors" : [ {
[error] "domain" : "global",
[error] "message" : "Access Denied: Table beanstalk-app:beanstalk_engines_all_features_v2.scenario_237_all_features_v2_20190701: The user ml-testing-workflow-sa@formats-insights.iam.gserviceaccount.com does not have permission to query table beanstalk-app:beanstalk_engines_all_features_v2.scenario_237_all_features_v2_20190701.",
[error] "reason" : "accessDenied"
[error] } ],
[error] "message" : "Access Denied: Table beanstalk-app:beanstalk_engines_all_features_v2.scenario_237_all_features_v2_20190701: The user ml-testing-workflow-sa@formats-insights.iam.gserviceaccount.com does not have permission to query table beanstalk-app:beanstalk_engines_all_features_v2.scenario_237_all_features_v2_20190701.",
[error] "status" : "PERMISSION_DENIED"
[error] }


#standardSQL
SELECT
TO_HEX(user_id) as user_id,
device.type as device_type,
device.os as device_os,
product.type as product_type,
product.category as product_category,
product.subcategory as product_subcategory,
product.associate_name as associate_name,
event_type_signature,
value
FROM
`beanstalk-app.event_counts_end_content_fact_xt_2_v2.event_counts_end_content_fact_xt_2_v2_20190701`,UNNEST(event_counts)
WHERE user_id IS NOT NULL
AND DATE_FROM_UNIX_DATE(utc_date) = '2019-07-01'

# If your Scio pipeline uses BigQueryType, then uncomment the line below and specify your BigQuery project id
sbt clean verify -Dbigquery.project=formats-insights -J-Dfile.encoding=UTF8 -J-Xss1M -J-Xms2G -J-Xmx4G -J-XX:ReservedCodeCacheSize=512M -J-XX:MaxMetaspaceSize=1G -J-XX:+CMSClassUnloadingEnabled -J-Dsun.io.serialization.extendedDebugInfo=true docker


https://docs.google.com/presentation/d/1dgB-yM_NDxTvEayTYD4N2aTmCnn9Absdz50vWdtGSe0/edit#slide=id.g1f34120cd8_0_17
Dataflow Profiling to GCS (Runs in GCP)
Requires pprof and graphviz
Create GCS location for profiles
Add option to pipeline
--saveProfilesToGcs=gs://[BUCKET_NAME]/[OBJECT_PATH]
Retrieve the profiles
gsutil cp -r gs://[GCS_LOCATION]/[JOB_ID]  [PATH_ON_YOUR_MACHINE]
Run pprof
pprof -[dot|web]  -output profile.dot [JOB_ID]/*/*_cpu_*.gz
Generate the visualization  - dot -Tpng profile.dot > profile.png


=========================
## Ladron Endpoints
=========================
https://backstage.spotify.net/docs/ladron/FAQ/index.html

ladron.candidate.UserSelectionInfo
https://backstage.spotify.net/data-endpoints/ladron.candidate.UserSelectionInfo/counters?lcFrom=2019-12-31T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2019-12-31T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2019-12-31T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=user_info_export.user_info_export_missing_user_locale_count&lcConfigs%5B1%5D=user_info_export.user_info_export_missing_notification_optout_count&lcConfigs%5B2%5D=user_info_export.user_info_export_missing_join_data_count

ladron.reactivation.Delivery
https://backstage.spotify.net/data-endpoints/ladron.reactivation.Delivery/counters?lcFrom=2019-12-31T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2019-12-31T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2019-12-31T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=delivery.delivery_input_push-dry-run-message-count&lcConfigs%5B1%5D=delivery.delivery_input_push-message-count

ladron.reactivation.Messages
https://backstage.spotify.net/data-endpoints/ladron.reactivation.Messages/counters?lcFrom=2019-12-31T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2019-12-31T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2019-12-31T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=message_history_metrics.message_history_metrics_output_count&lcConfigs%5B1%5D=ladron_metrics.ladron_metrics_processed_audience&lcConfigs%5B2%5D=message_history.message_history_sent_count&lcConfigs%5B3%5D=delivery_metrics.delivery_metrics_created_push_dry_run

ladron.engagement.Messages.gcs
https://backstage.spotify.net/data-endpoints/ladron.engagement.Messages.gcs/counters?lcFrom=2019-12-31T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2019-12-31T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2019-12-31T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=delivery_metrics.delivery_metrics_created_push&lcConfigs%5B1%5D=message_history.message_history_output_count&lcConfigs%5B2%5D=message_history_metrics.message_history_metrics_output_count&lcConfigs%5B3%5D=assignment_metrics.assignment_metrics_total_assigned_candidates_holdout

ladron.engagement.Delivery.pubsub
https://backstage.spotify.net/data-endpoints/ladron.engagement.Delivery.pubsub/counters?lcFrom=2019-12-31T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2019-12-31T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2019-12-31T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=delivery.delivery_input_email-dry-run-message-count&lcConfigs%5B1%5D=delivery.delivery_input_count&lcConfigs%5B2%5D=delivery.delivery_input_email-message-count

ladron.activation.Messages.gcs
https://backstage.spotify.net/data-endpoints/ladron.activation.Messages.gcs/counters?lcFrom=2020-01-06T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2020-01-06T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2020-01-06T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=delivery_metrics.delivery_metrics_created_total&lcConfigs%5B1%5D=assignment_metrics.assignment_metrics_total_assigned_candidates_holdout&lcConfigs%5B2%5D=ladron_metrics.ladron_metrics_processed_audience&lcConfigs%5B3%5D=message_history_metrics.message_history_metrics_output_count

ladron.activation.Delivery.pubsub
https://backstage.spotify.net/data-endpoints/ladron.activation.Delivery.pubsub/counters?lcFrom=2020-01-06T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2020-01-06T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2020-01-06T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=delivery.delivery_input_push-dry-run-message-count&lcConfigs%5B1%5D=delivery.delivery_input_email-message-count&lcConfigs%5B2%5D=delivery.delivery_input_push-message-count

ladron.activation.AggregatedCandidates
https://backstage.spotify.net/data-endpoints/ladron.activation.AggregatedCandidates/counters?lcFrom=2020-01-06T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2020-01-06T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2020-01-06T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=activation_candidates.activation_candidates_candidate_endpoints_count&lcConfigs%5B1%5D=activation_candidates.activation_candidates_candidate_batch_count&lcConfigs%5B2%5D=activation_candidates.activation_candidates_candidate_realtime_count

ladron.metrics.operational.DeliveryStatus
https://backstage.spotify.net/data-endpoints/ladron.metrics.operational.DeliveryStatus/counters?lcFrom=2019-12-31T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2019-12-31T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2019-12-31T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=delivery_metrics.delivery_metrics_system_failed_MAX&lcConfigs%5B1%5D=delivery_metrics.delivery_metrics_messages_contains_unknown_event_type%255BSUM%255D&lcConfigs%5B2%5D=delivery_metrics.delivery_metrics_messages_contains_published%255BMAX%255D&lcConfigs%5B3%5D=delivery_metrics.delivery_metrics_system_published%255BMIN%255D

ladron.metrics.UserMessageInteraction
https://backstage.spotify.net/data-endpoints/ladron.metrics.UserMessageInteraction/counters?lcFrom=2019-12-31T18%3A00&lcTo=2020-01-07T18%3A00&lcPerrow=2&lcIscompact=true&paFrom=2019-12-31T18%3A00&paTo=2020-01-07T18%3A00&hmFrom=2019-12-31T18%3A00&hmTo=2020-01-07T18%3A00&lcConfigs%5B0%5D=user_message_interaction_extension_metrics.user_message_interaction_extension_metrics_Activation_missing_user_assignment_count&lcConfigs%5B1%5D=user_message_interaction_extension_metrics.user_message_interaction_extension_metrics_Engagement_assigned_user_missing_interaction_count&lcConfigs%5B2%5D=user_message_interaction_extension_metrics.user_message_interaction_extension_metrics_Activation_multiple_user_interaction_count&lcConfigs%5B3%5D=user_interaction_metrics.user_interaction_metrics_input_relevant_count

https://ghe.spotify.net/white-mouse/ladron/blob/master/bin/eval.sh

https://ghe.spotify.net/white-mouse/ladron/blob/master/docs/logs/2019-12-05-logged-data.md

https://spotify.stackenterprise.co/questions/1369/scio-pipelines-how-to-disable-style-coverage-test-checks/1395#1395

endpoints:
- name: "ladron.candidate.UserSelectionInfo"
  partitioning: "days"
  description: "All of the users that will be processed for the ladron messaging modules"
  storageUriPattern: "hades:///ladron.candidate.UserSelectionInfo/%Y-%M-%D"
  isContinuous: true
  latenessSlo: “PT24H”

hades ls ladron.reactivation.Messages 2019-07-01
gsutil ls gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/
gsutil cp gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/part-00000-of-00027.avro .

java -jar avro_files/avro-tools-1.9.0.jar tojson part-00000-of-00027.avro  jq .
{
  "record_id": "2f072c25454e4632b690da2f159fcbdd#ea0ecbe4-4e92-43ac-8109-7da0379f1a79#168#2019-07-03",
  "user_id": "/\u0007,%ENF2¶Ú/\u0015ËÝ",
  "campaign_id": "ea0ecbe4-4e92-43ac-8109-7da0379f1a79",
  "template_id": "168",
  "send_at": {
    "long": 1562173200000
  },
  "dry_run": false,
  "channel": "Push",
  "push_specific": {
    "com.spotify.ladron.avro.delivery.PushSpecific": {
      "platform": []
    }
  }
}


hades ls ladron.reactivation.Messages 2019-07-31
gsutil ls gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-30

gsutil cat gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-30/20190731T125914.818804-317906c5c985/part-00133-of-00134.avro  avro-tools tojson - 

{"record_id":"3db006234e524b82b56ed66b02c909b1#89be9bdb-3d1e-4966-ad1a-70deebd1ee3a#171#2019-07-31","user_id":"=°\u0006#NRKµnÖk\u0002É\t±","campaign_id":"89be9bdb-3d1e-4966-ad1a-70deebd1ee3a","template_id":"171","send_at":{"long":1564678800000},"dry_run":false,"channel":"Push","push_specific":{"com.spotify.ladron.avro.delivery.PushSpecific":{"platform":[]}},"template_values":{}}


gsutil ls gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-30/20190731T125914.818804-317906c5c985CqoBEqMBagtlfmhhZGVzLXhwbnKDAQsSDERhdGFFbmRwb2ludCIcbGFkcm9uLnJlYWN0aXZhdGlvbi5NZXNzYWdlcwwLEglQYXJ0aXRpb24iFDIwMTktMDctMzBUMDA6MDA6MDBaDAsSCFJldmlzaW9uIiQ0NTUyZmM1Mi04MmRkLTQxMzctOTdlNi0wYjk4N2YzYzU2OTIMogENaGFkZXMtc2VydmljZRgAIAA= 

	 avro-tools tojson -  jq .

gsutil cat gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/part-00000-of-00027.avro  jq .
gsutil cp gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/part-00000-of-00027.avro .


gsutil ls gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/
gsutil cat gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/part-00000-of-00027.avro  avro-tools tojson -
gsutil cat gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/part-00000-of-00027.avro  jq .


hades ls ladron.reactivation.Messages 2019-07-01 
gsutilhS ls gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/ 
gsutil cat gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/part-00000-of-00027.avro | avro-tools tojson - | head -1 | jq .


=========================
## ml-testing
=========================
>>> What are some of the data squads and tools at Spotify?

https://confluence.spotify.net/display/PartnerSubs/FAQs

gcloud auth login
gcloud auth list

gcloud config set project formats-insights
gcloud config list

>>>UserJob
## ml-testing
sbt
project ml-testing
runMain com.spotify.data.example.UserJob --project=formats-insights --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://lingh/dataflow/tmp --bqUnsub=lingh.unsub_20190715 --bqActive=lingh.active_20190715

sbt clean verify docker
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks UserJob --local-scheduler

>>>PushLabelJob
sbt
project ml-testing
runMain com.spotify.data.example.PushLabelJob --project=formats-insights --date=20190630 --output=lingh.push_labels_20190630  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1
runMain com.spotify.data.example.PushLabelJob --project=formats-insights --date=20190701 --output=lingh.push_labels_20190701  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1
runMain com.spotify.data.example.PushLabelJob --project=formats-insights --date=20190702 --output=lingh.push_labels_20190702  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1
runMain com.spotify.data.example.PushLabelJob --project=formats-insights --date=20190706 --output=lingh.push_labels_20190706  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1
runMain com.spotify.data.example.PushLabelJob --project=formats-insights --date=20191007 --output=lingh.push_labels_20191007  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1
runMain com.spotify.data.example.PushLabelJob --project=formats-insights --date=20200111 --output=lingh.push_labels_20200111  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1


PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks_train PushLabelJob --date 2019-10-09  --channel Push --local-scheduler

docker run -it -v $(pwd)/ml-testing-workflow-sa.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json  gcr.io/formats-insights/ml-testing:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module luigi_tasks_train PushLabelJob --date 2019-10-10  --channel Push"
docker run -it -v $(pwd)/ml-testing-workflow-sa.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json  gcr.io/formats-insights/ml-testing:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module luigi_tasks_train PushLabelJob --date 2019-10-11  --channel Push"


https://spotify.stackenterprise.co/questions/5540/how-can-i-integrate-my-application-with-tingle
https://confluence.spotify.net/display/DI/Setting+up+a+GCP+Service+Account
https://ghe.spotify.net/datainfra/quick-styx#sudo

gcloud auth activate-service-account ml-testing-workflow-sa@formats-insights.iam.gserviceaccount.com --key-file ml-testing-workflow-sa.json 
gcloud config list  # just double check if the account and project are correct. If not, set the project to yours
gcloud pubsub subscriptions create com.spotify.tingle.build.<SERVICE_CONSUMING_NAME> --topic com.spotify.tingle.build --topic-project=xpn-tingle-1


docker images
rm -rf _qstyx
qstyx run -f data-info.yaml -w ml-testing.PushLabelJob -p 2019-11-08 -i gcr.io/formats-insights/ml-testing
qstyx run -f data-info.yaml -w ml-testing.PushLabelJob -p 2019-11-08 -i gcr.io/formats-insights/ml-testing

https://docs.google.com/forms/d/e/1FAIpQLSfqpo4hu-xUYlt4lPv3MdqC71NOdSn81mSTa8Rp578hzi06Eg/viewform


sbt clean verify docker
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks_label PushLabelJob --date 2019-07-03  --local-scheduler
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks_label PushLabelJob --date 2019-07-04  --channel Push --local-scheduler
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks_label PushLabelJob --date 2019-07-05  --channel Push --local-scheduler
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks_train PushLabelJob --date 2019-07-06  --channel Push --local-scheduler
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks_train PushLabelJob --date 2019-07-07  --channel Push --local-scheduler


>>>UserAggregatesJob
sbt
project ml-testing
runMain com.spotify.data.example.UserAggregatesJob --project=formats-insights --date=20190705 --output=lingh.user_aggregates_20190705  --usersTableToJoin=formats-insights:lingh.push_labels_20190705  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1
runMain com.spotify.data.example.UserAggregatesJob --project=formats-insights --date=20190704 --output=lingh.user_aggregates_20190704  --usersTableToJoin=formats-insights:lingh.push_labels_20190705  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1
runMain com.spotify.data.example.UserAggregatesJob --project=formats-insights --date=20190703 --output=lingh.user_aggregates_20190703  --usersTableToJoin=formats-insights:lingh.push_labels_20190705  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1
runMain com.spotify.data.example.UserAggregatesJob --project=formats-insights --date=20201111 --output=lingh.user_aggregates_20201111  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1


sbt clean verify docker
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks_train UserAggregatesJob --date 2019-07-02 --UserAggregatesJob-date 2019-07-02  --channel Push  --UserAggregatesJob-channel Push --UserAggregatesJob-date-label-table-to-join 2019-07-05 --local-scheduler
PYTHONPATH='ml-testing/src/main/python/' JAR_DIR='ml-testing/target/pack/lib' luigi --module luigi_tasks_train UserAggregatesJob --date 2019-07-01 --UserAggregatesJob-date 2019-07-01  --channel Push  --UserAggregatesJob-channel Push --UserAggregatesJob-date-label-table-to-join 2019-07-05 --local-scheduler

docker images
qstyx run -f data-info.yaml -w ml-testing.UserAggregatesJob -p 2019-11-08 -i gcr.io/formats-insights/ml-testing

qstyx run -f data-info.yaml -w ml-testing.UserAggregatesJob -p 2019-09-08 -i gcr.io/formats-insights/ml-testing
qstyx run -f data-info.yaml -w ml-testing.PushLabelJob -p 2019-10-08 -i gcr.io/formats-insights/ml-testing
qstyx run -f data-info.yaml -w ml-testing.PushLabelJob -p 2019-11-08 -i gcr.io/formats-insights/ml-testing

qstyx run -f data-info.yaml -w ml-testing.UserJob -p 2019-07-16 -i gcr.io/formats-insights/ml-testing

runMain com.spotify.data.example.UserAggregatesJob --project=formats-insights --date=20200111 --output=lingh.user_aggregates_20200111  --usersTableToJoin=formats-insights:lingh.useraggregates_20200111  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1
runMain com.spotify.data.example.UserAggregatesJob --project=formats-insights --date=20200112 --output=lingh.user_aggregates_20200112  --usersTableToJoin=formats-insights:lingh.useraggregates_20200112  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1



===== Luigi Execution Summary =====

Scheduled 2 tasks of which:
* 1 complete ones were encountered:
    - 1 CommunicationsHealth(date=2019-07-05)
* 1 ran successfully:
    - 1 PushLabelJob(date=2019-07-05, channel=Push)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====  

git branch -d  origin/addlabeling
  origin/changetable
  origin/master
  origin/opt-out-label
  origin/pushlabel
  origin/update-service-account
  origin/user-aggregatesb


SELECT
user_id, 
os_level_unsub, 
meta.campaign_type
FROM (
SELECT "20191007" AS date,
channel,
user_id,
message_id,
push.campaign_id,
STRING(push.time_send) AS time_send,
push.os_level_unsub,
push.optout_diff,
push.optout_type AS optout_type
FROM `users-protection.ccd.communications_health_20191007`
WHERE
channel = "push"
AND push.optout_type IS NOT NULL
AND LOWER(push.optout_type) NOT LIKE "%transactional%"
AND push.time_send <= TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m%d", "20191007"),
INTERVAL 24*2 HOUR)
AND push.time_send >= TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y%m%d", "20191007"),
INTERVAL 24*3 HOUR)) label
JOIN (
SELECT
id,
campaign_type
FROM `users-protection.ucd.campaigns_20191007`
WHERE
# Conditional on channel is necessary, as campaign_id is not globally identifiable
channel="push"
AND campaign_type IS NOT NULL
AND campaign_type!="") meta
ON label.campaign_id=meta.id
GROUP BY 1,2,3


qstyx run -f data-info.yaml -w ml-testing.PushLabelJob -p 2019-10-08 -i gcr.io/ml-testing/
qstyx run -f data-info.yaml -w formats-insights.ml-testing.PushLabelJob -p 2019-10-10 -i gcr.io/ml-sketchbook/ml-golden-path-workshop
qstyx run -f data-info.yaml -w di-golden-path-pipeline-<username>.TopArtistsJob -p 2018-10-31 -r  di-golden-path-pipeline-<username>/target/image-name
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-12


1 
notify-recommended-music
2 
notify-new-music
3 
notify-product-news
4 
notify-playlist-updates
5 
notify-news-and-offers
6 
notify-artist-updates
7 
notify-concert-notifications


SELECT *
FROM
  `automated-marketing-engagement.campaign_executions.engagement` ce
WHERE campaign_id = 'edb4fe90-447e-4019-a6ad-926cfcfd62f8';

SELECT 
campaign_id,
channel,
count(*) cnt, 
sum(case when is_click then 1 else 0 end) clicks,
sum(case when is_open then 1 else 0 end) opens
FROM `content-marketing-messaging.user_message_interaction.user_message_interaction_*` 
WHERE _table_suffix >= '20191007' and _table_suffix <= '20191014'
AND channel = 'Push'
AND campaign_id = 'edb4fe90-447e-4019-a6ad-926cfcfd62f8'
GROUP BY 1, 2

docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train LabelJob --date 2019-08-13 --channel Push"


qstyx run -f data-info.yaml -w ml-golden-path-workshop.ml-testing.PushLabelJob -p 2019-10-07 -i gcr.io/formats-insights/ml-testing
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.BucketizedFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-11


ERROR:qstyx: non-zero exit code (35) from `/usr/local/bin/

docker run -it -v /Users/lingh/Git/ml-testing/_qstyx:/etc/_qstyx 
-e STYX_COMPONENT_ID=ml-testing 
-e STYX_WORKFLOW_ID=ml-testing.PushLabelJob 
-e STYX_PARAMETER=2019-10-09 
-e STYX_DOCKER_IMAGE=gcr.io/formats-insights/ml-testing 
-e STYX_DOCKER_ARGS="wrap-luigi 
--module luigi_tasks_train PushLabelJob 
--channel Push 
--date 2019-10-09
" -e STYX_EXECUTION_ID=styx-run-5754550a-f9d9-462d-849f-95cdb55887ca 
-e STYX_TRIGGER_ID=qstyx-f3048723-9887-4cba-af51-4eece3aada2b 
-e STYX_ENVIRONMENT=qstyx 
-e STYX_LOGGING=text 
-e GOOGLE_APPLICATION_CREDENTIALS=/etc/_qstyx/gcp-sa-key.json 
-e STYX_SERVICE_ACCOUNT=ml-testing-workflow-sa@formats-insights.iam.gserviceaccount.com 
gcr.io/formats-insights/ml-testing wrap-luigi --module luigi_tasks_train PushLabelJob --channel Push --date 2019-10-09`


===================================
## Paradox messaging-optimization
===================================

pip install -r tf-supervised/src/main/python/requirements.txt

gcloud auth login
gcloud auth list

gcloud config set project paradox-mo
gcloud config list

>>>PushLabelJob
sbt
project tfSupervised
runMain com.spotify.tf.PushLabelJob --date=20190701 --output=paradox-mo:lingh_test.push_labels_20190701 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.PushLabelJob --date=20190704 --output=paradox-mo:lingh_test.push_labels_20190704 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.PushLabelJob --date=20190820 --output=paradox-mo:lingh_test.push_labels_20190820 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1


docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train LabelJob --date 2019-08-11 --channel Push"
docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/slayton:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train LabelJob --date 2019-08-12 --channel Push"
docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train LabelJob --date 2019-08-13 --channel Push"

>>>UserAggregatesJob
sbt
runMain com.spotify.tf.UserAggregatesJob --date=20190704 --output=paradox-mo:lingh_test.user_aggregates_20190704 --usersTableToJoin=paradox-mo:lingh_test.push_labels_20190704 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.UserAggregatesJob --date=20190703 --output=paradox-mo:lingh_test.user_aggregates_20190703 --usersTableToJoin=paradox-mo:lingh_test.push_labels_20190704 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.UserAggregatesJob --date=20190702 --output=paradox-mo:lingh_test.user_aggregates_20190702 --usersTableToJoin=paradox-mo:lingh_test.push_labels_20190704 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.UserAggregatesJob --date=20190701 --output=paradox-mo:lingh_test.user_aggregates_20190701 --usersTableToJoin=paradox-mo:lingh_test.push_labels_20190704 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.UserAggregatesJob --date=20190630 --output=paradox-mo:lingh_test.user_aggregates_20190630 --usersTableToJoin=paradox-mo:lingh_test.push_labels_20190704 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.UserAggregatesJob --date=20190629 --output=paradox-mo:lingh_test.user_aggregates_20190629 --usersTableToJoin=paradox-mo:lingh_test.push_labels_20190704 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.UserAggregatesJob --date=20190628 --output=paradox-mo:lingh_test.user_aggregates_20190628 --usersTableToJoin=paradox-mo:lingh_test.push_labels_20190704 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.UserAggregatesJob --date=20190627 --output=paradox-mo:lingh_test.user_aggregates_20190627 --usersTableToJoin=paradox-mo:lingh_test.push_labels_20190704 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1
runMain com.spotify.tf.UserAggregatesJob --date=20190820 --output=paradox-mo:lingh_test.user_aggregates_20190820 --usersTableToJoin=paradox-mo:lingh_test.push_labels_20190820 --project=paradox-mo   --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/tmp --region=europe-west1


docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train UserAggregatesJob --date 2019-08-11  --UserAggregatesJob-date 2019-08-11  --channel Push --UserAggregatesJob-channel Push --UserAggregatesJob-date-label-table-to-join 2019-08-11"
docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/slayton:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train UserAggregatesJob --date 2019-08-12  --UserAggregatesJob-date 2019-08-12  --channel Push --UserAggregatesJob-channel Push --UserAggregatesJob-date-label-table-to-join 2019-08-12 "
docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train UserAggregatesJob --date 2019-08-13  --UserAggregatesJob-date 2019-08-13  --channel Push --UserAggregatesJob-channel Push --UserAggregatesJob-date-label-table-to-join 2019-08-13"


>>>FeatureToTfRecord
sbt
runMain com.spotify.tf.FeatureToTfRecord --channel=Push --training_data=gs://mo_ml/lingh/push/training_data --evaluation_data=gs://mo_ml/lingh/push/evaluation_data --date=20190704 --days-back=1 --days-prior-useragg=1 --labels_0_days_back=paradox-mo:lingh_test.push_labels_20190704 --user-aggregations_1_days_back=paradox-mo:lingh_test.user_aggregates_20190627 --project=paradox-mo --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/dataflow/tmp --region=europe-west1 
runMain com.spotify.tf.FeatureToTfRecord --channel=Push --training_data=gs://mo_ml/lingh/push/training_data --evaluation_data=gs://mo_ml/lingh/push/evaluation_data --date=20190820 --days-back=1 --days-prior-useragg=1 --labels_0_days_back=paradox-mo:lingh_test.push_labels_20190820 --user-aggregations_1_days_back=paradox-mo:lingh_test.user_aggregates_20190820 --project=paradox-mo --runner=DataflowRunner --tempLocation=gs://mo_ml/lingh/push/dataflow/tmp --region=europe-west1 


Manual run outputs -

https://console.cloud.google.com/storage/browser/mo_ml/lingh/push/training_data/?project=paradox-mo
https://console.cloud.google.com/storage/browser/mo_ml/lingh/push/evaluation_data/?project=paradox-mo

gsutil ls gs://mo_ml/lingh/push/training_data
gsutil ls gs://mo_ml/lingh/push/evaluation_data


-->>Dataflow param
--channel=Push, 
--date=20190806, 
--days-back=7, 
--days-prior-useragg=4, 
--evaluation_data=gs://mo_ml/push/tf/tfrecords/Push.BaseInputDataV1.evaluation/2019-08-06/20190813T190336.379146-b1ee6d3e592b, 
--labels_0_days_back=paradox-mo:mo_ml_push.labels_v1_20190806, 
--labels_1_days_back=paradox-mo:mo_ml_push.labels_v1_20190805, 
--labels_2_days_back=paradox-mo:mo_ml_push.labels_v1_20190804, 
--labels_3_days_back=paradox-mo:mo_ml_push.labels_v1_20190803, 
--labels_4_days_back=paradox-mo:mo_ml_push.labels_v1_20190802, 
--labels_5_days_back=paradox-mo:mo_ml_push.labels_v1_20190801, 
--labels_6_days_back=paradox-mo:mo_ml_push.labels_v1_20190731, 
--over-sample-rate=1.0, 
--sample=True, 
--sample-rate=0.5, 
--training_data=gs://mo_ml/push/tf/tfrecords/Push.BaseInputDataV1.train/2019-08-06/20190813T190336.379355-c6bd4cf4cd0d, 
--user-aggregations_4_days_back=paradox-mo:mo_ml_push.user_aggregation_data_v1_20190802, 
--user-aggregations_5_days_back=paradox-mo:mo_ml_push.user_aggregation_data_v1_20190801, 
--user-aggregations_6_days_back=paradox-mo:mo_ml_push.user_aggregation_data_v1_20190731, 
--user-aggregations_7_days_back=paradox-mo:mo_ml_push.user_aggregation_data_v1_20190730, 
--user-aggregations_8_days_back=paradox-mo:mo_ml_push.user_aggregation_data_v1_20190729, 
--user-aggregations_9_days_back=paradox-mo:mo_ml_push.user_aggregation_data_v1_20190728,
--user-aggregations_10_days_back=paradox-mo:mo_ml_push.user_aggregation_data_v1_20190727, 


docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/slayton:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train FeatureToTfRecord --channel Push --date 2019-08-12 --days-back 1 --schema-file push.pbtxt"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train FeatureToTfRecord --channel Push --date 2019-08-11 --days-back 1 --schema-file push.pbtxt --base-path gs://mo_ml/lingh/"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train FeatureToTfRecord --channel Push --date 2019-08-13 --days-back 1 --schema-file push.pbtxt --base-path gs://mo_ml/lingh/"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train FeatureToTfRecord --channel Push --date 2019-08-21 --days-back 1 --schema-file push.pbtxt --base-path gs://mo_ml/lingh/"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train FeatureToTfRecord --channel Push --date 2019-09-18 --days-back 1 --schema-file push.pbtxt --base-path gs://mo_ml/lingh/"


===== Luigi Execution Summary =====

Scheduled 5 tasks of which:
* 2 complete ones were encountered:
    - 1 CommunicationsHealth(date=2019-08-21)
    - 1 UserCommunicationSnapshot(date=2019-08-17)
* 3 ran successfully:
    - 1 FeatureToTfRecord(...)
    - 1 LabelJob(date=2019-08-21, channel=Push)
    - 1 UserAggregatesJob(date=2019-08-17, date_label_table_to_join=2019-08-21, channel=Push)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 5 tasks of which:
* 2 complete ones were encountered:
    - 1 CommunicationsHealth(date=2019-08-21)
    - 1 UserCommunicationSnapshot(date=2019-08-17)
* 3 ran successfully:
    - 1 FeatureToTfRecord(...)
    - 1 LabelJob(date=2019-08-21, channel=Push)
    - 1 UserAggregatesJob(date=2019-08-17, date_label_table_to_join=2019-08-21, channel=Push)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

Pipeline output -

gsutil ls gs://mo_ml/lingh/tfrecords/pdx_mo.Push.BaseInputDataV1.train/2019-08-21/20190827T173336.732193-c52ea29f77ca
gsutil ls gs://mo_ml/lingh/tfrecords/pdx_mo.Push.BaseInputDataV1.evaluation/2019-08-21/20190827T173336.731882-f43a57c51327


>>>Create schema
https://ghe.spotify.net/paradox/messaging-optimization/blob/master/messaging-optimization-pipeline/tf-supervised/src/main/python/trainers/schemas/push.pbtxt
https://ghe.spotify.net/lingh/ds-golden-path-lingh/blob/master/notebooks/ml-testing-notebooks/canonical_schema_worksheet_template.ipynb


>>>PreprocessingJob

FeatureToTfRecord(date=2019-09-18, days_back=7, channel=Push, test_run=False, schema_file=push.pbtxt, sample=True, sample_rate=0.5, over_sample_rate=1.0, base_path=gs://mo_ml/push/tf)

docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train PreprocessingJob --date 2019-08-11 --channel Push --label-name os_level_unsub --schema-file push.pbtxt --feature-set OneHotFeatures --sample-rate 0.50 --base-path gs://mo_ml/lingh/"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train PreprocessingJob --date 2019-08-12 --channel Push --label-name os_level_unsub --schema-file push.pbtxt --feature-set OneHotFeatures --sample-rate 0.50 --base-path gs://mo_ml/lingh/"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train PreprocessingJob --date 2019-08-13 --channel Push --label-name os_level_unsub --schema-file push.pbtxt --feature-set OneHotFeatures --sample-rate 0.50 --base-path gs://mo_ml/lingh/"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train PreprocessingJob --date 2019-08-21 --channel Push --label-name os_level_unsub --schema-file push.pbtxt --feature-set OneHotFeatures --sample-rate 0.50 --base-path gs://mo_ml/lingh/"

docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train PreprocessingJob --date 2019-09-18 --channel Push --label-name os_level_unsub --schema-file push.pbtxt --feature-set OneHotFeatures --sample-rate 0.50 --base-path gs://mo_ml/lingh/"

===== Luigi Execution Summary =====

Scheduled 2 tasks of which:
* 1 complete ones were encountered:
    - 1 FeatureToTfRecord(...)
* 1 ran successfully:
    - 1 PreprocessingJob(...)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 2 tasks of which:
* 1 complete ones were encountered:
    - 1 FeatureToTfRecord(...)
* 1 ran successfully:
    - 1 PreprocessingJob(...)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

Outputs -

gsutil ls gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21

gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/transformed_metadata/schema.pbtxt
gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/transformed_metadata/schema.pbtxt \

>>>TrainingJob locally

https://docs.google.com/document/d/1Nsqhnn_joBvbnbsra19tRgp9nlMPK_PWZZVjpXQwQYM/edit

gcloud ml-engine local train \
--module-name trainer.task \
--package-path trainer/ \
--distributed \
-- \
--train-files "gs://si-ml-at-scale-ua-es-w7-f20/2016*/*/*.tfrecords" \
--eval-files "gs://si-ml-at-scale-ua-es-w7-f20/2017*/*/*.tfrecords" \
--train-steps 5000 \
--eval-steps 10 \
--train-batch-size 200 \
--eval-batch-size 200 \
--job-dir ../output/


-->> local training and evaluation


gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/transform_fn/{saved_model.pbtxt|saved_model.pb}


gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/part-*.tfrecords
gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-12/20190820T142317.888107-ba9c8cf49c3c"


gsutil mv gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/part-*.tfrecords gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/training/
gsutil mv gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/evaluation/part-*.tfrecords gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/evaluation/
gsutil mv gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/transform_fn/* gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/transform_fn/


gcloud ml-engine local train \
--job-dir gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8 \
--module-name trainers.models.ModelTask \
--package-path tf-supervised/src/main/python/tasks/../trainers --distributed \
-- \
--verbosity DEBUG \
--tf-transform-dir gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/ \
--schema-txt-file gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/transformed_metadata/schema.pbtxt \
--local-run False --channel Push --feature-set OneHotFeatures \
--label-name os_level_unsub \
--estimator_type NeuralNet \
--n_classes 2 \
--batch-size 32 \
--max-steps 25000 \
--log-eval-bq-table paradox-mo.up_ml_push.eval_out_Push_os_level_unsub_OneHotFeatures_NeuralNet

Use standard file APIs to check for files with this prefix.
INFO:tensorflow:Restoring parameters from gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/model.ckpt-25002
INFO:tensorflow:Running local_init_op.
INFO:tensorflow:Done running local_init_op.
INFO:tensorflow:Evaluation [10/100]
INFO:tensorflow:Evaluation [20/100]
INFO:tensorflow:Evaluation [30/100]
INFO:tensorflow:Evaluation [40/100]
INFO:tensorflow:Evaluation [50/100]
INFO:tensorflow:Evaluation [60/100]
INFO:tensorflow:Evaluation [70/100]
INFO:tensorflow:Evaluation [80/100]
INFO:tensorflow:Evaluation [90/100]
INFO:tensorflow:Evaluation [100/100]
INFO:tensorflow:Finished evaluation at 2019-09-03-17:16:22
INFO:tensorflow:Saving dict for global step 25003: accuracy = 0.9978125, auc = 0.573017, confusion_matrix = [[3193.    0.]
 [   7.    0.]], f1_score = 0.0043654502, global_step = 25003, loss = 0.6072883, precision = 0.0, recall = 0.0, true_pos = 0.0
INFO:tensorflow:Summary for np.ndarray is not visible in Tensorboard by default. Consider using a Tensorboard plugin for visualization (see https://github.com/tensorflow/tensorboard-plugin-example/blob/master/README.md for more information).
INFO:tensorflow:Saving 'checkpoint_path' summary for global step 25003: gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/model.ckpt-25002
INFO:tensorflow:Performing the final export in the end of training.


saved_model_dir, logical_input_map, tensor_replacement_map)
  File "/usr/local/lib/python2.7/site-packages/tensorflow_transform/saved/saved_transform_io.py", line 173, in _partially_apply_saved_transform_impl
    'to transform: {}'.format(unexpected_inputs))
ValueError: Unexpected inputs to transform: set([u'weight_column'])


-->>Train the model using Cloud ML Engine (Failed due to multiple reasons)

docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/slayton:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-06 --schema-file inapp.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"

===== Luigi Execution Summary =====

Scheduled 31 tasks of which:
* 14 complete ones were encountered:
    - 7 CommunicationsHealth(date=2019-07-31...2019-08-06)
    - 7 UserCommunicationSnapshot(date=2019-07-27...2019-08-02)
* 14 ran successfully:
    - 7 LabelJob(date=2019-07-31...2019-08-06, channel=Push)
    - 7 UserAggregatesJob(date=2019-07-27, date_label_table_to_join=2019-07-31, channel=Push) ...
* 1 failed:
    - 1 FeatureToTfRecord(...)
* 2 were left pending, among these:
    * 2 had failed dependencies:
        - 1 PreprocessingJob(...)
        - 1 TrainingJob(...)

This progress looks :( because there were failed tasks

===== Luigi Execution Summary =====


docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/slayton:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-06 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 31 tasks of which:
* 14 complete ones were encountered:
    - 7 CommunicationsHealth(date=2019-07-31...2019-08-06)
    - 7 UserCommunicationSnapshot(date=2019-07-27...2019-08-02)
* 14 ran successfully:
    - 7 LabelJob(date=2019-07-31...2019-08-06, channel=Push)
    - 7 UserAggregatesJob(date=2019-07-27, date_label_table_to_join=2019-07-31, channel=Push) ...
* 1 failed:
    - 1 FeatureToTfRecord(...)
* 2 were left pending, among these:
    * 2 had failed dependencies:
        - 1 PreprocessingJob(...)
        - 1 TrainingJob(...)

This progress looks :( because there were failed tasks


https://console.cloud.google.com/dataflow/jobsDetail/locations/europe-west1/jobs/2019-08-13_12_04_11-6484089745223505211?project=paradox-mo
===== Luigi Execution Summary =====

docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-10 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-11 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"


===== Luigi Execution Summary =====

Scheduled 25 tasks of which:
* 14 complete ones were encountered:
    - 4 CommunicationsHealth(date=2019-08-07...2019-08-10)
    - 3 LabelJob(date=2019-08-04...2019-08-06, channel=Push)
    - 3 UserAggregatesJob(date=2019-07-31, date_label_table_to_join=2019-08-04, channel=Push) ...
    - 4 UserCommunicationSnapshot(date=2019-08-03...2019-08-06)
* 10 ran successfully:
    - 1 FeatureToTfRecord(...)
    - 4 LabelJob(date=2019-08-07...2019-08-10, channel=Push)
    - 1 PreprocessingJob(...)
    - 4 UserAggregatesJob(date=2019-08-03, date_label_table_to_join=2019-08-07, channel=Push) ...
* 1 failed:
    - 1 TrainingJob(...)

This progress looks :( because there were failed tasks


===== Luigi Execution Summary =====

Scheduled 25 tasks of which:
* 14 complete ones were encountered:
    - 4 CommunicationsHealth(date=2019-08-07...2019-08-10)
    - 3 LabelJob(date=2019-08-04...2019-08-06, channel=Push)
    - 3 UserAggregatesJob(date=2019-07-31, date_label_table_to_join=2019-08-04, channel=Push) ...
    - 4 UserCommunicationSnapshot(date=2019-08-03...2019-08-06)
* 10 ran successfully:
    - 1 FeatureToTfRecord(...)
    - 4 LabelJob(date=2019-08-07...2019-08-10, channel=Push)
    - 1 PreprocessingJob(...)
    - 4 UserAggregatesJob(date=2019-08-03, date_label_table_to_join=2019-08-07, channel=Push) ...
* 1 failed:
    - 1 TrainingJob(...)

This progress looks :( because there were failed tasks

===== Luigi Execution Summary =====

/api/v3/revisions/pdx_mo.Push.os_level_unsub.Train.OneHotFeatures.NeuralNet/2019-08-10

hades ls mo_ml.push 2019-08-11
gsutil ls gs://mo_ml/lingh/

gsutil ls gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.OneHotFeatures.NeuralNet/2019-08-11

hades ls mo_ml.lingh.tfrecords.pdx_mo.Push.BaseInputDataV1.train 2019-08-13

hades ls mo_ml.push.tf.tfrecords.Push.BaseInputDataV1.train 2019-08-13

>>> TrainingJob E2E Pipeline
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"

INFO:
===== Luigi Execution Summary =====

Scheduled 2 tasks of which:
* 1 complete ones were encountered:
    - 1 PreprocessingJob(...)
* 1 ran successfully:
    - 1 TrainingJob(...)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 2 tasks of which:
* 1 complete ones were encountered:
    - 1 PreprocessingJob(...)
* 1 ran successfully:
    - 1 TrainingJob(...)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

>>> TrainingJobTrainingJob E2E Pipeline
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-12 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"

===== Luigi Execution Summary =====

Scheduled 17 tasks of which:
* 14 complete ones were encountered:
    - 7 LabelJob(date=2019-08-06...2019-08-12, channel=Push)
    - 7 UserAggregatesJob(date=2019-08-02, date_label_table_to_join=2019-08-06, channel=Push) ...
* 3 ran successfully:
    - 1 FeatureToTfRecord(...)
    - 1 PreprocessingJob(...)
    - 1 TrainingJob(...)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 17 tasks of which:
* 14 complete ones were encountered:
    - 7 LabelJob(date=2019-08-06...2019-08-12, channel=Push)
    - 7 UserAggregatesJob(date=2019-08-02, date_label_table_to_join=2019-08-06, channel=Push) ...
* 3 ran successfully:
    - 1 FeatureToTfRecord(...)
    - 1 PreprocessingJob(...)
    - 1 TrainingJob(...)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====


gsutil mv gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/transformed_metadata/v1-json/schema.json

https://github.com/GoogleCloudPlatform/cloudml-samples/blob/master/census/tftransformestimator/trainer/model.py

gcloud ml-engine local train \
  --job-dir gs://slayton_test/email_labels/tf/jobs/20190211/15 \
  --module-name trainers.linear_model \
  --package-path tf-supervised/src/main/python/trainers \
  --distributed \
  -- \
  --labeled-data gs://slayton_test/email_labels/tf/examples/20190211 \
  --verbosity DEBUG \
  --tf-transform-dir gs://slayton_test/preprocess/20190211 \
  --schema-txt-file gs://slayton_test/email_labels/tf/examples/20190211/training/schema.pbtxt


gcloud ml-engine local train \
  --job-dir gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8 \
  --module-name trainers.models.ModelTask \
  --package-path tf-supervised/src/main/python/tasks/../trainers --distributed \
  -- \
  --verbosity DEBUG \
  --tf-transform-dir gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/ \
  --schema-txt-file gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/transformed_metadata/schema.pbtxt \
  --local-run False --channel Push --feature-set OneHotFeatures \
  --label-name os_level_unsub \
  --estimator_type NeuralNet \
  --n_classes 2 \
  --batch-size 32 \
  --max-steps 25000 \
  --log-eval-bq-table paradox-mo.up_ml_push.eval_out_Push_os_level_unsub_OneHotFeatures_NeuralNet


gcloud beta ai-platform jobs submit training $JOB_ID \
  $HYPERTUNE_PARAM \
  --region $REGION \
  --scale-tier BASIC_GPU \
  --master-image-uri $IMAGE_URI \
  -- \
  ai-platform-example $MODULE \
  --job-id=1

Google Cloud Platform (GCP) let you build and host applications and websites, store data, analyze data on Goolge infrastructure.
AI Platform is a managed service that enables you to easily build ML models that work on any types of data, of any size.
Goolge Cloud Storages(GCS) is a unified object storage for developers from live serving to data analytics/ML to data archiving.
Cloud SDK is a command line tool which allows you to interact with Google Cloud products. 


What is the difference between tf.estimator.TrainSpec


-->> NeuralNet

docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set DefaultFeatures --sample-rate 0.50 --max-steps 25000"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set BucketizedFeatures --sample-rate 0.50 --max-steps 25000"

docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-21 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set BucketizedFeatures --sample-rate 0.50 --max-steps 25000"

qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.BucketizedFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-11

-->> Linear model

docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type LinearModel --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type LinearModel --feature-set DefaultFeatures --sample-rate 0.50 --max-steps 25000"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type LinearModel --feature-set BucketizedFeatures --sample-rate 0.50 --max-steps 25000"
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-09-18 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type LinearModel --feature-set BucketizedFeatures --sample-rate 0.50 --max-steps 25000"

-- Need to drop hades before retrain models
docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-21 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set DefaultFeatures --sample-rate 1.0 --over-sample-rate 100.0 --max-steps 25000"

-- Run qstyx to run jobs
qstyx run -f data-info.yaml -w <id.name> -i <docker_image> --append-commands -- --model-suffix=<your_suffix>

qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-12
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.OneHotFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-21

--> qstyx Errors
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-12
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-12


qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.LinearClassifier.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-1
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.LinearClassifier.OneHotFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-21
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-11
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.OneHotFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-21

->> BoostedTrees

docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type BoostedTrees --feature-set OneHotFeatures --sample-rate 0.50 --max-steps 25000"

Traceback (most recent call last):
  File "/usr/local/lib/python2.7/dist-packages/luigi/worker.py", line 199, in run
    new_deps = self._run_get_new_deps()
  File "/usr/local/lib/python2.7/dist-packages/luigi/worker.py", line 139, in _run_get_new_deps
    task_gen = self.task.run()
  File "/usr/share/tf-supervised/python/tasks/luigi_task_train.py", line 454, in run
    super(TrainingJob, self).run()
  File "/usr/local/lib/python2.7/dist-packages/spotify_tensorflow/luigi/tensorflow_task.py", line 93, in run
    run_with_logging(cmd, logger)
  File "/usr/local/lib/python2.7/dist-packages/spotify_tensorflow/luigi/utils.py", line 68, in run_with_logging
    raise subprocess.CalledProcessError(exit_code, cmd, output=output)
CalledProcessError: Command '['gcloud', 'ml-engine', '--project=paradox-mo', 'jobs', 'submit', 'training', 'root_TrainingJob_d75c8cc5_6f46_433e_a5be_da5d9b1b6546', '--region=europe-west1', '--job-dir=gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.OneHotFeatures.BoostedTrees/2019-08-13/20190913T181022.535166-70cc94df7884', '--stream-logs', '--scale-tier=STANDARD_1', '--package-path=/usr/share/tf-supervised/python/tasks/../trainers', '--module-name=trainers.models.ModelTask', '--', u'--tf-transform-dir=gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-13/20190816T173250.368887-0cc0cba73eea', '--verbosity=DEBUG', '--schema-txt-file=/root/.local/lib/python2.7/site-packages/trainers/schemas/push.pbtxt', '--local-run=False', '--channel=Push', '--feature-set=OneHotFeatures', '--label-name=os_level_unsub', '--estimator_type=BoostedTrees', '--n_classes=2', '--batch-size=32', '--max-steps=25000', '--log-eval-bq-table=paradox-mo.mo_ml_push.eval_out_Push_os_level_unsub_OneHotFeatures_BoostedTrees']' returned non-zero exit status 1
DEBUG: 1 running tasks, waiting for next task to finish
DEBUG:luigi-interface:1 running tasks, waiting for next task to finish
INFO: Informed scheduler that task   TrainingJob_True_Push_False_8811171691   has status   FAILED
INFO:luigi-interface:Informed scheduler that task   TrainingJob_True_Push_False_8811171691   has status   FAILED
DEBUG: Asking scheduler for work...
DEBUG:luigi-interface:Asking scheduler for work...
DEBUG: Done
DEBUG:luigi-interface:Done
DEBUG: There are no more tasks to run at this time
DEBUG:luigi-interface:There are no more tasks to run at this time
DEBUG: There are 1 pending tasks possibly being run by other workers
DEBUG:luigi-interface:There are 1 pending tasks possibly being run by other workers
DEBUG: There are 1 pending tasks unique to this worker
DEBUG:luigi-interface:There are 1 pending tasks unique to this worker
DEBUG: There are 1 pending tasks last scheduled by this worker
DEBUG:luigi-interface:There are 1 pending tasks last scheduled by this worker
INFO: Worker Worker(salt=626794397, workers=1, host=d3e97eab9925, username=root, pid=32) was stopped. Shutting down Keep-Alive thread
INFO:luigi-interface:Worker Worker(salt=626794397, workers=1, host=d3e97eab9925, username=root, pid=32) was stopped. Shutting down Keep-Alive thread
INFO:
===== Luigi Execution Summary =====

Scheduled 2 tasks of which:
* 1 complete ones were encountered:
    - 1 PreprocessingJob(...)
* 1 failed:
    - 1 TrainingJob(...)

This progress looks :( because there were failed tasks

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 2 tasks of which:
* 1 complete ones were encountered:
    - 1 PreprocessingJob(...)
* 1 failed:
    - 1 TrainingJob(...)

This progress looks :( because there were failed tasks

===== Luigi Execution Summary =====

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 31 tasks of which:
* 12 complete ones were encountered:
    - 7 CommunicationsHealth(date=2019-10-07...2019-10-13)
    - 5 UserCommunicationSnapshot(date=2019-10-03...2019-10-07)
* 12 ran successfully:
    - 7 LabelJob(date=2019-10-07...2019-10-13, channel=Push)
    - 5 UserAggregatesJob(date=2019-10-03, date_label_table_to_join=2019-10-07, channel=Push) ...
* 7 were left pending, among these:
    * 2 were missing external dependencies:
        - 2 UserCommunicationSnapshot(date=2019-10-08,2019-10-09)
    * 5 had missing dependencies:
        - 1 FeatureToTfRecord(...)
        - 1 PreprocessingJob(...)
        - 1 TrainingJob(...)
        - 2 UserAggregatesJob(date=2019-10-08, date_label_table_to_join=2019-10-12, channel=Push) and UserAggregatesJob(date=2019-10-09, date_label_table_to_join=2019-10-13, channel=Push)

This progress looks :| because there were missing external dependencies

===== Luigi Execution Summary =====


===================================================
## Paradox messaging-optimization -Edison Tasks
===================================================
- change the model estimator (existing estimator)
- change the featureset - TRANSFORM + FEATURES (existing featureset)
- change hypeparameters

- generate feature importance for model
- generate predictions for model

- add a new featureset
- add a new label (previous 7 days active days) 
- add new estimator-specific hyperparam
- add a new model estimater


->> Luigi_task_predict
https://ghe.spotify.net/paradox/messaging-optimization/blob/master/messaging-optimization-pipeline/tf-supervised/src/main/python/tasks/luigi_task_predict.py

docker run -it \
-v $(pwd)/../key.json:/key.json \
-e GOOGLE_APPLICATION_CREDENTIALS=/key.json \
gcr.io/users-protection/tf-supervised/shuoy:latest \
bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_predict_email EmailBatchPredictionsJob \
--date 2019-04-17 --bq-dataset hchudgar --bq-table-input alm_users_treatment --bq-table-output test_out_shuoy \
--test-run --schema-file gs://slayton_test/email_open/tf/examples/schema_snapshot_txt.pbtxt \
--campaign-meta '{\"optout_type\": \"Spotify News and Offers (default) - Email\", \"campaign_type\": \"Spotify News and Offers (default) - Email\"}'"


    date = luigi.DateParameter(default=dt.date.today())
    # NOTE: We might need specific combinations of channels and labels, e.g. Email.opened and
    # Push.clicked; some channels might not have support for some labels e.g. Push for opened
    channel = luigi.Parameter(default=env_vars.CHANNEL_EMAIL)
    label_name = luigi.Parameter(default="os_level_unsub")
    campaign_meta = luigi.DictParameter(description="Dict for campaign meta information")
    bq_dataset = luigi.Parameter()
    bq_table_input = luigi.Parameter()
    bq_table_output = luigi.Parameter()
    schema_file = luigi.Parameter(description="Schema File")
    schema_file_path = None
    estimator_type = luigi.Parameter(default="LinearModel")
    feature_set = luigi.Parameter(default="DefaultFeatures")
    test_run = luigi.BoolParameter(default=False, description="Run the model in 'test' mode.")



docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_predict BatchPredictionJob --date 2019-08-22 --bq-dataset mo_ml_push --bq-table-input labels_v1_20190914 --bq-table-output prediction_1 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set BucketizedFeatures --campaign-meta '{\"optout_type\": \"Push\", \"campaign_type\": \"Push\"}' "
docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_predict BatchPredictionJob --date 2019-08-21 --bq-dataset mo_ml_push --bq-table-input labels_v1_20191007 --bq-table-output prediction_2 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set BucketizedFeatures --campaign-meta '{\"optout_type\": \"Push\", \"campaign_type\": \"Push\"}' "

➜  messaging-optimization-pipeline git:(opt-out-label) ✗ docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_predict BatchPredictionJob --date 2019-08-22 --bq-dataset mo_ml_push --bq-table-input labels_v1_20190914 --bq-table-output prediction_1 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set BucketizedFeatures --campaign-meta '{\"optout_type\": \"Push\", \"campaign_type\": \"Push\"}' "
Activated service account credentials for: [paradox-mo@paradox-mo.iam.gserviceaccount.com]
Updated property [core/project].
DEBUG: Checking if BatchPredictionJob(date=2019-08-22, channel=Push, label_name=os_level_unsub, campaign_meta={"optout_type": "Push", "campaign_type": "Push"}, bq_dataset=mo_ml_push, bq_table_input=labels_v1_20190914, bq_table_output=prediction_1, schema_file=push.pbtxt, estimator_type=NeuralNet, feature_set=BucketizedFeatures, test_run=False) is complete
DEBUG:luigi-interface:Checking if BatchPredictionJob(date=2019-08-22, channel=Push, label_name=os_level_unsub, campaign_meta={"optout_type": "Push", "campaign_type": "Push"}, bq_dataset=mo_ml_push, bq_table_input=labels_v1_20190914, bq_table_output=prediction_1, schema_file=push.pbtxt, estimator_type=NeuralNet, feature_set=BucketizedFeatures, test_run=False) is complete
INFO: Informed scheduler that task   BatchPredictionJob_mo_ml_push_labels_v1_201909_prediction_1_b7c6947f42   has status   DONE
INFO:luigi-interface:Informed scheduler that task   BatchPredictionJob_mo_ml_push_labels_v1_201909_prediction_1_b7c6947f42   has status   DONE
INFO: Done scheduling tasks
INFO:luigi-interface:Done scheduling tasks
INFO: Running Worker with 1 processes
INFO:luigi-interface:Running Worker with 1 processes
DEBUG: Asking scheduler for work...
DEBUG:luigi-interface:Asking scheduler for work...
DEBUG: Done
DEBUG:luigi-interface:Done
DEBUG: There are no more tasks to run at this time
DEBUG:luigi-interface:There are no more tasks to run at this time
INFO: Worker Worker(salt=914244436, workers=1, host=58c2135e5a89, username=root, pid=33) was stopped. Shutting down Keep-Alive thread
INFO:luigi-interface:Worker Worker(salt=914244436, workers=1, host=58c2135e5a89, username=root, pid=33) was stopped. Shutting down Keep-Alive thread
INFO:
===== Luigi Execution Summary =====

Scheduled 1 tasks of which:
* 1 complete ones were encountered:
    - 1 BatchPredictionJob(...)

Did not run any tasks
This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 1 tasks of which:
* 1 complete ones were encountered:
    - 1 BatchPredictionJob(...)

Did not run any tasks
This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====


->> Feature Importance

python tf-supervised/src/main/python/trainers/feature_importance.py \ 
--runner DataflowRunner \
--dataset hchudgar \
--input_bq_table alm_temp \ 
--output_bq_table importance_test_15 \ 
--output_dataset slayton_test_eu \
--model_dir gs://slayton_test/email_open/tf/job_dir/email_open.Train.LinearModel.DefaultFeatures.test_run/2019-04-16/20190417T223015.151765-90618422cb77/export/final_model/1555540698 \ 
--schema tf-supervised/src/main/python/trainers/schemas/email_open.pbtxt \
--max_rows 10000

        SELECT EmailLabel AS emailLabelTable,
               UserAgg AS userAggTable
        FROM `users-protection.up_ml.email_labels_v1_20190416` as EmailLabel
        RIGHT JOIN
         `users-protection.up_ml.user_aggregation_data_v1_20190408` as UserAgg
        ON EmailLabel.user_id = UserAgg.user_id
        RIGHT JOIN `users-protection.{dataset}.{input_bq_table}` predict_table
        ON UserAgg.user_id = predict_table.userId
        limit {max_rows}

-- would like to use board audience
        SELECT os_level_unsub AS LabelTable,
               UserAgg AS userAggTable
        FROM `paradox-mo.mo_ml_push.labels_v1_20190914` as Label
        RIGHT JOIN `paradox-mo.mo_ml_push.user_aggregation_data_v1_20190914` as UserAgg
        ON Label.user_id = UserAgg.user_id
        RIGHT JOIN `paradox-mo.lingh_test.broad_audience_20191013` predict_table
        ON UserAgg.user_id = predict_table.user_id


-- use the original data set with extreme samll counts
        with rawdata As (
        SELECT Label AS LabelTable,
               UserAgg AS userAggTable
        FROM `paradox-mo.mo_ml_push.labels_v1_20190914` as Label
        RIGHT JOIN `paradox-mo.mo_ml_push.user_aggregation_data_v1_20190914` as UserAgg
        ON Label.user_id = UserAgg.user_id
        RIGHT JOIN `paradox-mo.mo_ml_push.labels_v1_20190914` predict_table
        ON UserAgg.user_id = predict_table.user_id)
        
        select sum(LabelTable.os_level_unsub) from rawdata;


        SELECT Label AS LabelTable,
               UserAgg AS userAggTable
        FROM `paradox-mo.mo_ml_push.labels_v1_20190914` as Label
        JOIN `paradox-mo.mo_ml_push.user_aggregation_data_v1_20190913`as UserAgg
        ON Label.user_id = UserAgg.user_id

python tf-supervised/src/main/python/trainers/feature_importance.py \ 
--runner DataflowRunner \
--dataset hchudgar \
--input_bq_table alm_temp \ 
--output_bq_table importance_test_15 \ 
--output_dataset slayton_test_eu \
--model_dir gs://slayton_test/email_open/tf/job_dir/email_open.Train.LinearModel.DefaultFeatures.test_run/2019-04-16/20190417T223015.151765-90618422cb77/export/final_model/1555540698 \ 
--schema tf-supervised/src/main/python/trainers/schemas/email_open.pbtxt \
--max_rows 10000

python tf-supervised/src/main/python/trainers/feature_importance.py --runner DataflowRunner --dataset mo_ml_push --input_bq_table labels_v1_20190821 --output_bq_table importance_test_1 --output_dataset mo_ml_push \
--model_dir gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/ \
--schema gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.BucketizedFeatures/2019-08-21/20190920T180822.182445-4c59f8160685/transformed_metadata/schema.pbtxt \
--max_rows 1000


python tf-supervised/src/main/python/trainers/feature_importance.py --runner DataflowRunner --dataset mo_ml_push \
--input_bq_table labels_v1_20190821 \
--output_bq_table importance_test_1 \
--output_dataset mo_ml_push \
--model_dir gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/ \
--schema gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.BucketizedFeatures/2019-08-21/20190920T180822.182445-4c59f8160685/transformed_metadata/schema.pbtxt \
--max_rows 2000


➜  messaging-optimization-pipeline git:(opt-out-label) ✗ python tf-supervised/src/main/python/trainers/feature_importance.py --runner DataflowRunner --dataset mo_ml_push --input_bq_table labels_v1_20190914 --output_bq_table importance_test_1 --output_dataset mo_ml_push \
--model_dir gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/ \
--schema gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.BucketizedFeatures/2019-08-21/20190920T180822.182445-4c59f8160685/transformed_metadata/schema.pbtxt \
--max_rows 10000000
Should Write to BQ
warning: sdist: standard file not found: should have one of README, README.rst, README.txt, README.md

warning: check: missing meta-data: either (author and author_email) or (maintainer and maintainer_email) must be supplied

DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
Traceback (most recent call last):
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 398, in <module>
    known_args.max_rows
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 282, in predict_tfrecords
    print("Should Write to BQ")
  File "/usr/local/lib/python2.7/site-packages/apache_beam/pipeline.py", line 426, in __exit__
    self.run().wait_until_finish()
  File "/usr/local/lib/python2.7/site-packages/apache_beam/runners/dataflow/dataflow_runner.py", line 1238, in wait_until_finish
    (self.state, getattr(self._runner, 'last_error_msg', None)), self)
apache_beam.runners.dataflow.dataflow_runner.DataflowRuntimeException: Dataflow pipeline failed. State: FAILED, Error:
Traceback (most recent call last):
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/batchworker.py", line 649, in do_work
    work_executor.execute()
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/executor.py", line 176, in execute
    op.start()
  File "dataflow_worker/native_operations.py", line 38, in dataflow_worker.native_operations.NativeReadOperation.start
    def start(self):
  File "dataflow_worker/native_operations.py", line 39, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.scoped_start_state:
  File "dataflow_worker/native_operations.py", line 44, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.spec.source.reader() as reader:
  File "dataflow_worker/native_operations.py", line 54, in dataflow_worker.native_operations.NativeReadOperation.start
    self.output(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 223, in apache_beam.runners.worker.operations.Operation.output
    cython.cast(Receiver, self.receivers[output_index]).receive(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 131, in apache_beam.runners.worker.operations.SingletonConsumerSet.receive
    self.consumer.process(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 537, in apache_beam.runners.worker.operations.DoOperation.process
    with self.scoped_process_state:
  File "apache_beam/runners/worker/operations.py", line 538, in apache_beam.runners.worker.operations.DoOperation.process
    delayed_application = self.dofn_receiver.receive(o)
  File "apache_beam/runners/common.py", line 723, in apache_beam.runners.common.DoFnRunner.receive
    self.process(windowed_value)
  File "apache_beam/runners/common.py", line 729, in apache_beam.runners.common.DoFnRunner.process
    self._reraise_augmented(exn)
  File "apache_beam/runners/common.py", line 777, in apache_beam.runners.common.DoFnRunner._reraise_augmented
    raise_with_traceback(new_exn)
  File "apache_beam/runners/common.py", line 727, in apache_beam.runners.common.DoFnRunner.process
    return self.do_fn_invoker.invoke_process(windowed_value)
  File "apache_beam/runners/common.py", line 418, in apache_beam.runners.common.SimpleInvoker.invoke_process
    output_processor.process_outputs(
  File "apache_beam/runners/common.py", line 823, in apache_beam.runners.common._OutputProcessor.process_outputs
    for result in results:
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 153, in _predict_pipe
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 68, in _convert
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 52, in _int64_feature
TypeError: u'android' has type numpy.unicode_, but expected one of: int, long [while running 'Feature Importance Preds/ParDo(_FeatureImportanceDoFn)']


➜  messaging-optimization-pipeline git:(opt-out-label) ✗ python tf-supervised/src/main/python/trainers/feature_importance.py --runner DataflowRunner --dataset mo_ml_push --input_bq_table labels_v1_20190914 --output_bq_table importance_test_1 --output_dataset mo_ml_push \
--model_dir gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/ \
--schema gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.BucketizedFeatures/2019-08-21/20190920T180822.182445-4c59f8160685/transformed_metadata/schema.pbtxt \
--max_rows 10000000
Should Write to BQ
warning: sdist: standard file not found: should have one of README, README.rst, README.txt, README.md

warning: check: missing meta-data: either (author and author_email) or (maintainer and maintainer_email) must be supplied

DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
Traceback (most recent call last):
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 396, in <module>
    known_args.max_rows
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 280, in predict_tfrecords
    print("Should Write to BQ")
  File "/usr/local/lib/python2.7/site-packages/apache_beam/pipeline.py", line 426, in __exit__
    self.run().wait_until_finish()
  File "/usr/local/lib/python2.7/site-packages/apache_beam/runners/dataflow/dataflow_runner.py", line 1238, in wait_until_finish
    (self.state, getattr(self._runner, 'last_error_msg', None)), self)
apache_beam.runners.dataflow.dataflow_runner.DataflowRuntimeException: Dataflow pipeline failed. State: FAILED, Error:
Traceback (most recent call last):
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/batchworker.py", line 649, in do_work
    work_executor.execute()
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/executor.py", line 176, in execute
    op.start()
  File "dataflow_worker/native_operations.py", line 38, in dataflow_worker.native_operations.NativeReadOperation.start
    def start(self):
  File "dataflow_worker/native_operations.py", line 39, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.scoped_start_state:
  File "dataflow_worker/native_operations.py", line 44, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.spec.source.reader() as reader:
  File "dataflow_worker/native_operations.py", line 54, in dataflow_worker.native_operations.NativeReadOperation.start
    self.output(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 223, in apache_beam.runners.worker.operations.Operation.output
    cython.cast(Receiver, self.receivers[output_index]).receive(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 131, in apache_beam.runners.worker.operations.SingletonConsumerSet.receive
    self.consumer.process(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 537, in apache_beam.runners.worker.operations.DoOperation.process
    with self.scoped_process_state:
  File "apache_beam/runners/worker/operations.py", line 538, in apache_beam.runners.worker.operations.DoOperation.process
    delayed_application = self.dofn_receiver.receive(o)
  File "apache_beam/runners/common.py", line 723, in apache_beam.runners.common.DoFnRunner.receive
    self.process(windowed_value)
  File "apache_beam/runners/common.py", line 729, in apache_beam.runners.common.DoFnRunner.process
    self._reraise_augmented(exn)
  File "apache_beam/runners/common.py", line 777, in apache_beam.runners.common.DoFnRunner._reraise_augmented
    raise_with_traceback(new_exn)
  File "apache_beam/runners/common.py", line 727, in apache_beam.runners.common.DoFnRunner.process
    return self.do_fn_invoker.invoke_process(windowed_value)
  File "apache_beam/runners/common.py", line 418, in apache_beam.runners.common.SimpleInvoker.invoke_process
    output_processor.process_outputs(
  File "apache_beam/runners/common.py", line 823, in apache_beam.runners.common._OutputProcessor.process_outputs
    for result in results:
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 151, in _predict_pipe
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 72, in _convert
KeyError: u"pushLabelTable.time_send [while running 'Feature Importance Preds/ParDo(_FeatureImportanceDoFn)']"


➜  messaging-optimization-pipeline git:(opt-out-label) ✗ python tf-supervised/src/main/python/trainers/feature_importance.py --runner DataflowRunner --dataset mo_ml_push \
--input_bq_table labels_v1_20190821 \
--output_bq_table importance_test_1 \
--output_dataset mo_ml_push \
--model_dir gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/ \
--schema gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.BucketizedFeatures/2019-08-21/20190920T180822.182445-4c59f8160685/transformed_metadata/schema.pbtxt \
--max_rows 2000
Should Write to BQ
warning: sdist: standard file not found: should have one of README, README.rst, README.txt, README.md

warning: check: missing meta-data: either (author and author_email) or (maintainer and maintainer_email) must be supplied

DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
WARNING:root:Discarding unparseable args: ['--max_workers=10']
WARNING:root:Discarding unparseable args: ['--max_workers=10']
Traceback (most recent call last):
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 397, in <module>
    known_args.max_rows
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 280, in predict_tfrecords
    print("Should Write to BQ")
  File "/usr/local/lib/python2.7/site-packages/apache_beam/pipeline.py", line 426, in __exit__
    self.run().wait_until_finish()
  File "/usr/local/lib/python2.7/site-packages/apache_beam/runners/dataflow/dataflow_runner.py", line 1238, in wait_until_finish
    (self.state, getattr(self._runner, 'last_error_msg', None)), self)
apache_beam.runners.dataflow.dataflow_runner.DataflowRuntimeException: Dataflow pipeline failed. State: FAILED, Error:
Traceback (most recent call last):
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/batchworker.py", line 649, in do_work
    work_executor.execute()
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/executor.py", line 176, in execute
    op.start()
  File "dataflow_worker/native_operations.py", line 38, in dataflow_worker.native_operations.NativeReadOperation.start
    def start(self):
  File "dataflow_worker/native_operations.py", line 39, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.scoped_start_state:
  File "dataflow_worker/native_operations.py", line 44, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.spec.source.reader() as reader:
  File "dataflow_worker/native_operations.py", line 54, in dataflow_worker.native_operations.NativeReadOperation.start
    self.output(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 223, in apache_beam.runners.worker.operations.Operation.output
    cython.cast(Receiver, self.receivers[output_index]).receive(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 131, in apache_beam.runners.worker.operations.SingletonConsumerSet.receive
    self.consumer.process(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 537, in apache_beam.runners.worker.operations.DoOperation.process
    with self.scoped_process_state:
  File "apache_beam/runners/worker/operations.py", line 538, in apache_beam.runners.worker.operations.DoOperation.process
    delayed_application = self.dofn_receiver.receive(o)
  File "apache_beam/runners/common.py", line 723, in apache_beam.runners.common.DoFnRunner.receive
    self.process(windowed_value)
  File "apache_beam/runners/common.py", line 729, in apache_beam.runners.common.DoFnRunner.process
    self._reraise_augmented(exn)
  File "apache_beam/runners/common.py", line 777, in apache_beam.runners.common.DoFnRunner._reraise_augmented
    raise_with_traceback(new_exn)
  File "apache_beam/runners/common.py", line 727, in apache_beam.runners.common.DoFnRunner.process
    return self.do_fn_invoker.invoke_process(windowed_value)
  File "apache_beam/runners/common.py", line 418, in apache_beam.runners.common.SimpleInvoker.invoke_process
    output_processor.process_outputs(
  File "apache_beam/runners/common.py", line 823, in apache_beam.runners.common._OutputProcessor.process_outputs
    for result in results:
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 146, in _predict_pipe
KeyError: u"LabelTable.os_level_unsub [while running 'Feature Importance Preds/ParDo(_FeatureImportanceDoFn)']"


An exception was raised when trying to execute the workitem 3348208138100509997 : 
Traceback (most recent call last): File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/batchworker.py", 
line 649, in do_work work_executor.execute() File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/executor.py", 
line 176, in execute op.start() File "dataflow_worker/native_operations.py", 
line 38, in dataflow_worker.native_operations.NativeReadOperation.start def start(self): File "dataflow_worker/native_operations.py", 
line 39, in dataflow_worker.native_operations.NativeReadOperation.start with self.scoped_start_state: File "dataflow_worker/native_operations.py", 
line 44, in dataflow_worker.native_operations.NativeReadOperation.start with self.spec.source.reader() as reader: File "dataflow_worker/native_operations.py", 
line 54, in dataflow_worker.native_operations.NativeReadOperation.start self.output(windowed_value) File "apache_beam/runners/worker/operations.py", 
line 223, in apache_beam.runners.worker.operations.Operation.output cython.cast(Receiver, self.receivers[output_index]).receive(windowed_value) File "apache_beam/runners/worker/operations.py", 
line 131, in apache_beam.runners.worker.operations.SingletonConsumerSet.receive self.consumer.process(windowed_value) File "apache_beam/runners/worker/operations.py", 
line 537, in apache_beam.runners.worker.operations.DoOperation.process with self.scoped_process_state: File "apache_beam/runners/worker/operations.py", 
line 538, in apache_beam.runners.worker.operations.DoOperation.process delayed_application = self.dofn_receiver.receive(o) File "apache_beam/runners/common.py", 
line 723, in apache_beam.runners.common.DoFnRunner.receive self.process(windowed_value) File "apache_beam/runners/common.py", 
line 729, in apache_beam.runners.common.DoFnRunner.process self._reraise_augmented(exn) File "apache_beam/runners/common.py", 
line 777, in apache_beam.runners.common.DoFnRunner._reraise_augmented raise_with_traceback(new_exn) File "apache_beam/runners/common.py", 
line 727, in apache_beam.runners.common.DoFnRunner.process return self.do_fn_invoker.invoke_process(windowed_value) File "apache_beam/runners/common.py", 
line 418, in apache_beam.runners.common.SimpleInvoker.invoke_process output_processor.process_outputs( File "apache_beam/runners/common.py", 
line 823, in apache_beam.runners.common._OutputProcessor.process_outputs for result in results: File "tf-supervised/src/main/python/trainers/feature_importance.py", 
line 146, in _predict_pipe 
KeyError: u"LabelTable.os_level_unsub [while running 'Feature Importance Preds/ParDo(_FeatureImportanceDoFn)']"


➜  messaging-optimization-pipeline git:(opt-out-label) ✗ python tf-supervised/src/main/python/trainers/feature_importance.py --runner DataflowRunner --dataset mo_ml_push \
--input_bq_table labels_v1_20190821 \
--output_bq_table importance_test_1 \
--output_dataset mo_ml_push \
--model_dir gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/ \
--schema gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.BucketizedFeatures/2019-08-21/20190920T180822.182445-4c59f8160685/transformed_metadata/schema.pbtxt \
--max_rows 2000
Should Write to BQ
warning: sdist: standard file not found: should have one of README, README.rst, README.txt, README.md

warning: check: missing meta-data: either (author and author_email) or (maintainer and maintainer_email) must be supplied

DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
Traceback (most recent call last):
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 400, in <module>
    known_args.max_rows
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 283, in predict_tfrecords
    print("Should Write to BQ")
  File "/usr/local/lib/python2.7/site-packages/apache_beam/pipeline.py", line 426, in __exit__
    self.run().wait_until_finish()
  File "/usr/local/lib/python2.7/site-packages/apache_beam/runners/dataflow/dataflow_runner.py", line 1238, in wait_until_finish
    (self.state, getattr(self._runner, 'last_error_msg', None)), self)
apache_beam.runners.dataflow.dataflow_runner.DataflowRuntimeException: Dataflow pipeline failed. State: FAILED, Error:
Traceback (most recent call last):
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/batchworker.py", line 649, in do_work
    work_executor.execute()
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/executor.py", line 176, in execute
    op.start()
  File "dataflow_worker/native_operations.py", line 38, in dataflow_worker.native_operations.NativeReadOperation.start
    def start(self):
  File "dataflow_worker/native_operations.py", line 39, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.scoped_start_state:
  File "dataflow_worker/native_operations.py", line 44, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.spec.source.reader() as reader:
  File "dataflow_worker/native_operations.py", line 54, in dataflow_worker.native_operations.NativeReadOperation.start
    self.output(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 223, in apache_beam.runners.worker.operations.Operation.output
    cython.cast(Receiver, self.receivers[output_index]).receive(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 131, in apache_beam.runners.worker.operations.SingletonConsumerSet.receive
    self.consumer.process(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 537, in apache_beam.runners.worker.operations.DoOperation.process
    with self.scoped_process_state:
  File "apache_beam/runners/worker/operations.py", line 538, in apache_beam.runners.worker.operations.DoOperation.process
    delayed_application = self.dofn_receiver.receive(o)
  File "apache_beam/runners/common.py", line 723, in apache_beam.runners.common.DoFnRunner.receive
    self.process(windowed_value)
  File "apache_beam/runners/common.py", line 729, in apache_beam.runners.common.DoFnRunner.process
    self._reraise_augmented(exn)
  File "apache_beam/runners/common.py", line 777, in apache_beam.runners.common.DoFnRunner._reraise_augmented
    raise_with_traceback(new_exn)
  File "apache_beam/runners/common.py", line 727, in apache_beam.runners.common.DoFnRunner.process
    return self.do_fn_invoker.invoke_process(windowed_value)
  File "apache_beam/runners/common.py", line 418, in apache_beam.runners.common.SimpleInvoker.invoke_process
    output_processor.process_outputs(
  File "apache_beam/runners/common.py", line 823, in apache_beam.runners.common._OutputProcessor.process_outputs
    for result in results:
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 154, in _predict_pipe
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 69, in _convert
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 52, in _int64_feature
TypeError: No positional arguments allowed [while running 'Feature Importance Preds/ParDo(_FeatureImportanceDoFn)']


➜  messaging-optimization-pipeline git:(opt-out-label) ✗ python tf-supervised/src/main/python/trainers/feature_importance.py --runner DataflowRunner --dataset mo_ml_push \
--input_bq_table labels_v1_20190821 \
--output_bq_table importance_test_1 \
--output_dataset mo_ml_push \
--model_dir gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/ \
--schema gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.BucketizedFeatures/2019-08-21/20190920T180822.182445-4c59f8160685/transformed_metadata/schema.pbtxt \
--max_rows 2000
Should Write to BQ
warning: sdist: standard file not found: should have one of README, README.rst, README.txt, README.md

warning: check: missing meta-data: either (author and author_email) or (maintainer and maintainer_email) must be supplied

DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Please upgrade your Python as Python 2.7 won't be maintained after that date. A future version of pip will drop support for Python 2.7. More details about Python 2 support in pip, can be found at https://pip.pypa.io/en/latest/development/release-process/#python-2-support
Traceback (most recent call last):
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 400, in <module>
    known_args.max_rows
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 283, in predict_tfrecords
    print("Should Write to BQ")
  File "/usr/local/lib/python2.7/site-packages/apache_beam/pipeline.py", line 426, in __exit__
    self.run().wait_until_finish()
  File "/usr/local/lib/python2.7/site-packages/apache_beam/runners/dataflow/dataflow_runner.py", line 1238, in wait_until_finish
    (self.state, getattr(self._runner, 'last_error_msg', None)), self)
apache_beam.runners.dataflow.dataflow_runner.DataflowRuntimeException: Dataflow pipeline failed. State: FAILED, Error:
Traceback (most recent call last):
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/batchworker.py", line 649, in do_work
    work_executor.execute()
  File "/usr/local/lib/python2.7/dist-packages/dataflow_worker/executor.py", line 176, in execute
    op.start()
  File "dataflow_worker/native_operations.py", line 38, in dataflow_worker.native_operations.NativeReadOperation.start
    def start(self):
  File "dataflow_worker/native_operations.py", line 39, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.scoped_start_state:
  File "dataflow_worker/native_operations.py", line 44, in dataflow_worker.native_operations.NativeReadOperation.start
    with self.spec.source.reader() as reader:
  File "dataflow_worker/native_operations.py", line 54, in dataflow_worker.native_operations.NativeReadOperation.start
    self.output(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 223, in apache_beam.runners.worker.operations.Operation.output
    cython.cast(Receiver, self.receivers[output_index]).receive(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 131, in apache_beam.runners.worker.operations.SingletonConsumerSet.receive
    self.consumer.process(windowed_value)
  File "apache_beam/runners/worker/operations.py", line 537, in apache_beam.runners.worker.operations.DoOperation.process
    with self.scoped_process_state:
  File "apache_beam/runners/worker/operations.py", line 538, in apache_beam.runners.worker.operations.DoOperation.process
    delayed_application = self.dofn_receiver.receive(o)
  File "apache_beam/runners/common.py", line 723, in apache_beam.runners.common.DoFnRunner.receive
    self.process(windowed_value)
  File "apache_beam/runners/common.py", line 729, in apache_beam.runners.common.DoFnRunner.process
    self._reraise_augmented(exn)
  File "apache_beam/runners/common.py", line 777, in apache_beam.runners.common.DoFnRunner._reraise_augmented
    raise_with_traceback(new_exn)
  File "apache_beam/runners/common.py", line 727, in apache_beam.runners.common.DoFnRunner.process
    return self.do_fn_invoker.invoke_process(windowed_value)
  File "apache_beam/runners/common.py", line 418, in apache_beam.runners.common.SimpleInvoker.invoke_process
    output_processor.process_outputs(
  File "apache_beam/runners/common.py", line 823, in apache_beam.runners.common._OutputProcessor.process_outputs
    for result in results:
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 154, in _predict_pipe
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 66, in _convert
  File "tf-supervised/src/main/python/trainers/feature_importance.py", line 50, in _int64_feature
TypeError: u'android' has type numpy.unicode_, but expected one of: int, long [while running 'Feature Importance Preds/ParDo(_FeatureImportanceDoFn)']


 python tf-supervised/src/main/python/trainers/feature_importance.py --runner DataflowRunner --dataset mo_ml_push \
 --input_bq_table labels_v1_20190914 \
 --output_bq_table importance_test_1 \
 --output_dataset mo_ml_push \
--model_dir gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/ \
--schema gs://mo_ml/lingh/push/training_dataschema.pbtxt \
--max_rows 2000

 ➜  messaging-optimization-pipeline git:(opt-out-label) ✗ gsutil ls gs://mo_ml/push/tf
gs://mo_ml/push/tf/dataflow_staging/
gs://mo_ml/push/tf/dataflow_temp/
gs://mo_ml/push/tf/preprocessing/
gs://mo_ml/push/tf/tfrecords/
gs://mo_ml/push/tf/training/

gsutil ls gs://lingh/Trainer/output/2094/20191008T210721.648053-2ed65366ad9d/eval_model_dir/1570568872/

>> Preprocessing -
 gsuitl ls gs://mo_ml/push/tf/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.BucketizedFeatures/2019-08-21/20190920T180822.182445-4c59f8160685 
Final models -
 gsutil ls gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/


>> Training -  
 gsutil ls gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/ 
➜  messaging-optimization-pipeline git:(opt-out-label) ✗ gsutil ls gs://mo_ml/push/tf/training
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.LinearModel/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.DefaultFeatures.LinearModel/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.DefaultFeatures.NeuralNet/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.OneHotFeatures.BoostedTrees/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.OneHotFeatures.LinearModel/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.OneHotFeatures.NeuralNet/

>> Final Models
➜  beams git:(master) ✗ gsutil ls gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/_MANIFEST.json
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/model_eval.pbtxt
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/saved_model.pb
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/assets/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/variables/


>> JobDir -
 gsutil ls gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8
➜  messaging-optimization-pipeline git:(opt-out-label) ✗ gsutil ls gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-13/
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/


gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/transformed_metadata/schema.pbtxt
gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/transformed_metadata/schema.pbtxt \
gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.OneHotFeatures.NeuralNet/2019-08-11/20190920T191843.805309-d350904d71ff/packages/8e83456dbae7b02f2140795899b360e14330d3a0b79ed465aca014bf5193f75c/


https://beam.apache.org/documentation/pipelines/design-your-pipeline/
https://docs.google.com/document/d/1CX93gRZ-2pOLQ4bL4P1TutlxMwiCSK9X1l5FsdnnpgA/edit

upload file -
gsutil cp -r data/data.csv gs://mo_ml/lingh
gsutil -m cp -r data/data.csv gs://mo_ml/lingh/data
gsutil -m rm -rf gs://mo_ml/lingh/data

with date_data as
(
SELECT _TABLE_SUFFIX as dt FROM `automated-marketing-engagement.campaigns.viva_latino_common_*` 
)

select dt, count(*) from date_data group by 1 order by 1
;


python -m apache_beam.examples.wordcount_minimal \
--input gs://mo_ml/lingh/preprocessing/pdx_mo.Push.os_level_unsub.PreprocessingV1.OneHotFeatures/2019-08-21/20190827T190136.586152-1a00748f68d8/training/transformed_metadata/schema.pbtxt \
--output gs://mo_ml/lingh/data

python wordcount.py --input  "gs://lingh/kubeflow-platform-default/files/66e2e15b29807b05f094869e35ed47df/preprocessing.py" --output  "/Users/lingh/Git/beams/tmp/output.txt"

docker run -it -v $(pwd)/../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest \
bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_predict BatchPredictionJob --date 2019-08-21 \
--bq-dataset mo_ml_push --bq-table-input labels_v1_20191007 --bq-table-output prediction_2 --schema-file push.pbtxt --channel Push \
--label-name os_level_unsub --estimator-type NeuralNet --feature-set BucketizedFeatures --campaign-meta '{\"optout_type\": \"Push\", \"campaign_type\": \"Push\"}' "

python predict.py --input gs://mo_ml/lingh/tfrecords/pdx_mo.Push.BaseInputDataV1.train/2019-09-18/20191016T201928.547801-88e1c3dbdc6a/ \
--output  /Users/lingh/Git/beams/tmp/prediction.txt \
--model-path gs://mo_ml/push/tf/training/pdx_mo.Push.os_level_unsub.Train.BucketizedFeatures.NeuralNet/2019-08-21/20190920T180817.940451-1ca5a7d604d8/export/final_model/1569004297/ \
--batch-size 1000 --project paradox-mo --temp_location gs://mo_ml/lingh/push/tmp/ --staging_location gs://mo_ml/lingh/push/tmp/staging/

python -m apache_beam.examples.wordcount_minimal --input gs://mo_ml/lingh/tfrecords/pdx_mo.Push.BaseInputDataV1.train/2019-09-18/20191016T201928.547801-88e1c3dbdc6a/part-00000-of-00003.tfrecords --output gs://mo_ml/lingh/result.txt


gsutil ls gs://mo_ml/lingh/tfrecords/pdx_mo.Push.BaseInputDataV1.train/2019-08-21/20190827T173336.732193-c52ea29f77ca
gsutil ls gs://mo_ml/lingh/tfrecords/pdx_mo.Push.BaseInputDataV1.evaluation/2019-08-21/20190827T173336.731882-f43a57c51327

python predict.py --input-data gs://mo_ml/lingh/tfrecords/pdx_mo.Push.BaseInputDataV1.train/2019-09-18/20191016T201928.547801-88e1c3dbdc6a/ \
--output gs://mo_ml/lingh/predictions MODEL_PATH 
[--batch-size BATCH_SIZE]
[--runner {DirectRunner,DataflowRunner}]
[--compression_type COMPRESSION_TYPE]


=========================
## kubeflow
=========================
https://github.com/tensorflow/agents/blob/master/tf_agents/colabs/0_intro_rl.ipynb
https://github.com/tensorflow/agents/blob/master/tf_agents/bandits/colabs/bandits_tutorial.ipynb
https://ghe.spotify.net/bandits/seneca
https://ghe.spotify.net/olegs/bandits/blob/master/cb_simulation_delay.ipynb


https://dev.kubeflow-platform.spotify.net/pipeline/#/runs/details/8dd58da3-d8a6-11e9-89ba-42010a8400aa

hades partitions $endpoint | awk '{print $2}' | head -2 | tail -1
hades publish MySandyEndpoint 2019-01-21 gs://bucket/sand/2019-01-21
hades unpublish pdx_mo.Push.os_level_unsub.Train.DefaultFeatures.NeuralNet 2019-08-13 d72e2114-3099-4b3a-8942-c2451f1056f7


Which of these Spotify ML products are you considering, or currently make use of? (Select all that apply.)
Featran (http://go/featran)
Jukebox (http://go/jukebox)
Klio (http://go/klio)
Luigi-GKE (http://go/luigi-gke)
ML Paved Road (http://go/ml-paved-road)
ML Projects in Backstage (http://go/backstage-ml)
Science Box (http://go/science-box)
Spotify Kubeflow (http://go/kubeflow)
Starlord (http://go/starlord)
Spotify-Tensorflow (http://go/sp-tensorflow)
Zoltar (http://go/zoltar)

https://dev.kubeflow-platform.spotify.net/pipeline/#/runs/details/c11c888f-5b11-41d7-83ed-0f1cf9173b7b


virtualenv env -p python3
source env/bin/activate
brew install skaffold
pip install spotify-kubeflow
skf --version

pip install spotify-kubeflow==0.1.4-dev3 --index-url https://pypi.spotify.net/spotify/dev

skf -mc dev run -e "[spotify-kubeflow-tutorial] Print Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.print.pipeline.pipeline
skf -mc dev run -e "[spotify-kubeflow-example] Taxi Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.taxi.pipeline.pipeline

skf -mc dev run -e "[spotify-kubeflow-example] Taxi Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.taxi.pipeline.pipeline
skf -mc dev run -e "[ml-test-kuberflow] Campaigns" -r "test run - 'ACM Engagement' - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.campaigns.pipeline.pipeline --launch-browser

skf -mc dev run -e "[spotify-kubeflow-example] ml-testing Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.mltest.pipeline.pipeline
skf -mc dev run -e "Edison ML Testing" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.taxi.pipeline.pipeline \
-- \
enable_cache=false learning_rate=0.001 optimizer=adam

skf --managed-cluster dev run -e trex-artist-preference 
-r "$USER - artistpreference_dl_model - $(date '+%Y-%m-%dT%H:%M:%S')" pipelines.artist_preference.prod_pipeline \
-- \
enable_cache=false \
learning_rate=0.001 \
optimizer=adam

skf run ml_test_spotify_kubeflow.examples.print.pipeline.pipeline

    run \
    ... \
    pipelines.my_test_pipeline \
    -- \
    10 \
    learning_rate=0.01

skf -mc dev run -e "[ml-test-kuberflow] Campaigns" -r "test run - 'ACM Engagement' - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.campaigns.pipeline2.pipeline --launch-browser


skf -mc dev run -e "[ml-test-kuberflow] Campaigns" -r "test run - 'ACM Engagement' - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.campaigns.pipeline.pipeline --launch-browser

skf run -e "[core-data-dev-sp] ML Training Pipeline" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.campaigns.pipeline.pipeline --launch-browser

skf run -e "[[ML Golden Path Pipeline <your_username>] Artist Preference Pipeline" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_gp_lingh.examples.apm_golden_path.pipeline.pipeline

skf run -e "[spotify-kubeflow-tutorial] Print Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')"  ml_gp_lingh.examples.print.pipeline.pipeline

skf -mc prod run -e "[ML Golden Path Pipeline lingh] Print Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_gp_lingh.examples.print.pipeline.pipeline

skf -mc prod run -e "[ML Golden Path Pipeline lingh] Print Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_gp_lingh.examples.print.pipeline.pipeline -- enable_cache=false  --launch-browser
skf run -e "[ML Golden Path Pipeline lingh] Artist Preference Pipeline" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_gp_lingh.examples.apm_golden_path.pipeline.pipeline -- enable_cache=false
skf -mc dev run -e "[ML Golden Path Pipeline lingh] Taxi Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_gp_lingh.examples.clustering.pipeline.pipeline -- enable_cache=false

skf -mc dev run -e "[ML Golden Path Pipeline lingh] BQ Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_gp_lingh.examples.clustering.pipeline.pipeline -- enable_cache=false

docker run -it -v $(pwd)/../../key.json:/key.json -e GOOGLE_APPLICATION_CREDENTIALS=/key.json gcr.io/paradox-mo/tf-supervised/lingh:latest bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_task_train TrainingJob --date 2019-08-13 --schema-file push.pbtxt --channel Push --label-name os_level_unsub --estimator-type NeuralNet --feature-set BucketizedFeatures --sample-rate 0.50 --max-steps 25000"

SELECT * FROM `sp-fine-ltv.one_off_aggregates.end_content_xt_28d_aggregate_p1_v2` LIMIT 1000

With Dataflow
Without Dataflow

tensorflow
tensorflow extention

gcloud config set project formats-insights

skf -mc dev run -e "[spotify-kubeflow-example] ml-testing Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.mltest.pipeline.pipeline --launch-browser
skf -mc dev run -e "[spotify-kubeflow-example] ml-testing Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" ml_test_spotify_kubeflow.examples.mltesting.pipeline.pipeline --launch-browser


SERVICE_ACCOUNT = "ml-testing-workflow-sa@formats-insights.iam.gserviceaccount.com"

https://ghe.spotify.net/pikachu/ml-bootcamp/blob/master/ml-golden-path-workshop/src/main/python/trainer/feature.py

(env) ➜  lingh-spotify-kubeflow git:(master) gsutil ls -l gs://lingh
                                 gs://lingh/dataflow/
                                 gs://lingh/ml-testing/
                                 gs://lingh/push/

https://ghe.spotify.net/kubeflow-platform/spotify-kubeflow/pull/486

gcloud iam service-accounts get-iam-policy acq-kubeflow-sa@acmacquisition.iam.gserviceaccount.com
kubectl --context=gke_kubeflow-platform_europe-west1-d_kf-dev  get sa/default-editor -n life-acquatic -o yaml
kubectl --context=gke_kubeflow-platform_europe-west1-d_kf-prod  get sa/default-editor -n life-acquatic -o yaml


====================================
## ml-golden-path-workshop
====================================
cookiecutter git@ghe.spotify.net:kubeflow-platform/spotify-kubeflow-cookie.git

➜  Git cookiecutter git@ghe.spotify.net:kubeflow-platform/spotify-kubeflow-cookie.git
You've downloaded /Users/lingh/.cookiecutters/spotify-kubeflow-cookie before. Is it okay to delete and re-download it? [yes]: yes
owner []: lingh
component_id [ml-gp-lingh]:
owner_email [lingh@spotify.com]:
owner_slack [#lingh]:
gcp_project_for_dataflow [ml-sketchbook]:
gcp_service_account_secret_name [ml-paved-road-pipeline-styx]:
gcs_bucket_for_pipeline_output [gs://ml-golden-path-pipeline/output/lingh]:
gcs_bucket_for_dataflow_staging [gs://dataflow-staging-europe-west1-364472652419]:
system [ml-golden-path]:
project_name [ML Golden Path Pipeline lingh]:
description [ML Golden Path Pipeline lingh spotify-kubeflow pipeline]:
python_module_name [ml_gp_lingh]:
Select enable_mypy:
1 - yes
2 - no
Choose from 1, 2 (1, 2) [1]: 1
Initialized empty Git repository in /Users/lingh/Git/ml-gp-lingh/.git/
[master (root-commit) 69d6214] Initial commit
 41 files changed, 1618 insertions(+)
 create mode 100644 .gitignore
 create mode 100644 .pre-commit-config.yaml
 create mode 100644 Makefile
 create mode 100644 build-info.yaml
 create mode 100644 data-endpoints.yaml
 create mode 100644 data-info.yaml
 create mode 100644 dev-requirements.txt
 create mode 100644 docs-requirements.txt
 create mode 100644 docs/README.md
 create mode 100644 mkdocs.yml
 create mode 100644 ml_gp_lingh/__init__.py
 create mode 100644 ml_gp_lingh/examples/__init__.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/__init__.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/data_defaults.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/defaults.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/evaluator.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/features/__init__.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/features/all_features.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/features/basic_features.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/features/endpoints.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/features/spec.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/pipeline.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/preprocessing.py
 create mode 100644 ml_gp_lingh/examples/apm_golden_path/trainer.py
 create mode 100644 ml_gp_lingh/examples/print/__init__.py
 create mode 100644 ml_gp_lingh/examples/print/pipeline.py
 create mode 100644 ml_gp_lingh/examples/taxi/__init__.py
 create mode 100644 ml_gp_lingh/examples/taxi/data_defaults.py
 create mode 100644 ml_gp_lingh/examples/taxi/defaults.py
 create mode 100644 ml_gp_lingh/examples/taxi/evaluator.py
 create mode 100644 ml_gp_lingh/examples/taxi/pipeline.py
 create mode 100644 ml_gp_lingh/examples/taxi/preprocessing.py
 create mode 100644 ml_gp_lingh/examples/taxi/trainer.py
 create mode 100644 mypy.ini
 create mode 100644 pyproject.toml
 create mode 100644 requirements.txt
 create mode 100755 scripts/productionize.py
 create mode 100644 test-requirements.txt
 create mode 100644 tests/__init__.py
 create mode 100644 tests/main.py
 create mode 100644 tox.ini

 skf -mc prod run \
    -e "[ML Golden Path Pipeline Ling] Print Example" \
    -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" \
    ml_gp_lingh.examples.print.pipeline.

skf run \
    -e "[[ML Golden Path Pipeline lingh] Artist Preference Pipeline" \
    -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" \
    ml_gp_lingh.examples.apm_golden_path.pipeline.pipeline    

artist_features = skf.FeatureCollector.op(
       pipeline_options,
       custom_name="artist-feature-collector",
       inputs=features.basic_features.ARTIST_FEATURE_SPEC.resolved_inputs(pipeline_options),
       config=features.basic_features.ARTIST_FEATURE_SPEC.config,
   )   

   user_features = skf.FeatureCollector.op(
       pipeline_options,
       custom_name="user-feature-collector",
       inputs=features.basic_features.USER_FEATURE_SPEC.resolved_inputs(pipeline_options),
       config=features.basic_features.USER_FEATURE_SPEC.config,
   )

=========================
## create campaign id
=========================
- define template id and opt out type
echo '{ "name": "2019q4_acm_lifecycle_new_album_release_notification_all_album_types_ladron", "push_template_id": 576, "optout_type": "notify-recommended-music", "platforms": [ "ios", "android" ], "active": true, "delivery_date": "2019-12-06T17:00:00", "metadata": { "business_metric": "Engagement - MAU", "business_owner": "Growth Opportunities", "type": "Lifecycle Campaigns", "business_purpose": "New Release, broad" } }' | jhurl --user-header "SSO-User=lingh" --user-header "SSO-Groups=pusheditors" --site services.guc3 -X POST hm://nudge/messenger/campaigns
echo '{ "name": "2019q4_acm_lifecycle_rap_caviar_recommendation_ladron_top_track", "push_template_id": 543, "optout_type": "notify-recommended-music", "platforms": [ "ios", "android" ], "active": true, "delivery_date": "2019-11-06T17:00:00", "metadata": { "business_metric": "Engagement - MAU", "business_owner": "Growth Opportunities", "type": "Lifecycle Campaigns", "business_purpose": "Rap Caviar Recommendation, broad" } }' | jhurl --user-header "SSO-User=nataniaw" --user-header "SSO-Groups=pusheditors" --site services.guc3 -X POST hm://nudge/messenger/campaigns

-- double check the message setup
Get a Push Message 

jhurl --user-header "SSO-User=lingh" --user-header "SSO-Groups=pusheditors" --site services.guc3 hm://nudge/messenger/campaigns/4170125d-ae5f-4a4b-9d32-8792fd0f6d9e

Get messager/campaigns

jhurl --user-header "SSO-User=lingh" --user-header "SSO-Groups=pusheditors" --site services.guc3 hm://nudge/messenger/campaigns

➜  push-common-tasks git:(master) jhurl --user-header "SSO-User=lingh" --user-header "SSO-Groups=pusheditors" --site services.guc3 hm://nudge/messenger/campaigns
Reply UUID:      05999707c31257-49da2f-d61c-0001-630a0200
Request UUID:    05999706c07cc9-615d8f-e958-099c-00000000
Duration:        16691ms
Status:          200 OK
Content-Type:  application/json
Content-Length:  56320

"optout_type":"notify-recommended-music"
"name":"2019q3_acm_lifecycle_hanzo_dryrun_testusers","optout_type":"notify-news-and-offers"

➜  engagement-bq-runner git:(viva_latino) ✗ echo '{ "name": "2019q4_acm_lifecycle_viva_latino_recommendation_ladron_top_track", "push_template_id": 558, "optout_type": "notify-recommended-music", "platforms": [ "ios", "android" ], "active": true, "delivery_date": "2019-11-15T17:00:00", "metadata": { "business_metric": "Engagement - MAU", "business_owner": "Growth Opportunities", "type": "Lifecycle Campaigns", "business_purpose": "Viva Latino Recommendation, broad" } }' | jhurl --user-header "SSO-User=lingh" --user-header "SSO-Groups=pusheditors" --site services.guc3 -X POST hm://nudge/messenger/campaigns

Reply UUID:      05975571a7787f-933691-0953-0001-630a0200
Request UUID:    05975571991b04-cf92f5-7afe-250d-00000000
Duration:        771ms
Status:          201 Created
Content-Type:  application/json
Content-Length:  539

['{"message":"Campaign created","campaign":{"id":"8a372e73-26fa-4e68-8f91-24a07e2bb8fb","name":"2019q4_acm_lifecycle_viva_latino_recommendation_ladron_top_track","optout_type":"notify-recommended-music","active":true,"metadata":{"business_metric":"Engagement - MAU","business_owner":"Growth Opportunities","business_purpose":"Viva Latino Recommendation, broad","type":"Lifecycle Campaigns"},"created_by":"lingh","created_date":"2019-11-14T21:48:38.034174Z","modified_by":"lingh","modified_date":"2019-11-14T21:48:38.034174Z"},"success":true}']

 ➜  campaign-runner git:(viva-latino) ✗ echo '{ "name": "2019q4_acm_lifecycle_viva_latino_personalized_ladron_top_track", "push_template_id": 558, "optout_type": "notify-recommended-music", "platforms": [ "ios", "android" ], "active": true, "delivery_date": "2019-11-15T17:00:00", "metadata": { "business_metric": "Engagement - MAU", "business_owner": "Growth Opportunities", "type": "Lifecycle Campaigns", "business_purpose": "Viva Latino Recommendation, broad" } }' | jhurl --user-header "SSO-User=lingh" --user-header "SSO-Groups=pusheditors" --site services.guc3 -X POST hm://nudge/messenger/campaigns
Reply UUID:      05975648e1cdda-4042bf-0cc0-0001-630a0200
Request UUID:    05975648d6c433-f1724b-ce78-2ade-00000000
Duration:        456ms
Status:          201 Created
Content-Type:  application/json
Content-Length:  537

➜  engagement-bq-runner git:(new_release_query) ✗ echo '{ "name": "2019q4_acm_lifecycle_new_album_release_notification_all_album_types_ladron", "push_template_id": 576, "optout_type": "notify-recommended-music", "platforms": [ "ios", "android" ], "active": true, "delivery_date": "2019-12-06T17:00:00", "metadata": { "business_metric": "Engagement - MAU", "business_owner": "Growth Opportunities", "type": "Lifecycle Campaigns", "business_purpose": "New Release, broad" } }' | jhurl --user-header "SSO-User=lingh" --user-header "SSO-Groups=pusheditors" --site services.guc3 -X POST hm://nudge/messenger/campaigns

Reply UUID:      0598d45d8bf90f-f4e1b8-6638-0001-630a0200
Request UUID:    0598d45d799049-10ee61-25cf-2496-00000000
Duration:        699ms
Status:          201 Created
Content-Type:  application/json
Content-Length:  534

['{"message":"Campaign created","campaign":{"id":"4170125d-ae5f-4a4b-9d32-8792fd0f6d9e","name":"2019q4_acm_lifecycle_new_album_release_notification_all_album_types_ladron","optout_type":"notify-recommended-music","active":true,"metadata":{"business_metric":"Engagement - MAU","business_owner":"Growth Opportunities","business_purpose":"New Release, broad","type":"Lifecycle Campaigns"},"created_by":"lingh","created_date":"2019-12-03T22:39:13.126285Z","modified_by":"lingh","modified_date":"2019-12-03T22:39:13.126285Z"},"success":true}']

➜  Git jhurl --user-header "SSO-User=lingh" --user-header "SSO-Groups=pusheditors" --site services.guc3 hm://nudge/messenger/campaigns/4170125d-ae5f-4a4b-9d32-8792fd0f6d9e

Reply UUID:      0598f63aa404b2-3a9294-b06b-0001-630a0200
Request UUID:    0598f63a9843d1-ce98ee-8816-08c9-00000000
Duration:        614ms
Status:          200 OK
Content-Type:  application/json
Content-Length:  477

['{"id":"4170125d-ae5f-4a4b-9d32-8792fd0f6d9e","name":"2019q4_acm_lifecycle_new_album_release_notification_all_album_types_ladron","optout_type":"notify-recommended-music","active":true,"metadata":{"business_metric":"Engagement - MAU","business_owner":"Growth Opportunities","business_purpose":"New Release, broad","type":"Lifecycle Campaigns"},"created_by":"lingh","created_date":"2019-12-03T22:39:13.126285Z","modified_by":"lingh","modified_date":"2019-12-03T22:39:13.126285" ]


=========================
## BQ Runners
=========================
bq ls -p
bq ls
bq ls lingh_test
bq ls -j
bq ls lingh_test
bq show broad_audience_20191013
bq show lingh_test.broad_audience_20191013
bq query --use_legacy_sql=false 'SELECT * from `paradox-mo.lingh_test.broad_audience_20191013` LIMIT 100'\n

        select 
          if(os_level_unsub.user_id is NULL, 0, 1) as os_level_unsub,
          feature.*
        from 
        (
        SELECT 
            user_id
        FROM `content-marketing-messaging.message_history_daily.message_history_daily_20191125`, 
            unnest(events) as events
        where campaign_id in ('29a11a77-d149-4410-9d4d-e871a31af702') 
        and events.event_type = 'Delivered'
        ) base_users
        left join
        (
        select 
          user_id
        from  `users-protection.push_health.push_health_20191029` 
        where campaign_id in ('29a11a77-d149-4410-9d4d-e871a31af702') 
        and DATE(TIME_SEND) between '2019-10-20' and '2019-10-26'
        and os_level_unsub = 1
        group by 1
        ) os_level_unsub
        on base_users.user_id = os_level_unsub.user_id


LUIGI_LOG_LEVEL=DEBUG bq-runner RunYaml --dry-run --partition 2019-11-13 --target automated-marketing-engagement.campaigns.viva_latino_common_YYYYMMDD
https://docs.google.com/spreadsheets/d/1Lfcpb6nVsq5n8SLHKNW3p1i3XuKlOr0rKRmV-lNaC0Q/edit#gid=818330719

LUIGI_LOG_LEVEL=DEBUG bq-runner RunYaml --dry-run --partition 2019-11-17 --target automated-marketing-engagement.campaigns.rap_caviar_common_YYYYMMDD

SELECT * FROM `gabo-anonym.Push_Messenger_PushMessengerEvent.PushMessengerEvent_20191119` WHERE message.campaign_id = '05975571a7787f-933691-0953-0001-630a0200'

SELECT * FROM `content-marketing-messaging.message_history_daily.message_history_daily_20191119` WHERE campaign_id = "05975648e1cdda-4042bf-0cc0-0001-630a0200"

SELECT * 
FROM `events-anonym-bq.PushMessengerEventV1.PushMessengerEventV1_20191119` 
WHERE message.campaign_id = "05975648e1cdda-4042bf-0cc0-0001-630a0200"
LIMIT 1000

https://ghe.spotify.net/push-messaging/push-messenger/blob/master/docs/data-events.md#example-queries

LUIGI_LOG_LEVEL=DEBUG bq-runner RunYaml --dry-run --partition 2019-11-13 --target automated-marketing-engagement.campaigns.viva_latino_common_YYYYMMDD

awk '{$1=""}1' tmp.txt | awk '{$1=$1}1' > tmp2.txt


=========================
## Campaign Runners
=========================
hades partitions ladron.engagement.CandidateLog.gcs
hades ls ladron.engagement.CandidateLog.gcs 2019-09-17T00:00:00Z
gsutil cat gs://engagement-candidate-log-546122/ladron.engagement.CandidateLog.gcs/2019-09-17/20190917T140148.763349-af5d14cdf3c0/part-00000-of-00001.json | jq .
{
  "campaign-runner.engagement.push.ThisIsPlaylistPersonalized1Campaign": "Skipped_too_old",
  "campaign-runner.engagement.push.TopHitsUSGenericCampaign": "Skipped_too_old",
  "campaign-runner.engagement.push.DiscoverWeeklyUSGenericCampaign": "Skipped_too_old",
  "campaign-runner.engagement.push.ReleaseRadarUSGenericCampaign": "Skipped_too_old",
  "ladron.reactivation.FixedCandidate": "Ok",
  "campaign-runner.engagement.push.ReleaseRadarUSPersonalized1Campaign": "Skipped_too_old"
}

gcloud auth login
gcloud auth list

gcloud config set project automated-marketing-engagement
gcloud config list

Building, Testing, Verfifying and Packaging

docker run --rm -i -w $(pwd) -v $(pwd):$(pwd) gcr.io/action-containers/tox:3.5.2-2 -c tox.ini


gcr.io/automated-marketing-engagement/campaign-runner

sbt clean verify docker
docker images | head -10
sbt pack dockerBuildAndPush

qstyx run -f data-info.yaml -w campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign  -i gcr.io/automated-marketing-engagement/campaign-runner:latest -p 2019-11-21T00:00:00Z

➜  campaign-runner git:(filtering-opt-out) ✗ qstyx run -f data-info.yaml -w campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign  -i gcr.io/automated-marketing-engagement/campaign-runner:latest -p 2019-11-21T00:00:00Z
...
===== Luigi Execution Summary =====

Scheduled 3 tasks of which:
* 2 complete ones were encountered:
    - 1 CheckQueryDependenciesTask(project=automated-marketing-engagement, config_file=resources/campaigns/engagement/push/viva_latino_personalized.yaml, partition=2019-11-21T00:00:00Z)
    - 1 RecordCampaignMetadataTask(config_file=resources/campaigns/engagement/push/viva_latino_personalized.yaml, execution_ts=2019-11-21T000000, start_date=2019-11-21, end_date=2019-11-23, tags=engagement)
* 1 ran successfully:
    - 1 CampaignRunner(...)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 3 tasks of which:
* 2 complete ones were encountered:
    - 1 CheckQueryDependenciesTask(project=automated-marketing-engagement, config_file=resources/campaigns/engagement/push/viva_latino_personalized.yaml, partition=2019-11-21T00:00:00Z)
    - 1 RecordCampaignMetadataTask(config_file=resources/campaigns/engagement/push/viva_latino_personalized.yaml, execution_ts=2019-11-21T000000, start_date=2019-11-21, end_date=2019-11-23, tags=engagement)
* 1 ran successfully:
    - 1 CampaignRunner(...)

This progress looks :) because there were no failed tasks or missing dependencies


runMain com.spotify.data.example.PushLabelJob 
--project=formats-insights 
--date=2020-01-01 
--output=lingh.push_labels_20190630  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1

qstyx run -f data-info.yaml -w campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign  -i gcr.io/automated-marketing-engagement/campaign-runner:lastest -p 2020-01-01T00:00:00
gcr.io/automated-marketing-engagement/campaign-runner:lastest 

ERROR:qstyx: non-zero exit code (125) from `/usr/local/bin/docker run -it -v 
/Users/lingh/Git/campaign-runner/_qstyx:/etc/_qstyx 
-e STYX_COMPONENT_ID=campaign-runner 
-e STYX_WORKFLOW_ID=campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign 
-e STYX_PARAMETER=2020-01-01 
-e STYX_DOCKER_IMAGE=gcr.io/automated-marketing-engagement/campaign-runner:lastest 
-e STYX_DOCKER_ARGS="wrap-luigi --local-scheduler --module campaign_runner CampaignRunner 
--endpoint campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign 
--uri-prefix gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/ 
--config-file engagement/push/viva_latino_personalized.yaml 
--tags engagement 
--styx-date-second 2020-01-01" 
-e STYX_EXECUTION_ID=styx-run-c1e016ec-4437-4825-82fd-7ffa248f0dab -e STYX_TRIGGER_ID=qstyx-25076080-9f81-4fb5-adc7-2ef120024c05 -e STYX_ENVIRONMENT=qstyx -e STYX_LOGGING=text -e GOOGLE_APPLICATION_CREDENTIALS=/etc/_qstyx/gcp-sa-key.json -e STYX_SERVICE_ACCOUNT=campaign-runner@automated-marketing-engagement.iam.gserviceaccount.com gcr.io/automated-marketing-engagement/campaign-runner:lastest wrap-luigi --local-scheduler --module campaign_runner CampaignRunner --endpoint campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign --uri-prefix gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/ --config-file engagement/push/viva_latino_personalized.yaml --tags engagement --styx-date-second 2020-01-01`

hades ls campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign 2019-11-21
gsutil ls gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign/2019-11-21/20200106T181950.851209-39a558f4019d
gsutil cat gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign/2019-11-21/20200106T181950.851209-39a558f4019d/part-00000-of-00001.avro avro-tools tojson - 


qstyx run -f data-info.yaml -w MyJob -p 2017-09-05T00 -r target/docker/image-name
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.OneHotFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-21
qstyx run -f data-info.yaml -w campaign-runner.engagement.push.ReleaseRadarUSGenericCampaign -i gcr.io/automated-marketing-engagement/campaign-runner:lastest 

campaign-runner@automated-marketing-engagement.iam.gserviceaccount.com

git push --set-upstream origin campaigns-meta

awk '{$1=""}1' tmp.txt | awk '{$1=$1}1' > tmp2.txt

Stackdriver run_with_logging

gsutil cat gs://engagement-messages-99df72/ladron.engagement.Messages.gcs/2019-09-27/20190927T140129.896850-4aee1d81511b/part-00000-of-00005.avro avro-tools tojson 

gsutil cat gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-30/20190731T125914.818804-317906c5c985/part-00133-of-00134.avro  avro-tools tojson - 
gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/2019-11-21/

https://backstage.spotify.net/docs/ladron/

https://docs.google.com/spreadsheets/d/1CyuPtGTGstjIIt2Yg_1HSJIHwVxivhOQrtPXEACnMoA/edit#gid=0

gcloud auth login
gcloud auth list

gcloud config set project automated-marketing-engagement
gcloud config list

Building, Testing, Verfifying and Packaging

docker run --rm -i -w $(pwd) -v $(pwd):$(pwd) gcr.io/action-containers/tox:3.5.2-2 -c tox.ini

gcr.io/automated-marketing-engagement/campaign-runner

sbt clean verify docker
docker images | head -10
sbt pack dockerBuildAndPush

qstyx run -f data-info.yaml -w campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign  -i gcr.io/automated-marketing-engagement/campaign-runner:latest -p 2019-11-21T00:00:00Z

➜  campaign-runner git:(filtering-opt-out) ✗ qstyx run -f data-info.yaml -w campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign  -i gcr.io/automated-marketing-engagement/campaign-runner:latest -p 2019-11-21T00:00:00Z
...
===== Luigi Execution Summary =====

Scheduled 3 tasks of which:
* 2 complete ones were encountered:
    - 1 CheckQueryDependenciesTask(project=automated-marketing-engagement, config_file=resources/campaigns/engagement/push/viva_latino_personalized.yaml, partition=2019-11-21T00:00:00Z)
    - 1 RecordCampaignMetadataTask(config_file=resources/campaigns/engagement/push/viva_latino_personalized.yaml, execution_ts=2019-11-21T000000, start_date=2019-11-21, end_date=2019-11-23, tags=engagement)
* 1 ran successfully:
    - 1 CampaignRunner(...)

This progress looks :) because there were no failed tasks or missing dependencies

===== Luigi Execution Summary =====

INFO:luigi-interface:
===== Luigi Execution Summary =====

Scheduled 3 tasks of which:
* 2 complete ones were encountered:
    - 1 CheckQueryDependenciesTask(project=automated-marketing-engagement, config_file=resources/campaigns/engagement/push/viva_latino_personalized.yaml, partition=2019-11-21T00:00:00Z)
    - 1 RecordCampaignMetadataTask(config_file=resources/campaigns/engagement/push/viva_latino_personalized.yaml, execution_ts=2019-11-21T000000, start_date=2019-11-21, end_date=2019-11-23, tags=engagement)
* 1 ran successfully:
    - 1 CampaignRunner(...)

This progress looks :) because there were no failed tasks or missing dependencies


runMain com.spotify.data.example.PushLabelJob 
--project=formats-insights 
--date=2020-01-01 
--output=lingh.push_labels_20190630  --runner=DataflowRunner  --tempLocation=gs://lingh/dataflow/tmp --region=europe-west1

qstyx run -f data-info.yaml -w campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign  -i gcr.io/automated-marketing-engagement/campaign-runner:lastest -p 2020-01-01T00:00:00
gcr.io/automated-marketing-engagement/campaign-runner:lastest 

ERROR:qstyx: non-zero exit code (125) from `/usr/local/bin/docker run -it -v 
/Users/lingh/Git/campaign-runner/_qstyx:/etc/_qstyx 
-e STYX_COMPONENT_ID=campaign-runner 
-e STYX_WORKFLOW_ID=campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign 
-e STYX_PARAMETER=2020-01-01 
-e STYX_DOCKER_IMAGE=gcr.io/automated-marketing-engagement/campaign-runner:lastest 
-e STYX_DOCKER_ARGS="wrap-luigi --local-scheduler --module campaign_runner CampaignRunner 
--endpoint campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign 
--uri-prefix gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/ 
--config-file engagement/push/viva_latino_personalized.yaml 
--tags engagement 
--styx-date-second 2020-01-01" 
-e STYX_EXECUTION_ID=styx-run-c1e016ec-4437-4825-82fd-7ffa248f0dab -e STYX_TRIGGER_ID=qstyx-25076080-9f81-4fb5-adc7-2ef120024c05 -e STYX_ENVIRONMENT=qstyx -e STYX_LOGGING=text -e GOOGLE_APPLICATION_CREDENTIALS=/etc/_qstyx/gcp-sa-key.json -e STYX_SERVICE_ACCOUNT=campaign-runner@automated-marketing-engagement.iam.gserviceaccount.com gcr.io/automated-marketing-engagement/campaign-runner:lastest wrap-luigi --local-scheduler --module campaign_runner CampaignRunner --endpoint campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign --uri-prefix gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/ --config-file engagement/push/viva_latino_personalized.yaml --tags engagement --styx-date-second 2020-01-01`

hades ls campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign 2019-11-21
gsutil ls gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign/2019-11-21/20200106T181950.851209-39a558f4019d
gsutil cat gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign/2019-11-21/20200106T181950.851209-39a558f4019d/part-00000-of-00001.avro avro-tools tojson - 


qstyx run -f data-info.yaml -w MyJob -p 2017-09-05T00 -r target/docker/image-name
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.OneHotFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-21
qstyx run -f data-info.yaml -w campaign-runner.engagement.push.ReleaseRadarUSGenericCampaign -i gcr.io/automated-marketing-engagement/campaign-runner:lastest 

campaign-runner@automated-marketing-engagement.iam.gserviceaccount.com

git push --set-upstream origin campaigns-meta

awk '{$1=""}1' tmp.txt | awk '{$1=$1}1' > tmp2.txt

Stackdriver run_with_logging

gsutil cat gs://engagement-messages-99df72/ladron.engagement.Messages.gcs/2019-09-27/20190927T140129.896850-4aee1d81511b/part-00000-of-00005.avro avro-tools tojson 

gsutil cat gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-30/20190731T125914.818804-317906c5c985/part-00133-of-00134.avro  avro-tools tojson - 
gs://ame-campaign-runner-storage-eu/engagement/push/VivaLatinoPersonalized/2019-11-21/

https://backstage.spotify.net/docs/ladron/

https://docs.google.com/spreadsheets/d/1CyuPtGTGstjIIt2Yg_1HSJIHwVxivhOQrtPXEACnMoA/edit#gid=0

=========================
## Data University
=========================
Metadata uri: gs://data-university-common-eu/lab-data/metadata/20190331/part*.avro
TrackPlay uri: gs://data-university-common-eu/lab-data/track_plays_20191201/part*.avro

sbt "data-pipeline-concepts/runMain com.spotify.data.university.p00_prework.DummyJob --project=data-university --tempLocation=gs://data-university-common-eu/staging --runner=DataflowRunner --trackPlays=gs://data-university-common-eu/lab-data/track_plays_20191201/part*.avro --output=gs://data-university-common-eu/output_sept2019/lingh/dummy-job-out"

sbt "data-pipeline-concepts/runMain com.spotify.data.university.p01_pipeline_example.PlayCountPerTrack --project=data-university --tempLocation=gs://data-university-common-eu/staging --runner=DataflowRunner --region=europe-west1 --trackPlays=gs://data-university-common-eu/lab-data/track_plays_20191201/part*.avro --output=gs://data-university-common-eu/output_sept2019/lingh/play-count-per-track-out --n=100 --maxNumWorkers=5"

sbt "data-pipeline-concepts/runMain com.spotify.data.university.p02_first_lab.PopularPlaylists --project=data-university --tempLocation=gs://data-university-common-eu/staging --runner=DataflowRunner --region=europe-west1 --trackPlays=gs://data-university-common-eu/lab-data/track_plays_20191201/part*.avro --output=gs://data-university-common-eu/output_sept2019/lingh/popular-playlists-out"
 
sbt "data-pipeline-concepts/runMain com.spotify.data.university.p03_join_example.PlayCountPerArtist --project=data-university --tempLocation=gs://data-university-common-eu/staging --runner=DataflowRunner --region=europe-west1 --trackPlays=gs://data-university-common-eu/lab-data/track_plays_20191201/part*.avro --meta=gs://data-university-common-eu/lab-data/metadata/part*.avro --output=gs://data-university-common-eu/output_sept2019/lingh/play-count_per-artist-out"

sbt "data-pipeline-concepts/runMain com.spotify.data.university.p04_second_lab.ArtistsInEachCountry --project=data-university --tempLocation=gs://data-university-common-eu/staging --runner=DataflowRunner --region=europe-west1 --trackPlays=gs://data-university-common-eu/lab-data/track_plays_20191201/part*.avro --meta=gs://data-university-common-eu/lab-data/metadata/20190331/part*.avro --output=gs://data-university-common-eu/output_sept2019/lingh/artists-in-each-country-out"

sbt "data-pipeline-concepts/runMain com.spotify.data.university.p05_sort_top_example.TopTracksPerArtist --project=data-university --tempLocation=gs://data-university-common-eu/staging --runner=DataflowRunner --region=europe-west1 --trackPlays=gs://data-university-common-eu/lab-data/track_plays_20191201/part*.avro --meta=gs://data-university-common-eu/lab-data/metadata/20190331/part*.avro --output=gs://data-university-common-eu/output_sept2019/lingh/top-tracks-per-artist-out --n=2"

sbt "data-pipeline-concepts/runMain com.spotify.data.university.p06_third_lab.TopArtistsPerCountry --project=data-university --tempLocation=gs://data-university-common-eu/staging --runner=DataflowRunner --region=europe-west1 --trackPlays=gs://data-university-common-eu/lab-data/track_plays_20191201/part*.avro --meta=gs://data-university-common-eu/lab-data/metadata/part*.avro --output=gs://data-university-common-eu/output_sept2019/lingh/top-artists-per-country-out --n=2"

sbt "data-pipeline-concepts/runMain com.spotify.data.university.p07_fourth_lab.DistinctUsersPerTrack --project=data-university --tempLocation=gs://data-university-common-eu/staging --runner=DataflowRunner --region=europe-west1 --trackPlays=gs://data-university-common-eu/lab-data/track_plays_20191201/part*.avro --output=gs://data-university-common-eu/output_sept2019/lingh/distinct-users-per-track-out"

sbt "data-university-lingh/runMain com.spotify.data.example.DistinctUsersPerTrackJob --project=data-university --runner=DataflowRunner --trackPlays=gs://data-university-common-eu/lab-data/extension/track_plays/employees_20190901/part-*.avro --region=europe-west1 --tempLocation=gs://data-university-common-eu/lingh/temp --output=gs://data-university-common-eu/lingh/output/distinct_users"

https://ghe.spotify.net/hanzo/hanzo-loader


docker run --rm -it --workdir "$(pwd)" --mount "type=bind,source=$(pwd),target=$(pwd)" gcr.io/action-containers/validate-sysmodel:0.0.10 data-info.yaml
gsutil cp gs://data-university-key/master/google-application-credentials.json gs://data-university-key/lingh/data-university-lingh/google-application-credentials.json

return {'trackPlays': LookupHourly('track_plays_data_university', self.when)}

gsutil cp gs://data-university-key/master/google-application-credentials.json /tmp/google-application-credentials.json
gcloud auth activate-service-account --key-file=/tmp/google-application-credentials.json
styx ls -c data-university-lingh
styx workflow ls | grep data-university-lingh
styx trigger data-university-lingh data-university-lingh.DistinctUsersPerTrackJob '2019-09-01T00'
styx ls -c data-university-lingh
hades partition data-university-lignh.DistinctUsersPerTrackJob
hades ls data-university-lignh.DistinctUsersPerTrackJob 2019-09-01T00

curl https://styx.spotify.net/api/v3/workflows/data-university-lingh | jq 

metrics-catalog-viz.spotify.net

record_id to assign the context 
training 
evaluation

➜  Git hades ls campaign-runner.engagement.push.NewReleaseCampaign 2019-12-16
REVISION_ID                            CREATION_TIME               EXPIRATION_TIME             URI
b920452f-a083-4ac7-a0a2-bcf1e1dc11f8   2019-12-16T06:09:02.142Z    2020-03-15T00:00:00Z        gs://ame-campaign-runner-storage-eu/engagement/push/NewReleaseCampaign/campaign-runner.engagement.push.NewReleaseCampaign/2019-12-16/20191216T060227.636518-1dab2bcdb66f
CURSOR
CsABErkBagtlfmhhZGVzLXhwbnKZAQsSDERhdGFFbmRwb2ludCIyY2FtcGFpZ24tcnVubmVyLmVuZ2FnZW1lbnQucHVzaC5OZXdSZWxlYXNlQ2FtcGFpZ24MCxIJUGFydGl0aW9uIhQyMDE5LTEyLTE2VDAwOjAwOjAwWgwLEghSZXZpc2lvbiIkYjkyMDQ1MmYtYTA4My00YWM3LWEwYTItYmNmMWUxZGMxMWY4DKIBDWhhZGVzLXNlcnZpY2UYACAA

gsutil cat gs://reactivation-messages-ee32ec/ladron.reactivation.Messages/2019-07-01/20190703T083646.616142-647911e178ed/part-00000-of-00027.avro  jq .

gsutil cat gs://ame-campaign-runner-storage-eu/engagement/push/NewReleaseCampaign/campaign-runner.engagement.push.NewReleaseCampaign/2019-12-16/20191216T060227.636518-1dab2bcdb66f/part-00000-of-00003.avro | avro-tools tojson | head -n 1

➜  campaign-runner git:(master) ✗ hades ls campaign-runner.engagement.push.NewReleaseCampaign 2019-12-16
REVISION_ID                            CREATION_TIME               EXPIRATION_TIME             URI
b920452f-a083-4ac7-a0a2-bcf1e1dc11f8   2019-12-16T06:09:02.142Z    2020-03-15T00:00:00Z        gs://ame-campaign-runner-storage-eu/engagement/push/NewReleaseCampaign/campaign-runner.engagement.push.NewReleaseCampaign/2019-12-16/20191216T060227.636518-1dab2bcdb66f
CURSOR
CsABErkBagtlfmhhZGVzLXhwbnKZAQsSDERhdGFFbmRwb2ludCIyY2FtcGFpZ24tcnVubmVyLmVuZ2FnZW1lbnQucHVzaC5OZXdSZWxlYXNlQ2FtcGFpZ24MCxIJUGFydGl0aW9uIhQyMDE5LTEyLTE2VDAwOjAwOjAwWgwLEghSZXZpc2lvbiIkYjkyMDQ1MmYtYTA4My00YWM3LWEwYTItYmNmMWUxZGMxMWY4DKIBDWhhZGVzLXNlcnZpY2UYACAA

➜  campaign-runner git:(master) ✗ gsutil cat gs://ame-campaign-runner-storage-eu/engagement/push/NewReleaseCampaign/campaign-runner.engagement.push.NewReleaseCampaign/2019-12-16/20191216T060227.636518-1dab2bcdb66f/part-00000-of-00003.avro | avro-tools tojson - | less
{"record_id":"a42bc5b3c4ef40e8a2db45d7e33ebadd#4170125d-ae5f-4a4b-9d32-8792fd0f6d9e#576#2019-12-16","user_id":"¤+Å³Äï@è¢ÛE×ã>ºÝ","channel":"Push","campaign_id":"4170125d-ae5f-4a4b-9d32-8792fd0f6d9e","template_id":"576","template_values":{"artist_1":{"resolved_value":{"com.spotify.ladron.avro.candidate.ResolvedValues":{"resolver":"hanzo/v4/artist","uri":"spotify:artist:3rRWzsERkCNBl27Nih029a"}},"simple_value":null},"userId":{"resolved_value":null,"simple_value":{"string":"a42bc5b3c4ef40e8a2db45d7e33ebadd"}},"album_1":{"resolved_value":{"com.spotify.ladron.avro.candidate.ResolvedValues":{"resolver":"hanzo/v4/album","uri":"spotify:album:0o02jBxOMnixZLpeHFnAa6"}},"simple_value":null}},"dry_run":false,"send_at_date_time":null,"send_at_time":{"string":"17:00:00"}}
{"record_id":"8d707e18f8404ad1b3e5cecd3a08ac91#4170125d-ae5f-4a4b-9d32-8792fd0f6d9e#576#2019-12-16","user_id":"<U+008D>p~\u0018ø@JÑ³åÎÍ:\b¬<U+0091>","channel":"Push","campaign_id":"4170125d-ae5f-4a4b-9d32-8792fd0f6d9e","template_id":"576","template_values":{"artist_1":{"resolved_value":{"com.spotify.ladron.avro.candidate.ResolvedValues":{"resolver":"hanzo/v4/artist","uri":"spotify:artist:5uCXJWo3WoXgqv3T1RlAbh"}},"simple_value":null},"userId":{"resolved_value":null,"simple_value":{"string":"8d707e18f8404ad1b3e5cecd3a08ac91"}},"album_1":{"resolved_value":{"com.spotify.ladron.avro.candidate.ResolvedValues":{"resolver":"hanzo/v4/album","uri":"spotify:album:4w6kxufTNwUAHZV8zCRoYm"}},"simple_value":null}},"dry_run":false,"send_at_date_time":null,"send_at_time":{"string":"17:00:00"}}
{"record_id":"3ef94846b98d4a4b8307910af0173c7e#4170125d-ae5f-4a4b-9d32-8792fd0f6d9e#576#2019-12-16","user_id":">ùHF¹<U+008D>JK<U+0083>\u0007<U+0091>\nð\u0017<~","channel":"Push","campaign_id":"4170125d-ae5f-4a4b-9d32-8792fd0f6d9e","template_id":"576","template_values":{"artist_1":{"resolved_value":{"com.spotify.ladron.avro.candidate.ResolvedValues":{"resolver":"hanzo/v4/artist","uri":"spotify:artist:2qDIR2WlcW3llkGqJWg9VJ"}},"simple_value":null},"userId":{"resolved_value":null,"simple_value":{"string":"3ef94846b98d4a4b8307910af0173c7e"}},"album_1":{"resolved_value":{"com.spotify.ladron.avro.candidate.ResolvedValues":{"resolver":"hanzo/v4/album","uri":"spotify:album:19MlQDyvGCFXcSaoPLJCAi"}},"simple_value":null}},"dry_run":false,"send_at_date_time":null,"send_at_time":{"string":"17:00:00"}}
{"record_id":"ff3b5e18b6624e249f831f54dfed779d#4170125d-ae5f-4a4b-9d32-8792fd0f6d9e#576#2019-12-16","user_id":"ÿ;^\u0018¶bN$<U+009F><U+0083>\u001FTßíw<U+009D>","channel":"Push","campaign_id":"4170125d-ae5f-4a4b-9d32-8792fd0f6d9e","template_id":"576","template_values":{"artist_1":{"resolved_value":{"com.spotify.ladron.avro.candidate.ResolvedValues":{"resolver":"hanzo/v4/artist","uri":"spotify:artist:2L0nCuTUHFPHC3Y8uqbUKw"}},"simple_value":null},"userId":{"resolved_value":null,"simple_value":{"string":"ff3b5e18b6624e249f831f54dfed779d"}},"album_1":{"resolved_value":{"com.spotify.ladron.avro.candidate.ResolvedValues":{"resolver":"hanzo/v4/album","uri":"spotify:album:1FzhLun9l0Z3gSrk00nUhQ"}},"simple_value":null}},"dry_run":false,"send_at_date_time":null,"send_at_time":{"string":"17:00:00"}}


//   run with (line breaks need to be removed):
//   $ sbt
project di-golden-path-pipeline-lingh
runMain com.spotify.data.example.BrownieRecsJob --input=gs://scio-playground/sample/data/di.golden.path.EndContentFactXT2/2018-10-31/20181031T075406.854146-12dc225803c3/*.avro --project=scio-playground --runner=DataflowRunner --region=europe-west1 --tempLocation=gs://lingh/dataflow/tmp --output=gs://lingh/di_golden_path/output/end_content_fact_example --topN=10  --metricsLocation=gs://lingh/di_golden_path/output/_metrics

SELECT * 
FROM `events-anonym-bq.PushMessengerEventV1.PushMessengerEventV1_20191119` 
WHERE message.campaign_id = "05975648e1cdda-4042bf-0cc0-0001-630a0200"
LIMIT 1000

https://ghe.spotify.net/push-messaging/push-messenger/blob/master/docs/data-events.md#example-queries

git branch | grep -v "master" | xargs git branch -D

https://spotify.github.io/scio/dev/Style-Guide.html

https://spotify.github.io/scio/examples/index.html

# for all tests
$ sbt test

# for one module of tests
$ sbt testOnly -z com.spotify.ladron.metrics.operational.MessageDeliveryStatusJobTest

# for one specific test
$ sbt testOnly -t com.spotify.mypackage.MyClassTest

sbt testOnly -t com.spotify.ladron.metrics.operational.MessageDeliveryStatusJobTest


============================
Ranking case-study Instagram
============================
user features 
item features
dot product of user feature and item feature
idex creation
tensorflow
google open source - hash function linear weighting for positive and negative features
word2vec on playlist

=========================
## Message-delivery-view
=========================
  its superset + clickhouse + custom SQL compiler which translate to beam jobs
In order to assess health of campaigns soon after rollout dashboard with smaller than weekly latency is needed.

We produce weekly denormalized dataset: (user_id, message_id, is_open, is_click)

One record corresponds to one message, because majority of clicking actions (especially in case of email) happens in 7 days and Clickhouse is immutable we have to wait a week until data becomes available.

Therefore the state of the event has to be recomputed as new data comes, instead of producing denormalized view from the pipelines. There are multiple design choices:
1. Append events (same structure as now) as they come, group per message_id, compute noop = total - clicked - opened
2. Split events by type in separate tables, join by message id, then noop is just count

Assess performance, window size, and possible caching techniques based on the event count (activation campaigns reach ~20m users X num of messages)

Refs:
https://ghe.spotify.net/white-mouse/message-delivery-view
https://ghe.spotify.net/metrics-catalog/metrics-catalog/tree/master/components/content-marketing-messaging
https://backstage.spotify.net/docs/metrics-catalog/
https://clickhouse.yandex/docs/en/operations/table_engines/
https://spotify.slack.com/archives/CA10ARMS4/p1572362297018900

==============================
## Q1 Ladron Improvement ideas
==============================
- Monitor operational metrics to create healthy campaign delivery
  - Anormaly detection for different failure reasons (capped, upstreamed failure, upreachable, ...)
- Evaluate optimization pipeline for engagement campaigns
  - Random policy: Treatment vs Control
  - EGreedy, AnnealingEpsilonGreedyPolicy, AnnealingSoftmaxPolices, SoftmaxPolicy
  - Contextual evaluations
- Investigate features for content messages
  - Message Feature Store
  - User Feature Store

Changes with introduction of selection id
We selected this version because we prefer an explicit selection id to the implicit (channel, date) id even if it requires more work.

Selection id is generated by the ladron job in step one (as user_id+module+channel+date), can be supplied by candidates in the future if needed (module is added to allow future simplifications when/if we add module as a field in the Candidate so that we do not have to know of all candidate campaigns in Ladron).

Changes marked in bold

MessageHistoryView: user_id, record_id
UserSelectionInfo: user_id, country
UserMessageInteraction: user_id, selection_id, channel, campaign_id, policy, "reward"
UserAssignment: user_id, policy
SelectionInfo: user_id, record_id, selection_id, score, explore
MatchedCampaigns: user_id, selection_id, [channel, campaign_id] Can produce multiple per day if more than one channel is provided
SelectionContext [tfrecord] (SC): user_id, selection_id, any context fields provided by model pipeline (logged as part of the LadronJob).
EvaluationContext [tfrecord] (EC): user_id, selection_id, optional reward, optional score, any context fields provided by model pipeline (selection context joined with reward for evaluation, contains examples without rewards that were not selected).
TrainingContext [tfrecord] (TC): User_id, selection_id, reward, score, any context fields provided by model pipeline (evaluation context filtered for selected messages).

NB Both evaluation context and training context could/should be pre-filtered to only contain random data (with probabilistic models we could either create a separate dataset or consider those random, not sure what is more correct).

Dataset usages
Candidates + CandidateContext + UserSelectionInfo -> LadronJob -> Messages + UserAssignment + SelectionInfo + MatchedCampaigns + SelectionContext
MessageHistoryView + SelectionInfo + UserAssignment -> UserMessageInteraction
UserAssignment + SelectionInfo + SelectionContext -> EvaluationContextJob -> EvaluationContext
EvaluationContext -> TrainingContextJob -> TrainingContext
UserMessageInteraction + UserSelectionInfo + SelectionInfo -> AgentTraining -> MAB JSON
TrainingContext -> ContextualTraining -> CTX TF
EvaluationContext + UserSelectionInfo (+ MatchedCampaigns) + MAB JSON + CTX TF -> EvaluationJob -> Evaluation CSV

Minimal datasets changes alternative
UserSelectionInfo: user_id, country
UserMessageInteraction: user_id, channel, campaign_id, policy, "reward"
UserAssignment: user_id, policy (could include channel to simplify the join in the evaluation pipeline, but then we would have to change existing pipelines to handle possible duplicates)
SelectionInfo: user_id, channel, score, explore
MatchedCampaigns: user_id, [channel, campaign_id]
SelectionContext [tfrecord] (SC): user_id, channel, any context fields provided by model pipeline (logged as part of the LadronJob).
EvaluationContext [tfrecord] (EC): user_id, channel, optional reward, optional score, any context fields provided by model pipeline (selection context joined with reward for evaluation, contains examples without rewards that were not selected).
TrainingContext [tfrecord] (TC): User_id, channel, reward, score, any context fields provided by model pipeline (evaluation context filtered for selected messages).
NB Both evaluation context and training context could/should be pre-filtered to only contain random data (with probabilistic models we could either create a separate dataset or consider those random, not sure what is more correct).

Dataset usages
UserAssignment + SelectionInfo + SelectionContext -> EvaluationContextJob -> EvaluationContext
EvaluationContext -> TrainingContextJob -> TrainingContext
UserMessageInteraction + UserSelectionInfo + SelectionInfo -> AgentTraining -> MAB JSON
TrainingContext -> ContextualTraining -> CTX TF
EvaluationContext + UserSelectionInfo (+ MatchedCampaign) + MAB JSON + CTX TF -> EvaluationJob -> Evaluation CSV


https://ghe.spotify.net/mmarchini/BelcantoRL/blob/bf771fecc48b4f17fe610baebb16860d056a6483/src/belcanto_rl/train_eval.py

git reset --hard 67a65725c426ae9968af9886bdcc041e51d5a10e

hades ls ladron.metrics.UserMessageInteraction 2020-03-08T00:00:00Z
gsutil ls gs://user-message-interaction-aggregation-c1c2a5/ladron.metrics.UserMessageInteraction/2020-03-08/20200309T051638.824181-686617cb87bb
spawk 'print user_id, module, randomly_selected, campaign_id, message_id, selection_id, channel, is_open, is_click, is_reject, is_no_action nr=10' gs://user-message-interaction-aggregation-c1c2a5/ladron.metrics.UserMessageInteraction/2020-03-08/
spawk 'print user_id, days_since_active, days_since_active_on_mobile, days_since_registration, locale, ok_to_email, ok_to_push, registration_country,reporting_product nr=10' gs://user-selection-info-3b8204/ladron.candidate.UserSelectionInfo/2020-03-08/
spawk 'print user_id, age, birth_date, gender, is_new_registered, product_period_id, registration_country, registration_date, registration_funnel, reporting_age_bucket, reporting_product, status, user_snapshot_errors nr=10' gs://bcd-usersnapshot4/bcd.UserSnapshot4/2020-03-08/

qstyx run -f data-info.yaml -w campaign-runner.engagement.push.VivaLatinoPersonalizedCampaign  -i gcr.io/automated-marketing-engagement/campaign-runner:latest -p 2019-11-21T00:00:00Z


https://ghe.spotify.net/white-mouse/ladron/blob/master/ladron-schemas/src/main/scala/com/spotify/ladron/candidate/CandidateMessage.scala

=============================
## Ladron counters
=============================
https://ghe.spotify.net/white-mouse/ladron/blob/master/ladron-pipelines/src/main/scala/com/spotify/ladron/delivery/InAppDeliveryJob.scala

https://ghe.spotify.net/white-mouse/ladron/pull/532

https://ghe.spotify.net/white-mouse/ladron/pull/542
qstyx run -f data-info.yaml -w ladron.activation.Delivery -p 2020-01-24T09 -r ladron-pipelines/target/image-name -- wrap-luigi --local-scheduler --module=ladron.delivery.job ActivationDeliveryJob --test-id=george --datehour 2020-01-24T09

=========================
## Message-ranking
=========================
# Use a high enough Python version
$ pyenv local 3.7.1

# Install pipx
$ python -m pip install pipx
$ python -m pipx ensurepath

# Install tox test runner, black code formatter, flake8 code format checker globally
$ pipx install tox
$ pipx install black
$ pipx install flake8

# Upgrade pipx later on, if needed
$ python -m pip install -U pipx


gcloud auth login
gcloud auth list

gcloud config set project automated-marketing-engagement
gcloud config list

PROJECT = 'content-marketing-messaging'
REGION = 'europe-west1'
SERVICE_ACCOUNT = 'message-ranking-runner@content-marketing-messaging.iam.gserviceaccount.com'
DEVELOPMENT_BUCKET = 'message-ranking-dev'

sbt "message-ranking/runMain \
com.spotify.features.EngagementCandidateContextJob \
--project=content-marketing-messaging \
--runner=DataflowRunner \
--region=europe-west1 \
--tempLocation=gs://andrewk-development/dataflow/tmp \
--userSelectionInfo=gs://user-selection-info-3b8204/ladron.candidate.UserSelectionInfo/2020-03-18/20200319T055906.075538-b2a4cb93756e/part-*.avro \
--userSnapshot=gs://bcd-usersnapshot4/bcd.UserSnapshot4/2020-03-11/styx-run-f770c563-12e6-4089-919c-442f6d287cb2/part-*.avro \
--endContentAggregate=business-critical-data:end_content_30d_aggregate_v2.end_content_30d_aggregate_v2_20200318 \
--output=gs://andrewk-development/dataflow/output \
"
    user_id: String,
    channel: String,
    campaign_id: String,
    country: String,
    age: Int,
    gender: String,
    days_since_registration: Int,
    reporting_product: String,
    days_since_active: Int,
    ms_played_sum: Long,
    stream_count: Long

 ✘ lingh@Lings-MacBook-Pro  ~/Git/ML/message-ranking   change-engagement-interaction ●  git stash
Saved working directory and index state WIP on change-engagement-interaction: 7c2b90c Merge pull request #51 from white-mouse/engagement-model-schedule


-- Run qstyx to run jobs
qstyx run -f data-info.yaml -w <id.name> -i <docker_image> --append-commands -- --model-suffix=<your_suffix>

qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-12
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.OneHotFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-21

--> qstyx Errors
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-12
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-12


qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.LinearClassifier.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-1
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.LinearClassifier.OneHotFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-21
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.DefaultFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-11
qstyx run -f data-info.yaml -w pdx_mo.push_unsub.Train.NeuralNet.OneHotFeatures -i gcr.io/paradox-mo/tf-supervised/lingh:latest -p 2019-08-21

qstyx run -f data-info.yaml -w message-ranking.trainer.engagement.CandidateContext.gcs -i gcr.io/content-marketing-messaging/message-ranking:20200325T165352-b017cf2 -p 2020-03-11


https://ghe.spotify.net/white-mouse/message-ranking/pull/35/files

change-engagement-dataset

PULL LATEST MASTER
git checkout master
git pull master

CREATE A HOTFIX BRANCH
git checkout -b hotfix/rollback

REVERT THE CHANGE THAT CAUSED THE ISSUE
git revert -m 1 HEAD^1
(Close VIM by ":wq" )

..or...
Alternative use the GUI and right click the latest commit on master (the merge) and choose "Revert"

MERGE THE HOTFIX BRANCH TO MASTER
git checkout master
git merge --no-ff hotfix/rollback
PUSH THE MASTER BRANCH
git push

https://spotify.stackenterprise.co/questions/450/i-no-longer-have-access-to-our-docker-images-in-gcr-io


## Ladron -> Message Ranking
   _.transform("LogCandidateContexts")(logContexts(logSide, args.candidateContextOutput))


  Ladron.luigi.engagement.job.py
  class EngagementJob(LadronJobDailyBaseTask):
              'candidateContextOutput': LadronHadesTarget(
                endpoint=constants.ENGAGEMENT_MATCHED_CONTEXT_ENDPOINT_NAME,
                partition=self.get_partition_str(),
                uri_prefix=self._make_uri_prefix('EngagementMatchedCandidateContext')),
                'agents': LookupLatest(endpoint=constants.ENGAGEMENT_MAB_AGENTS_ENDPOINT_NAME),


  def output(self):
      return {
          'candidateOutput': LadronHadesTarget(
              endpoint=constants.ENGAGEMENT_MATCHED_CAMPAIGNS_ENDPOINT_NAME,
              partition=self.get_partition_str(),
              uri_prefix=self._make_uri_prefix('EngagementMatchedCampaigns')),
          'candidateContextOutput': LadronHadesTarget(
              endpoint=constants.ENGAGEMENT_MATCHED_CONTEXT_ENDPOINT_NAME,
              partition=self.get_partition_str(),
              uri_prefix=self._make_uri_prefix('EngagementMatchedCandidateContext')),
          'candidateLogOutput': LadronHadesTarget(
              endpoint=constants.ENGAGEMENT_CANDIDATE_LOG_ENDPOINT_NAME,
              partition=self.get_partition_str(),
              uri_prefix=self._make_uri_prefix('EngagementCandidateLog')),
          'assignmentOutput': LadronHadesTarget(
              endpoint=constants.ENGAGEMENT_USER_ASSIGNMENT_TEMPLATES_ENDPOINT_NAME,
              partition=self.get_partition_str(),
              uri_prefix=self._make_uri_prefix('EngagementUserAssignment')),
          'selectionInfoOutput': LadronHadesTarget(
              endpoint=constants.ENGAGEMENT_SELECTION_INFO_ENDPOINT_NAME,
              partition=self.get_partition_str(),
              uri_prefix=self._make_uri_prefix('EngagementSelectionInfo')),
          'messageOutput': LadronHadesTarget(
              endpoint=constants.ENGAGEMENT_MESSAGES_ENDPOINT_NAME,
              partition=self.get_partition_str(),
              uri_prefix=self._make_uri_prefix('EngagementMessages'))
      }

  ENGAGEMENT_MATCHED_CONTEXT_ENDPOINT_NAME = 'ladron.engagement.MatchedCandidateContext.gcs'
  ENGAGEMENT_USER_ASSIGNMENT_TEMPLATES_ENDPOINT_NAME = 'ladron.engagement.UserAssignment.gcs'
  ENGAGEMENT_CANDIDATE_LOG_ENDPOINT_NAME = 'ladron.engagement.CandidateLog.gcs'


  describe("logCandidateContext") {
    it("should log contexts even if they're empty") {
      val selectionId = SelectionId(NonEmptyString.fromAvro("selection-id"))
      val assignmentPolicy =
        AssignmentPolicy(NonEmptyString.fromAvro("do_sth"), PolicyType.Random)
      val candidateMessage = CandidateMessageWithContext(DefaultMessage, Example.getDefaultInstance)
      val asm = AssignedSelectedMessage(
        userId = DefaultUserId,
        selectionId = selectionId,
        candidates = Seq(candidateMessage),
        selectedMessage = candidateMessage.message,
        group = assignmentPolicy,
        score = 0.8,
        explore = false
      )

      LadronJob.contextToExamples(asm).size must be(1)
    }
  }

 spawk 'print user_id nr=10' gs://bcd-usersnapshot4/bcd.UserSnapshot4/2020-03-08/
 ladron.engagement.MatchedCandidateContext.gcs
 spawk 'print userid,selectionId nr=10' gs://engagement-matched-candidate-context-b0f41e/
 ladron.engagement.MatchedCandidateContext.gcs/2020-03-08/20200308T142454.054078-9d34d3c01c3c/
 gs://engagement-matched-candidate-context-b0f41e/ladron.engagement.MatchedCandidateContext.gcs/2020-03-08/20200308T142454.054078-9d34d3c01c3c/part-00000-of-00001.tfrecords

brew link --overwrite python
brew unlink python && brew link python

class EngagementInteractionContextJob(InteractionContextJob):
    module = modules.MODULE_ENGAGEMENT

    def main_class(self):
        return "com.spotify.features.EngagementInteractionContextJob"

    def requires(self):
        # For messages that we sent 6 days ago, we have feedback today
        six_days_ago = self.date - dt.timedelta(days=6)
        # For messages that we sent 6 days ago, we have feedback today, but we used data from 7 days ago.
        # see https://backstage.spotify.net/docs/ladron/FAQ/#what-date-offsets-are-used-in-the-ladron-pipelines
        seven_days_ago = self.date - dt.timedelta(days=7)
        return {
            "userMessageInteraction": hades.LookupDaily(
                "ladron.metrics.UserMessageInteraction", self.date),
            "selectionInfo": luigi_base.CompletedLookupDaily(
                "ladron.engagement.SelectionInfo.gcs", six_days_ago),
            "userSelectionInfo": hades.LookupDaily(
                "ladron.candidate.UserSelectionInfo", seven_days_ago),
            "userSnapshot": hades.LookupDaily("bcd.UserSnapshot4",
                                              seven_days_ago),
            "endContentAggregate": luigi_base.BigQueryDailySnapshot(
                date=seven_days_ago,
                project="business-critical-data",
                dataset="end_content_30d_aggregate_v2",
                table="end_content_30d_aggregate_v2")
        }

One question to both of you - why do we pull in all the features again in InteractionContext (all the UserSnapshots etc.)? We could use MatchedCandidateContext that Ladron logs (it logs all the features of candidates considered for selection, which comes from CandidateContext) and just join in the UserMessageInteraction. In fact we need to do it anyways for the Evaluation job, so we could maybe create a separate dataset for both of them.

https://ghe.spotify.net/white-mouse/ladron/blob/master/ladron-pipelines/src/main/scala/com/spotify/ladron/selection/CandidateContext.scala

ladron.metrics.UserMessageInteraction/2020-03-15/ --> Label
['user_id', 'module', 'randomly_selected', 'campaign_id', 'message_id', 'selection_id', 'channel', 'is_open', 'is_click', 'is_reject', 'is_no_action']

ladron.engagement.SelectionInfo.gcs/2020-03-09 -> Delivery
['explore', 'record_id', 'selection_id', 'user_id']

ladron.candidate.UserSelectionInfo/2020-03-08/ --> Audience 
['user_id', 'days_since_active', 'days_since_active_on_mobile', 'days_since_registration', 'locale', 'ok_to_email', 'ok_to_push', 'registration_country', 'reporting_product']

bcd.UserSnapshot4/2020-03-08/                 --> UserFeature
['user_id', 'age', 'birth_date', 'gender', 'is_new_registered', 'product_period_id', 'registration_country', 'registration_date', 'registration_funnel', 'reporting_age_bucket', 'reporting_product', 'status', 'user_snapshot_errors']

Ladron reads CandidateContext for engagement and then writes it as MatchedCandidateContext for the messages that were considered for selection. Instead of pulling all the features again in InteractionContext, we can use MatchedCandidateContext directly.

use MatchedCandidateContext that Ladron logs (it logs all the features of candidates considered for selection, which comes from CandidateContext) and just join in the UserMessageInteraction. 

https://ghe.spotify.net/white-mouse/ladron/blob/master/ladron-pipelines/src/main/scala/com/spotify/ladron/selection/CandidateContext.scala


=========================
## Zissou Adjust
=========================
sbt test 
sbt testQuick
sbt testOnly com.spotify.features.EngagementCandidateContextJob

runMain com.spotify.data.zissou.adjust.AdjustFbDataJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputBq=acmacquisition:adjust.test5 --date=20200418 --padlockdate=2020041800 --tempLocation=gs://zissou-secrets/dataflow/tmp
runMain com.spotify.data.zissou.adjust.AdjustFbDataJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputBq=acmacquisition:adjust.test11 --date=20200503 --padlockdate=2020050323 --tempLocation=gs://zissou-secrets/dataflow/tmp --dataEndpoint=AdjustFbDataJob --dataPartition=2020-05-03
runMain com.spotify.data.zissou.adjust.AdjustFbDataJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputBq=acmacquisition:adjust.test12 --date=20200504 --padlockdate=2020050423 --tempLocation=gs://zissou-secrets/dataflow/tmp --dataEndpoint=AdjustFbDataJob --dataPartition=2020-05-04
runMain com.spotify.data.zissou.adjust.AdjustFbDataJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputBq=acmacquisition:adjust.test14 --date=20200504 --padlockdate=2020050423 --tempLocation=gs://zissou-secrets/dataflow/tmp --dataEndpoint=AdjustFbDataJob --dataPartition=2020-05-04


gcloud config set project acmacquisition
sbt clean verify docker
docker images

qstyx run -f data-info.yaml -w zissou.AdjustFbDataJob -i gcr.io/acmacquisition/zissou:20200428T191908-1eb05e6.DIRTY -p 2020-04-23
qstyx run -f data-info.yaml -w acmacquisition.zissou.AdjustFbDataJob -i gcr.io/acmacquisition/zissou:20200505T233722-71762b8.DIRTY -p 2020-05-04


https://ghe.spotify.net/tc4d/tc4d-examples
https://docs.google.com/document/d/1UtloJpkVkww9F3TY9Vpy908tOaw7bjPVU6ClTjC9OXs/edit

https://alvinalexander.com/scala/scala-type-aliases-syntax-examples/

bq rm -f "acmacquisition:adjust_fb.adjust_fb_20200501"
for %d in (01 02 03 04 05 06 07 08 09 10 11 12 13) DO bq rm -f acmacquisition:adjust_fb.adjust.test_%d

https://ghe.spotify.net/voice-artist-fulfillment/voice-artist-fulfillment-kfp/blob/master/data-endpoints.yaml

https://ghe.spotify.net/edison/acm-acquisition-analyses/blob/master/notebooks/margot/2020q2_display_phase1.ipynb

runMain com.spotify.data.zissou.adjust.AdjustDcmDataJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputBq=acmacquisition:adjust.test --date=20200506 --tempLocation=gs://zissou-secrets/dataflow/tmp --dataEndpoint=AdjustDcmDataJob --dataPartition=2020-05-06
runMain com.spotify.data.zissou.adjust.AdjustDcmDataJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputBq=acmacquisition:adjust.test1 --date=20200507 --tempLocation=gs://zissou-secrets/dataflow/tmp --dataEndpoint=AdjustDcmDataJob --dataPartition=2020-05-07


https://backstage.spotify.net/data-endpoints/bcd.UserSnapshot4
https://backstage.spotify.net/data-endpoints/bcd.UserAttributionWide


runMain com.spotify.data.BelafonteContentRanker.BelafonteContentRankerJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputbq=acmacquisition:belafonte.test1 --date=2020-05-28
runMain com.spotify.data.BelafonteContentRanker.BelafonteContentRankerJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputbq=acmacquisition:belafonte.test1 --knowledge-graph-table=acmacquisition:content_knowledge_graph.content_knowledge_graph_20200531 --date=2020-05-31 --google-trends-table=google_trends_popularity.google_trends_popularity_20200531 --tempLocation=gs://belafonte/dataflow/tmp --dataEndpoint=BelafonteContentRankerJob --dataPartition=2020-05-31

qstyx run -f data-info.yaml -w acmacquisition.user_retention_adjust.user_retention_adjust_YYYYMMDD -i gcr.io/acmacquisition/zissou:20200528T221440-dfa3081.DIRTY -p 2020-05-27
qstyx run -f data-info.yaml -w acmacquisition.user_retention_adjust.user_retention_adjust_YYYYMMDD -r zissou/target/image-name  -p 2020-05-25
qstyx run -f data-info.yaml -w acmacquisition.user_retention_adjust.user_retention_adjust_YYYYMMDD -r zissou/target/image-name  -p 2020-05-25


qstyx run -f data-info.yaml -w acmacquisition.belafonte_content_ranking.user_retention_prediction_YYYYMMDD -r belafonte-ranker/target/image-name  -p 2020-04-13
qstyx run -f data-info.yaml -w acmacquisition.belafonte_content_ranking.belafonte_content_ranking_YYYYMMDD  -r belafonte-ranker/target/image-name  -p 2020-04-13


qstyx run -f data-info.yaml -w acmacquisition.belafonte_content_ranking.belafonte_content_ranking_YYYYMMDD  -r belafonte-ranker/target/image-name  -p 2020-06-21
qstyx run -f data-info.yaml -w acmacquisition.belafonte_content_ranking.user_retention_prediction_YYYYMMDD -r belafonte-ranker/target/image-name  -p 2020-05-25


curl -v "https://datawhere-proxy.spotify.net/api/retainer/v1/retention?hadesEndpointId=oauth2-acceptance-metrics.tables.oauth2_acceptance_1d&type=HADES"


=========================
## BelafonteContentRanker
=========================

runMain com.spotify.data.BelafonteContentRanker.BelafonteContentRankerJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputbq=acmacquisition:belafonte.test1 --knowledge-graph-table=acmacquisition:content_knowledge_graph.content_knowledge_graph_20200531 --date=2020-05-31 --google-trends-table=google_trends_popularity.google_trends_popularity_20200531 --tempLocation=gs://belafonte/dataflow/tmp --dataEndpoint=BelafonteContentRankerJob --dataPartition=2020-05-31 --output=gs://belafonte/output
runMain com.spotify.data.BelafonteContentRanker.UserRetentionPredictionJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 


PYTHONPATH='belafonte-ranker/src/main/python/' JAR_DIR='belafonte-ranker/target/pack/lib' luigi --module user_retention_prediction UserRetentionPredictionJob --date 2020-06-10 --local-scheduler


# WORKFLOW_NAME is the workflow id defined in data-info.yaml.
# STYX_PARAMETER is usually a partition (e.g. 2016-09-14) identifying the workflow instance.
docker system prune --all --force --volumes
qstyx run -f data-info.yaml -i ${IMAGE_LOCATION} -w acmacquisition.user_retention_prediction -p 2020-06-29 --legacy-auth

PYTHONPATH='belafonte-ranker/src/main/python/' JAR_DIR='belafonte-ranker/target/pack/lib' luigi --module user_retention_prediction UserRetentionPredictionJob --date 2020-06-10 --local-scheduler

export GOOGLE_APPLICATION_CREDENTIALS="~/sa_credential/belafonte-ranker/google-application-credentials.json"
runMain com.spotify.data.BelafonteContentRanker.BelafonteContentRankerJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputbq=acmacquisition:belafonte.test1 --knowledge-graph-table=acmacquisition:content_knowledge_graph.content_knowledge_graph_20200703 --date=2020-07-03 --google-trends-table=google_trends_popularity.google_trends_popularity_20200703 --tempLocation=gs://belafonte/dataflow/tmp --dataEndpoint=BelafonteContentRankerJob --dataPartition=2020-07-03 --output=gs://belafonte/output

styx workflow enable google-trends-datapuller acmacquisition.google-trends-datapuller

select
content_uri,
campaign_name,
sum(spend) as spend,
sum(distinct total_spend_that_day) as total_spend_while_live,
sum(regs) as regs,
min(live_date) as content_live_date,
last_spend as last_spend,
case when max(table_suffix) = '20200703' then True else False end as is_active
from (
select
  content_uri,
  campaign_name,
  spend,
  regs,
  live_date,
  _TABLE_SUFFIX AS table_suffix,
LAST_VALUE(spend)
over (PARTITION BY content_uri, campaign_name ORDER BY _TABLE_SUFFIX asc
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
) as last_spend,
sum(spend) over (partition by _TABLE_SUFFIX, campaign_name
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as total_spend_that_day
from
`acmacquisition.zissou_belafonte_metrics.zissou_belafonte_metrics_*`
where _TABLE_SUFFIX <= '20200703'
)
group by
content_uri,
campaign_name,
last_spend

(
(2020q2_markets_single_performancemarketing_acmbudgetpoolscalingtest_pl+see+see+eur+smartly+20200415+cell2+content+appinstall	
spotify:playlist:37i9dQZF1DXdxcBWuJkbcy)	
KnowledgeGraphNode(spotify:playlist:37i9dQZF1DXdxcBWuJkbcy	2020q2_markets_single_performancemarketing_acmbudgetpoolscalingtest_pl+see+see+eur+smartly+20200415+cell2+content+appinstall	5	[Lcom.spotify.data.BelafonteContentRanker.BelafonteContentRankerJob$Link;@3970c6b8)
)

Deepak Bhat1:22 PM
https://docs.google.com/document/d/1EPFEu9st-7ziFurr3Z_TAimgejqZ3e3c6770eTOubAc/edit#heading=h.ds179ytfnrdh
Chris Tang1:43 PM
https://docs.google.com/document/d/1fbUnPH4jVqExeidvNTGTN6Jl8sBP4y8dBwn1PW5MVXw/edit?ts=5f22f91b

runMain com.spotify.data.BelafonteContentRanker.BelafonteContentRankerJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1 --outputbq=acmacquisition:belafonte.test1 --knowledge-graph-table=acmacquisition:content_knowledge_graph.content_knowledge_graph_20200703 --date=2020-07-03 --google-trends-table=google_trends_popularity.google_trends_popularity_20200703 --tempLocation=gs://belafonte/dataflow/tmp --dataEndpoint=BelafonteContentRankerJob --dataPartition=2020-07-03 --output=gs://belafonte/output

runMain com.spotify.data.BelafonteContentRanker.PublishPredictionsJob --project=acmacquisition --runner=DataflowRunner --region=europe-west1  --date=20200808 --inputGcs=gs://belafonte/output/life-acquatic/Inferrer.Inferrer/inference_result/223152/20200729T204644.557694-2ac5193a76fc/prediction_logs-00000-of-00003.gz  --tempLocation=gs://belafonte/dataflow/tmp  --output=gs://belafonte/sbt-batch-output



=======================================
## User Retention Prediction - Kubeflow
=======================================
define docker images
specify data-info.yaml
build customized docker_image


->> Configure your kubectl to talk to the ml-paved-road-training-eu Kubernetes cluster that we've provided
gcloud auth login
gcloud config list
gcloud config set project acmacquisition

gcloud config set container/cluster ml-paved-road-training-eu
gcloud container clusters get-credentials ml-paved-road-training-eu --zone=europe-west1-b

->> Add credentials/key to the Kubernetes cluster
kubectl create secret generic $(whoami)-paved-road --from-file=key.json=./key.json

sbt pack dockerBuildAndPush
docker run -it \
-v $(pwd)/key.json:/key.json \
-e GOOGLE_APPLICATION_CREDENTIALS=/key.json \
-e SECRET_KEY_NAME=$(whoami)-paved-road \
-e BUILD_ID=$(whoami) \
gcr.io/ml-sketchbook/tf-supervised/$(whoami) \
bash -c "PYTHONPATH=python luigi --local-scheduler --module tasks.luigi_tasks TrainingGKEJob"

https://artifactory.spotify.net/artifactory/api/pypi/pypi/packages/packages/7c/32/a11befbb003e0e6b7e062a77f010dfcec0ec3589be537b02d2eb2ff93b9a/xgboost-1.1.1-py3-none-manylinux2010_x86_64.whl (127.6 MB)
https://azureossd.github.io/2020/01/23/xgboost-library-could-not-be-loaded/index.html

https://ghe.spotify.net/kubeflow-platform/spotify-kubeflow/blob/master/docker/xgboost/Dockerfile.py3
https://ghe.spotify.net/edison/oseary-drakoulias/blob/master/src/main/java/com/spotify/osearydrakoulias/contentrankingretriever/BigQueryContentRankingRetrieverImpl.java#L101


https://ghe.spotify.net/home-assembly/bart-flow/blob/master/src/bartflow/pipeline/pipeline.py#L23

I apologize for the late follow-up, this week was a bit crazy! Luckily amidst the crazy, Hyperkube released a batch inference component that you can leverage in your pipelines.  You can see the code for the Inferrer here.

I also found that we do have XGBoost components that were contributed to SKF. You can find them here.

Lastly, if you do ever want to leverage our help we run workshops for Kubeflow. I do not have specific dates for the upcoming workshop but you can find an overview template with more information on the setup here.

kubectl get sa/default-editor -n life-acquatic -o yaml --context=gke_kubeflow-platform_europe-west1-d_kf-dev
kubectl get sa/default-editor -n life-acquatic -o yaml --context=gke_kubeflow-platform_europe-west1-d_kf-prod
kubectl get pod -n life-acquatic --context gke_kubeflow-platform_europe-west1-d_kf-prod
kubectl logs life-acquatic-belafonte-ranker-hl5zx-1244472212 -n life-acquatic --context gke_kubeflow-platform_europe-west1-d_kf-prod main
kubectl logs life-acquatic-belafonte-ranker-hl5zx-1355929109 -n life-acquatic --context gke_kubeflow-platform_europe-west1-d_kf-prod main

skf run -e "[User Retention Predictions] (predict)" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" -n life-acquatic ml_test_spotify_kubeflow.examples.user_retention_prediction.predict_pipeline.pipeline
gsutil ls gs://belafonte/Inferrer.Inferrer/inference_result/186056/20200706T190634.577253-7596916ece6f/prediction_logs-00000-of-00006.gz
python -m spotify_kubeflow.scripts.prediction_viewer "gs://belafonte/Inferrer.Inferrer/inference_result/186056/20200706T190634.577253-7596916ece6f/prediction_logs-00000-of-00006.gz" > results.json

# install spotify qstyx locally
pip install -i https://artifactory.spotify.net/artifactory/api/pypi/pypi/simple/ spotify-quick-styx

# define the location of the image to run your workflow
IMAGE_LOCATION=gcr.io/acmacquisition/user_retention_prediction_dnn

# build the docker image
docker build -t ${IMAGE_LOCATION} .

docker build -t gcr.io/gro-analytics/kubeflow-test-datapull:test . -f src/bqt-query-artist/Dockerfile

docker run -t -v /Users/mzhu/sa_credential/atlas-pipeline-bq/google-application-credentials.json/:/secrets/google-application-credentials.json -e GOOGLE_APPLICATION_CREDENTIALS=/secrets/google-application-credentials.json   gcr.io/gro-analytics/kubeflow-test-datapull:test  \
--partition 2019-06-30 --limit_n_artist=10 --dataset=mzhu --tablename=kubeflow_artist_test_input

styx workflow enable UserAggregatesJob ml-testing.UserAggregatesJob

Jukebox -
https://spotify.stackenterprise.co/questions/6908/6915#6915


cookiecutter git@ghe.spotify.net:kubeflow-platform/spotify-kubeflow-cookie.git


cookiecutter git@ghe.spotify.net:kubeflow-platform/spotify-kubeflow-cookie.git

 lingh@Lings-MacBook-Pro  ~/Git  cookiecutter git@ghe.spotify.net:kubeflow-platform/spotify-kubeflow-cookie.git
You've downloaded /Users/lingh/.cookiecutters/spotify-kubeflow-cookie before. Is it okay to delete and re-download it? [yes]: yes
owner []: life-acquatic
component_id [ml-gp-life-acquatic]: belafonte-user-retention
owner_email [life-acquatic@spotify.com]: life-acquatic-private@spotify.com
owner_slack [#life-acquatic]: #life-acquatic
gcp_project_for_dataflow [ml-sketchbook]: acmacquisition
gcp_service_account_email [ml-paved-road-pipeline-styx@ml-sketchbook.iam.gserviceaccount.com]: acq-kubeflow-sa@acmacquisition.iam.gserviceaccount.com
gcs_bucket_for_pipeline_output [gs://ml-golden-path-pipeline/output/life-acquatic]: gs://belafonte/output/life-acquatic
gcs_bucket_for_dataflow_staging [gs://dataflow-staging-europe-west1-364472652419]: gs://belafonte/dataflow-staging-europe-west1-364472652419
system [ml-golden-path]: belafonte-user-retention
project_name [ML Golden Path Pipeline life-acquatic]: Kubeflow Pipeline for Belafonte User Predictions
description [Kubeflow Pipeline for Belafonte User Predictions spotify-kubeflow pipeline]:
python_module_name [belafonte_user_retention]:
Select enable_mypy:
1 - yes
2 - no
Choose from 1, 2 [1]:
namespace [life-acquatic]:
Initialized empty Git repository in /Users/lingh/Git/belafonte-user-retention/.git/
[master (root-commit) 1f93d6d] Initial commit
 45 files changed, 1864 insertions(+)
 create mode 100644 .dockerignore
 create mode 100644 .gitignore
 create mode 100644 .pre-commit-config.yaml
 create mode 100644 Dockerfile
 create mode 100644 Makefile
 create mode 100644 belafonte_user_retention/__init__.py
 create mode 100644 belafonte_user_retention/examples/__init__.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/__init__.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/data_defaults.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/defaults.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/evaluator.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/features/__init__.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/features/all_features.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/features/basic_features.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/pipeline.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/predict_pipeline.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/preprocessing.py
 create mode 100644 belafonte_user_retention/examples/apm_golden_path/trainer.py
 create mode 100644 belafonte_user_retention/examples/luigi/__init__.py
 create mode 100644 belafonte_user_retention/examples/luigi/tasks.py
 create mode 100644 belafonte_user_retention/examples/print/__init__.py
 create mode 100644 belafonte_user_retention/examples/print/pipeline.py
 create mode 100644 belafonte_user_retention/examples/taxi/__init__.py
 create mode 100644 belafonte_user_retention/examples/taxi/data_defaults.py
 create mode 100644 belafonte_user_retention/examples/taxi/defaults.py
 create mode 100644 belafonte_user_retention/examples/taxi/evaluator.py
 create mode 100644 belafonte_user_retention/examples/taxi/pipeline.py
 create mode 100644 belafonte_user_retention/examples/taxi/preprocessing.py
 create mode 100644 belafonte_user_retention/examples/taxi/trainer.py
 create mode 100644 build-info.yaml
 create mode 100644 data-endpoints.yaml
 create mode 100644 data-info.yaml
 create mode 100644 dev-requirements.txt
 create mode 100644 docs-requirements.txt
 create mode 100644 docs/README.md
 create mode 100644 mkdocs.yml
 create mode 100644 mypy.ini
 create mode 100644 pyproject.toml
 create mode 100644 requirements.txt
 create mode 100755 scripts/productionize.py
 create mode 100644 test-requirements.txt
 create mode 100644 tests/__init__.py
 create mode 100644 tests/main.py
 create mode 100644 tests/test_nothing.py
 create mode 100644 tox.ini

 skf run -e "[User Retention Predictions] (training)" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" -n life-acquatic belafonte_user_retention.examples.print.pipeline.py
 skf -mc prod run -e "[Kubeflow Pipeline for Belafonte User Predictions] Print Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" -n life-acquatic belafonte_user_retention.examples.print.pipeline.pipeline
 skf -mc prod run -e "[Kubeflow Pipeline for Belafonte User Predictions] Print Example" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" -n life-acquatic belafonte_user_retention.user-retention-prediction.print.pipeline.pipeline

 skf -mc prod run \
    -e "[ML Golden Path Pipeline Ling] Print Example" \
    -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" \
    ml_gp_lingh.examples.print.pipeline

skf run \
    -e "[[ML Golden Path Pipeline lingh] Artist Preference Pipeline" \
    -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" \
    ml_gp_lingh.examples.apm_golden_path.pipeline.pipeline

git@ghe.spotify.net:kubeflow-platform/spotify-kubeflow-cookie.git    


skf run -e "[Kubeflow Pipeline for Belafonte User Predictions] User Retention Prediction" -r "test run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" -n life-acquatic belafonte_user_retention.user_retention_prediction.user_retention_prediction.pipeline.pipeline

skf run -e "[Kubeflow Pipeline for Belafonte User Retention Predictions] User Retention Prediction (Batch Prediction)" -r "prediction run - $USER - $(date '+%Y-%m-%dT%H:%M:%S')" -n life-acquatic belafonte_user_retention.user_retention_prediction.user_retention_prediction.predict_pipeline.pipeline

# install spotify qstyx locally
pip install -i https://artifactory.spotify.net/artifactory/api/pypi/pypi/simple/ spotify-quick-styx

# define the location of the image to run your workflow
IMAGE_LOCATION=gcr.io/${GCP_PROJECT}/${IMAGE_NAME_WITH_TAG}
IMAGE_LOCATION=gcr.io/acmacquisition/life-acquatic-belafonte-user-retention

# build the docker image
docker build -t gcr.io/acmacquisition/life-acquatic-belafonte-user-retention .
# WORKFLOW_NAME is the workflow id defined in data-info.yaml.
# STYX_PARAMETER is usually a partition (e.g. 2016-09-14) identifying the workflow instance.
qstyx run -f data-info.yaml \
          -i gcr.io/acmacquisition/life-acquatic-belafonte-user-retention \
          -w user-retention-prediction \
          -p ${STYX_PARAMETER} \
          --legacy-auth

https://docs.google.com/document/d/1LYEZvZvt8f7-s_qBn8XoyKOWPyXbZyJ2s3F8TUbt2cE/edit#heading=h.g7iosm59uvjh

qstyx run -f data-info.yaml -i gcr.io/acmacquisition/life-acquatic-belafonte-user-retention -w belafonte-user-retention-prediction.Training  -p 2020-07-31 --legacy-auth

python -m spotify_kubeflow.scripts.prediction_viewer "gs://belafonte/output/life-acquatic/Inferrer.Inferrer/inference_result/223152/20200729T204644.557694-2ac5193a76fc/prediction_logs-00000-of-*.gz" > results.json

python -m spotify_kubeflow.scripts.prediction_viewer "gs://belafonte/output/life-acquatic/Inferrer.Inferrer/inference_result/223152/20200729T204644.557694-2ac5193a76fc/prediction_logs-00000-of-00003.gz" > results.json

https://ghe.spotify.net/ltv-modeling-user/ltv-scio-data-pipelines/pull/19/files


=======================================
## acm-acquisition-bq-runner
=======================================
LUIGI_LOG_LEVEL=DEBUG bq-runner RunYaml --dry-run --partition 2020-08-01 --target acmacquisition.bqr.display_adjust_YYYYMMDD

LUIGI_LOG_LEVEL=DEBUG bq-runner RunYaml --dry-run --partition 2019-11-17 --target automated-marketing-engagement.campaigns.rap_caviar_common_YYYYMMDD
bq-runner RunYaml --partition {DATE} --target acmacquisition.example.example_YYYYMMDD

https://ghe.spotify.net/mixtapes-internal-tools/mixtapes-rec-metrics/blob/master/data-info.yaml


Display: ad_unit_id comes from creative
Facebook: ad_unit_id from ad_name


tensorboard --host localhost --port 8080 --logdir gs://ltv-modeling-user/ltv-kubeflow-pipeline/Trainer.Trainer/output/252545/20200819T165025.177842-dd365e91cec6/serving_model_dir
