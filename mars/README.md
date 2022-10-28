# Mars dataflow pipeline

```
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-central1'
export BUCKET=gs://${PROJECT_ID}-bucket
export PIPELINE_FOLDER=${BUCKET}
export MAIN_CLASS_NAME=com.moonbank.function.MarsActivitiesPipeline
export RUNNER=DataflowRunner
#export PUBSUB_TOPIC=projects/${PROJECT_ID}/topics/my_topic
export PUBSUB_TOPIC=projects/mb-deuser9/topics/mars-activities
export WINDOW_DURATION=60
export RAW_TABLE=${PROJECT_ID}:mars.raw
export OUTPUT_TABLE=${PROJECT_ID}:mars.activities

mvn compile exec:java \
-Dexec.mainClass=${MAIN_CLASS_NAME} \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT_ID} \
--region=${REGION} \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER}/temp \
--runner=${RUNNER} \
--inputTopic=${PUBSUB_TOPIC} \
--windowDuration=${WINDOW_DURATION} \
--rawTable=${RAW_TABLE} \
--outputTable=${OUTPUT_TABLE}"
```

#Sample csv message
```
20221016000006358410,203.172.77.210,SCHEDULE,GB63IOEE41708907941843,GB65TBIT49649150943375,691.42,Sara Anderson
20221016000006358410,203.172.77.210,SCHEDULE,GB63IOEE41708907941843,GB65TBIT49649150943375,691.44,Sara Anderson

```