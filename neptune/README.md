# Neptune cloud function 

```
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-central1'
export RUNTIME=java11
export SOURCE_PATH=./
export MAIN_CLASS=com.moonbank.function.NeptuneActivitiesProcessor
export PUBSUB_TOPIC=neptune-activities
export RAW_TABLE=${PROJECT_ID}:neptune.raw
export OUTPUT_TABLE=${PROJECT_ID}:neptune.activities

gcloud functions deploy neptune-activities-function \
--gen2 \
--region=${REGION} \
--runtime=${RUNTIME} \
--source=${SOURCE_PATH} \
--entry-point=${MAIN_CLASS} \
--trigger-topic=${PUBSUB_TOPIC}

```
