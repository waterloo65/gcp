# Neptune cloud function 

```
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-central1'
export PUBSUB_TOPIC=projects/mb-deuser9/topics/neptune-activities
export RAW_TABLE=${PROJECT_ID}:neptune.raw
export OUTPUT_TABLE=${PROJECT_ID}:neptune.activities

gcloud functions deploy neptune-activities-function \
[--gen2] \
--region=${REGION} \
--runtime=java11 \
--source=./src/main/java \
--entry-point=com.moonbank.function.StorageFunction \
--trigger-topic=${PUBSUB_TOPIC}


```
