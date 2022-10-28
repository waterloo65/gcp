# Neptune cloud function 

```
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-central1'
export RUNTIME=java11
export SOURCE_PATH=./
export MAIN_CLASS=com.moonbank.function.NeptuneActivitiesProcessor
export PUBSUB_TOPIC=neptune-activities

gcloud functions deploy mb-deuser9-neptune-function \
--gen2 \
--region=${REGION} \
--runtime=${RUNTIME} \
--source=${SOURCE_PATH} \
--entry-point=${MAIN_CLASS} \
--trigger-topic=${PUBSUB_TOPIC}

```

#Sample csv message
```
20200812040801981475,195.174.170.81,UPDATE,GB25BZMX47593824219489,8,Emily Blair,STAFF
20200812040801981475,195.174.170.81,UPDATE,GB25BZMX47593824219489,9,Emily Blair,STAFF
```