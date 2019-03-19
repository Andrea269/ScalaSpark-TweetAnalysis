#!/bin/bash
RUN_SESSION=20181122114000

# variables to be modified by the user
NUM_RUN_TYPE1=2 # number of run minus 1

# twitter keys
CONSUMER_KEY="INSERT-CONSUMER_KEY"
CONSUMER_KEY_SECRET="INSERT-CONSUMER_KEY_SECRET"
ACCESS_TOKEN="INSERT-ACCESS_TOKEN"
ACCESS_TOKEN_SECRET="INSERT-ACCESS_TOKEN_SECRET"

PATH_INPUT="gs://bucket-twitter/input/"
PATH_OUTPUT="gs://bucket-twitter/output/"
TIME_RUN=600000 # duration in milliseconds of each run
PERCENT=30 # percent of hashtags to extract in the successive run, based on the number of tweets in which the hashtag is present

SCALA_JAR_FILENAME=twitterAnalysis.jar
# end of variables to be modifed by the user


GCP_PROJECT=mytweetanalysis2
GCS_SRC_BUCKET_NAME=bucket-twitter

SCALA_JAR_FILE=codebase/target/scala-2.11/${SCALA_JAR_FILENAME}
SCALA_JAR_FILE_LOCALPATH=file://$(pwd) 
SCALA_RUNNABLE_CLASS=ScalaTweetAnalysis7
SCALA_JAR_FILE_FOR_JOB_SUBMIT=gs://${GCS_SRC_BUCKET_NAME}/${SCALA_JAR_FILENAME}

DATA_FILE=prova.txt

DATAPROC_CLUSTER_NAME=twitter-test-cluster-${RUN_SESSION}
DATAPROC_CLUSTER_REGION=europe-west1
DATAPROC_CLUSTER_ZONE=europe-west1-d

#create cluster
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+ "STARTING CLUSTER '${DATAPROC_CLUSTER_NAME}' ..."
echo "===================================================================================="
gcloud dataproc clusters create ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} --zone ${DATAPROC_CLUSTER_ZONE} --scopes storage-rw --worker-machine-type n1-standard-2 --image-version 1.1
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"CLUSTER '${DATAPROC_CLUSTER_NAME}' STARTED!"
echo "===================================================================================="


#run job Type 1 for N time$
for i in $(seq 1 $NUM_RUN_TYPE1); do

echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"RUNNING SPARK JOB '${SCALA_RUNNABLE_CLASS}' DATA FILE ON '${DATAPROC_CLUSTER_NAME}' CLUSTER ..."
echo "===================================================================================="
gcloud dataproc jobs submit spark --cluster ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} \
      --class ${SCALA_RUNNABLE_CLASS} \
      --jars ${SCALA_JAR_FILE_FOR_JOB_SUBMIT} \
      -- ${CONSUMER_KEY} ${CONSUMER_KEY_SECRET} ${ACCESS_TOKEN} ${ACCESS_TOKEN_SECRET} ${PATH_INPUT} ${PATH_INPUT} TypeRun1 ${TIME_RUN} ${PERCENT}
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"SPARK JOB '${SCALA_RUNNABLE_CLASS}' DONE!"
echo "===================================================================================="

done

#run job Type 2
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"RUNNING SPARK JOB '${SCALA_RUNNABLE_CLASS}' DATA FILE ON '${DATAPROC_CLUSTER_NAME}' CLUSTER ..."
echo "===================================================================================="
gcloud dataproc jobs submit spark --cluster ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} \
      --class ${SCALA_RUNNABLE_CLASS} \
      --jars ${SCALA_JAR_FILE_FOR_JOB_SUBMIT} \
      -- ${CONSUMER_KEY} ${CONSUMER_KEY_SECRET} ${ACCESS_TOKEN} ${ACCESS_TOKEN_SECRET} ${PATH_INPUT} ${PATH_OUTPUT} TypeRun2 ${TIME_RUN} ${PERCENT}
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"SPARK JOB '${SCALA_RUNNABLE_CLASS}' DONE!"
echo "===================================================================================="



#delete cluster
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"DELETING CLUSTER '${DATAPROC_CLUSTER_NAME}' ..."
echo "===================================================================================="
gcloud dataproc clusters delete -q ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION}
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"CLUSTER '${DATAPROC_CLUSTER_NAME}' DELETED!"
echo "===================================================================================="