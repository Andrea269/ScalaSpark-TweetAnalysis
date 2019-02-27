#!/bin/bash
RUN_SESSION=20181122114000

GCP_PROJECT=mytweetanalysis2
GCS_SRC_BUCKET_NAME=bucket-twitter

SCALA_JAR_FILENAME=twitterProva.jar
SCALA_JAR_FILE=codebase/target/scala-2.11/${SCALA_JAR_FILENAME}
SCALA_JAR_FILE_LOCALPATH=file://$(pwd) 
SCALA_RUNNABLE_CLASS=ScalaTweetAnalysis7
SCALA_JAR_FILE_FOR_JOB_SUBMIT=gs://${GCS_SRC_BUCKET_NAME}/${SCALA_JAR_FILENAME}

DATA_FILE=prova.txt

DATAPROC_CLUSTER_NAME=twitter-test-cluster-${RUN_SESSION}
DATAPROC_CLUSTER_REGION=europe-west1
DATAPROC_CLUSTER_ZONE=europe-west1-d

LOCAL_OUTPUT_PATH=/home/intersect/Desktop/localCloud

#create cluster
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+ "STARTING CLUSTER '${DATAPROC_CLUSTER_NAME}' ..."
echo "===================================================================================="
gcloud dataproc clusters create ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} --zone ${DATAPROC_CLUSTER_ZONE} --scopes storage-rw --worker-machine-type n1-standard-2 --image-version 1.1
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"CLUSTER '${DATAPROC_CLUSTER_NAME}' STARTED!"
echo "===================================================================================="

efTcZmWVuIOC9gncfFBb4Fnav SeCYzOYN3Azy3q24aSXauVAl4cHEqPaUt3vDHQF9OmIeQAWBqa 1073515508593541120-ykHDZFiyascCGAcU1YX001SySnJYOR liSyTQvHBzlgZQ5vq9KpeJFsyYv6LhrbyJKOpReYGfdP6 input/ Interface/GraphData/ Run2

#run job 1
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"RUNNING SPARK JOB '${SCALA_RUNNABLE_CLASS}' OVER '${DATA_FILE}' DATA FILE ON '${DATAPROC_CLUSTER_NAME}' CLUSTER ..."
echo "===================================================================================="
gcloud dataproc jobs submit spark --cluster ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} \
      --class ${SCALA_RUNNABLE_CLASS} \
      --jars ${SCALA_JAR_FILE_FOR_JOB_SUBMIT} \
      -- efTcZmWVuIOC9gncfFBb4Fnav SeCYzOYN3Azy3q24aSXauVAl4cHEqPaUt3vDHQF9OmIeQAWBqa 1073515508593541120-ykHDZFiyascCGAcU1YX001SySnJYOR liSyTQvHBzlgZQ5vq9KpeJFsyYv6LhrbyJKOpReYGfdP6 gs://bucket-twitter/input/ gs://bucket-twitter/input/ TypeRun1
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"SPARK JOB '${SCALA_RUNNABLE_CLASS}' DONE!"
echo "===================================================================================="

#run job 2
echo "===================================================================================="
echo "$(date +"%d/%m/%Y - %H:%M:%S") - "+"RUNNING SPARK JOB '${SCALA_RUNNABLE_CLASS}' OVER '${DATA_FILE}' DATA FILE ON '${DATAPROC_CLUSTER_NAME}' CLUSTER ..."
echo "===================================================================================="
gcloud dataproc jobs submit spark --cluster ${DATAPROC_CLUSTER_NAME} --region ${DATAPROC_CLUSTER_REGION} \
      --class ${SCALA_RUNNABLE_CLASS} \
      --jars ${SCALA_JAR_FILE_FOR_JOB_SUBMIT} \
      -- efTcZmWVuIOC9gncfFBb4Fnav SeCYzOYN3Azy3q24aSXauVAl4cHEqPaUt3vDHQF9OmIeQAWBqa 1073515508593541120-ykHDZFiyascCGAcU1YX001SySnJYOR liSyTQvHBzlgZQ5vq9KpeJFsyYv6LhrbyJKOpReYGfdP6 gs://bucket-twitter/input/ gs://bucket-twitter/Interface/GraphData/ TypeRun2
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

#download output
echo "downloading output"
gsutil cp gs://bucket-twitter/output/* ${LOCAL_OUTPUT_PATH}
echo "end of everything"