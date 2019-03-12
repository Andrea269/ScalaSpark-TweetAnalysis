# Tweets Analysis using Scala + Spark

Tested with JDK 1.8.

The project is dedicated to the analysis of the hashtags in the tweets through spark streaming. The final output are two files javascript in which there are data encoded with json that contain the information to compute on a web interface (also included into the project) the graph and the bubble chart.

## Requirements

* To execute the main analysis class it is necessary to obtain the following 4 access keys for api twitter requests: consumerKey, consumerKeySecret, accessToken
, accessTokenSecret. To get them you need to register a developer account on twitter and create an app.
* The code can be executed using the Google Cloud Platform
* Be connected to internet to be able to download the tweets

##Usage
To execute the program we make available the file "RunCloud.sh". In this file there are the following variables that need to be modified.

 * CONSUMER_KEY: String provided by Twitter Developer API
 * CONSUMER_KEY_SECRET: String provided by Twitter Developer API
 * ACCESS_TOKEN: String provided by Twitter Developer API
 * ACCESS_TOKEN_SECRET: String provided by Twitter Developer API
 * PATH_INPUT: GCP bucket path where data for the computations are saved
 * PATH_OUTPUT: GCP bucket path where the output is saved
 * TIME_RUN: milliseconds of the duration of the streaming
 * PERCENT: a percent number used to make the cutoff of the hashtags to be used in the successive run
 * SCALA_JAR_FILENAME: the name of the jar with the code and all the dependencies 

To use the Google Cloud Platform, using the command "sby assembly" from the project directory, it is possibile  

After entering the keys you can also pass a set of strings representing the filters to be applied to the download of the tweets, continuing to separate the parameters with a tab


