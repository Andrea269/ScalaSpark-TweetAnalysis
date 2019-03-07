# Tweets Analysis using Scala + Spark

The project is dedicated to the analysis of the hashtags in the tweets through spark streaming. The final output are two json files with the information to compute on a web interface (also included into the project) the graph and the bubble chart.

Tested with JDK 1.8

To execute the main analysis class it is necessary to obtain the following 4 access keys for api twitter requests:
* consumerKey
* consumerKeySecret
* accessToken
* accessTokenSecret

To get them you need to register a developer account on twitter and create an app.
##
##For the execution
Past parameters must be separated by a tab
It is mandatory to pass the 4 access keys to run the application
** consumerKey
** consumerKeySecret
** accessToken
** accessTokenSecret

After entering the keys you can also pass a set of strings representing the filters to be applied to the download of the tweets, continuing to separate the parameters with a tab


