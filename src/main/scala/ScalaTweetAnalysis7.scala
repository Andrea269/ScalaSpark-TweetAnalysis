/**
  * Fasano Domenico & Pascali Andrea - University of Bologna
  * Project for "Scala and Cloud Programming"
  * Tweet Analysis - Creation of a graph of correlated hashtags given a hashtag
  */


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder


object ScalaTweetAnalysis7 {
  var hashtagCounterMap: Map[String, Int] = scala.collection.immutable.Map[String, Int]() //contains couples formed by the hashtags and the count of tweets in which there is that hashtag
  var hashtagSentimentMap: Map[String, Int] = scala.collection.immutable.Map[String, Int]() //contains couples formed by the hashtags and the accumulator of sentiment values
  var edgeMap: Map[(String, String), Int] = scala.collection.immutable.Map[(String, String), Int]() //contains couples of hashtags with a counter of tweets in which there are both hashtags
  var nodeHigherEdgeValueMap: Map[String, Int] = scala.collection.immutable.Map[String, Int]() //contains couples formed by the hashtags and the value of their edge wtih the higher weight

  /**
    *
    * @param args consumerKey: String provided by Twitter Developer API
    *             consumerKeySecret: String provided by Twitter Developer API
    *             accessToken: String provided by Twitter Developer API
    *             accessTokenSecret: String provided by Twitter Developer API
    *             pathInput: GCP bucket path where data for the computations are saved
    *             pathOutput: GCP bucket path where the output is saved
    *             typeRun: type of run: "TypeRun1" (for the all run to download the tweets) or "TypeRun2" (for the last run that compute also the final data needed for write the graph data on file)
    *             timeRun: milliseconds of the duration of the streaming
    *             percentHashtag: a percent number used to make the cutoff of the hashtags to be used in the successive run
    */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF) //remove logs from the console output
    Logger.getLogger("akka").setLevel(Level.OFF)
    if (args.length < 9) { //check that all the needed parameters are given in input
      System.err.println("Provide input:<ConsumerKey><ConsumerSecret><AccessToken><AccessTokenSecret><PathInput><PathOutput><TypeRun><TimeRun><PercentHashtag>")
      System.exit(1)
    }
    val pathInput = args(4)
    val pathOutput = args(5)
    val typeRun = args(6)
    val percent: Int = args(8).toInt

    val sparkConf = new SparkConf()
    sparkConf.setAppName("ScalaTweetAnalysis7").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    downloadComputeTweet(sc, args)

    hashtagCounterMap = serializeMap(pathInput + "hashtagCounterMap", hashtagCounterMap)
    edgeMap = serializeMap(pathInput + "edgeMap", edgeMap.map(t => (t._1._1 + "," + t._1._2, t._2))).map(t => ((t._1.split(",")(0), t._1.split(",")(1)), t._2))
    hashtagSentimentMap = serializeMap(pathInput + "hashtagSentimentMap", hashtagSentimentMap)

    if (typeRun.equals("TypeRun1"))
      writeFile(pathOutput + "HashtagRun", getTopHashtag(percent))
    else {
      //fill nodeHigherEdgeValueMap
      for (a <- hashtagCounterMap) {
        nodeHigherEdgeValueMap += a._1 -> 0
      }
      for (b <- edgeMap) {
        if (b._2 > nodeHigherEdgeValueMap.getOrElse(b._1._1, 0)) {
          nodeHigherEdgeValueMap += b._1._1 -> b._2
        }
        if (b._2 > nodeHigherEdgeValueMap.getOrElse(b._1._2, 0)) {
          nodeHigherEdgeValueMap += b._1._2 -> b._2
        }
      }

      graphComputation(pathOutput)

      //clean files for eventual new computations
      cleanFile(pathInput + "hashtagCounterMap")
      cleanFile(pathInput + "edgeMap")
      cleanFile(pathInput + "hashtagSentimentMap")
    }
  }

  /**
    *  Set the context of streaming and ask for the downloads of the tweets, then it do computations on the tweet, extracting the sentiment of the tweets
    *  and the set of the hashtags present in them. After it fill the maps hashtagCounterMap, hashtagSentimentMap, hashtagSentimentMap and edgeMap.
    *
    * @param sc   the SparkContext
    * @param args the input parameters of the main
    */
  def downloadComputeTweet(sc: SparkContext, args: Array[String]): Unit = {
    val ssc = new StreamingContext(sc, Seconds(1)) //create the streaming context with mini-batch of 1 seconds
    val timeRun: Long = args(7).toLong
    val tweetsDownload = downloadTweet(ssc, args, args(4) + "HashtagRun").filter(_.getLang() == "en")
    val tweetEdit = tweetsDownload.map(t => (t, if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText))
      .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //delete tweet repetitions
      .map(t => TweetCompute.TweetComputeSample(t._2)) //it create the structure of the tweet in our code
      .persist()

    tweetEdit.foreachRDD(p => p.foreach(t => for (y <- t._1) {
      hashtagCounterMap += y -> (hashtagCounterMap.getOrElse(y, 0) + 1)
      hashtagSentimentMap += y -> (hashtagSentimentMap.getOrElse(y, 3) + t._2)
      for (i <- t._1) if (y > i) edgeMap += (i, y) -> (edgeMap.getOrElse((i, y), 0) + 1)
    }))

    ssc.start()
    Thread.sleep(timeRun) //sleep the computation, in order to download the tweets for the time set, before closing the streaming context
    ssc.stop(true, true)
  }

  /** todo
    * Download the tweets appling a filter, if there is one, based on the hashtags that we want to be present in the tweets we download
    *
    * @param ssc        Streaming context
    * @param args       the input parameters of the main
    * @param pathFilter viene passato alla funzione l'array contenente l'elenco degli hashtag che devono essere presenti nei tweet scaricati
    * @return
    */
  private def downloadTweet(ssc: StreamingContext, args: Array[String], pathFilter: String): ReceiverInputDStream[Status] = {
    //read from the parameters the 4 keys needed have access to the twitter API to download tweets
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret) = args.take(4)
    var filters = readFile(pathFilter).map(t => " " + t + " ")
    val confBuild = new ConfigurationBuilder //configuration of the tweets request
    confBuild.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerKeySecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setTweetModeExtended(true) //to receive the full text of tweets longer than 144 chars
      .setIncludeMyRetweetEnabled(false)
      .setUserStreamRepliesAllEnabled(false)
    val authorization = new OAuthAuthorization(confBuild.build)

    //create and return the DStream to download the tweets with the filters if they are present
    if (filters.length > 0) TwitterUtils.createStream(ssc, Some(authorization), filters) else TwitterUtils.createStream(ssc, Some(authorization))
  }

  /** todo
    * Serializza in un file una mappa applicando una codifica personalizzata del tipo chiave=valore
    *
    * @param pathFilename il path seguito dal nome del file su cui salvare la serializzazione
    * @param mapSerialize rappresenta la mappa da serializzare, la quale deve avere come chiave una Stringa e come valore un Intero
    */
  private def serializeMap(pathFilename: String, mapSerialize: Map[String, Int]): Map[String, Int] = {
    var mapToSerialize = mapSerialize
    val fileCountHashtag = readFile(pathFilename).map(t => t.split("="))
    var countHashtag: Int = 0
    if (!(fileCountHashtag(0).length < 2))
      for (a <- fileCountHashtag) mapToSerialize += a(0) -> (mapToSerialize.getOrElse(a(0), 0) + a(1).toInt)

    var text = ""
    for (hashtag <- mapToSerialize) text += hashtag._1 + "=" + hashtag._2.toString + "\n"

    writeFile(pathFilename, text)
    mapToSerialize
  }

  private def cleanFile(filename: String): Unit = {
    writeFile(filename, "")
  }

  /** todo
    * Tale funzione ci permette di leggere un file indipendentemente dal file system istanziato sulla macchina che la invoca
    *
    * @param pathFilename il path seguito dal nome del file su cui andare a leggere il file
    * @return
    */
  private def readFile(pathFilename: String): Array[String] = {
    val hadoopPath = new Path(pathFilename)
    val inputStream: FSDataInputStream = hadoopPath.getFileSystem(new Configuration()).open(hadoopPath)
    val wrappedStream = inputStream.getWrappedStream
    var textFile: String = ""
    var tempInt = wrappedStream.read()
    do {
      textFile += tempInt.toChar
      tempInt = wrappedStream.read()
    } while (!tempInt.equals(-1))
    wrappedStream.close()
    textFile.split("\n")
  }

  /** todo
    * Tale funzione ci permette di scrivere un file indipendentemente dal file system istanziato sulla macchina che la invoca
    *
    * @param pathFilename il path seguito dal nome del file su cui scrivere
    * @param text         testo da scrivere sul file
    */
  private def writeFile(pathFilename: String, text: String): Unit = {
    val hadoopPath = new Path(pathFilename)
    val outputPath: FSDataOutputStream = hadoopPath.getFileSystem(new Configuration()).create(hadoopPath)
    val wrappedStream = outputPath.getWrappedStream
    for (i <- text) {
      wrappedStream.write(i.toInt)
    }
    wrappedStream.close()
  }

  /** todo
    * Tale funzione estrae la percentuale X più siglificativa dell'insieme di hashtag rilevati e la restituisce in output
    *
    * @param percent percentuale di hashtag da estrarre
    * @return lista dei top hashtag estratti
    */
  private def getTopHashtag(percent: Int): String = {
    val orderHashtag = hashtagCounterMap.toSeq.sortWith(_._2 > _._2).map(t => t._1).toArray
    var topHashtag = ""
    for (i <- 0 to orderHashtag.length * percent / 100) topHashtag += orderHashtag(i) + "\n"
    topHashtag
  }

  /** todo vedi te di scriverlo meglio ma è quello che fà
    * genera e successivamente salva su file i dataset contenenti i dati per la visualizzazione del
    * grafo e del bubbleChart rappresentanti i le analisi eseguite sui tweet
    *
    * @param pathOutput path di output dei file generati dalla funzione
    */
  private def graphComputation(pathOutput: String): Unit = {
    val numberHashtag = hashtagCounterMap.size
    var count = 0
    var textBubbleChart = "var dataset = {\n    \"children\": ["
    var textGraph = "var dataset ={\n  \"nodes\": ["
    for (i <- hashtagCounterMap) {
      count += 1
      textBubbleChart += "\n        {\n            \"name\": \"" + i._1
      textBubbleChart += "\",\n            \"count\": " + i._2.toString
      textBubbleChart += "\n        }"

      textGraph += "\n    {\n      \"name\": \"" + i._1
      textGraph += "\",\n      \"group\": " + hashtagSentimentMap.getOrElse(i._1, 3) / i._2
      textGraph += ",\n      \"weightMax\": " + nodeHigherEdgeValueMap.getOrElse(i._1, 0)
      textGraph += "\n    }"

      if (count != numberHashtag) {
        textBubbleChart += ","
        textGraph += ","
      }
    }
    textBubbleChart += "\n    ]\n};"
    writeFile(pathOutput + "datiBubbleChart.js", textBubbleChart)

    textGraph += "\n  ],\n  \"links\": ["
    val numberEdge = edgeMap.size
    count = 0
    for (i <- edgeMap) {
      count += 1
      textGraph += "\n    {\n      \"source\": \"" + i._1._1
      textGraph += "\",\n      \"target\": \"" + i._1._2
      textGraph += "\",\n      \"weight\": " + i._2
      textGraph += "\n    }"
      if (count != numberEdge) textGraph += ","
    }
    textGraph += "\n  ]\n};"
    writeFile(pathOutput + "datiGraph.js", textGraph)
  }
}

//todo toglilo per ultimo altrimenti non rileva gli altri todo, almeno a me era così