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

//TODO un hashtag deve essere formato solo da lettere e numeri

object ScalaTweetAnalysis7 {
  var hashtagCounterMap: Map[String, Int] = scala.collection.immutable.Map[String, Int]()
  var hashtagSentimentMap: Map[String, Int] = scala.collection.immutable.Map[String, Int]()
  var edgeMap: Map[(String, String), Int] = scala.collection.immutable.Map[(String, String), Int]()
  val percent: Int = 30
  val thresholdLink: Int = 0

  /**
    *
    * @param args consumerKey consumerKeySecret accessToken accessTokenSecret pathInput pathOutput numberRun
    */
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    if (args.length < 7) {
      //controlla che tutti i parametri necessari siano stati forniti in input
      System.err.println("Provide input:<ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret><pathInput><pathOutput><numberRun>")
      System.exit(1)
    }
    val pathInput = args(4)
    val pathOutput = args(5)
    val numRun = args(6)
    val sparkConf = new SparkConf() //configura spark
    sparkConf.setAppName("ScalaTweetAnalysis7").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    downloadComputeTweet(sc, args) //esegue il download e la computazione dei tweet

    println(hashtagCounterMap)
    println(edgeMap)
    println(hashtagSentimentMap)
    println("\n\n")
    if (numRun.equals("TypeRun1")) {
      hashtagCounterMap = serializeMap(pathInput + "hashtagCounterMap", hashtagCounterMap, 1)
      edgeMap = serializeMap(pathInput + "edgeMap", edgeMap.map(t => (t._1._1 + "," + t._1._2, t._2)), 1).map(t => ((t._1.split(",")(0), t._1.split(",")(1)), t._2))
      hashtagSentimentMap = serializeMap(pathInput + "hashtagSentimentMap", hashtagSentimentMap, 2)

      writeFile(pathOutput + "HashtagRun", getTopHashtag)
    }
    else
      graphComputation(pathOutput)


    println(hashtagCounterMap)
    println(edgeMap)
    println(hashtagSentimentMap)
  }

  /**
    *
    * @param sc        SparkContext
    * @param args      consumerKey, consumerKeySecret, accessToken, accessTokenSecret
    */
  def downloadComputeTweet(sc: SparkContext, args: Array[String]): Unit = {
    val ssc = new StreamingContext(sc, Seconds(1)) //crea il contesto di streaming con un intervallo di X secondi
    var timeRun = readFile(args(4) + "Time").map(t => t.split("=")(1))
    val tweetsDownload = downloadTweet(ssc, args, args(4) + "HashtagRun").filter(_.getLang() == "en")
    val tweetEdit = tweetsDownload.map(t => (t, if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText)) //coppie (t._1, t._2) formate dall'intero tweet (_1) e il suo testo (_2)
      .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
      .map(t => TweetCompute.TweetComputeSample(t._2)) //crea la struttura del tweet
      .persist()

    tweetEdit.foreachRDD(p => p.foreach(t => for (y <- t._1) {
      hashtagCounterMap += y -> (hashtagCounterMap.getOrElse(y, 0) + 1)
      hashtagSentimentMap += y -> ((hashtagSentimentMap.getOrElse(y, 2) + t._2) / 2)
      for (i <- t._1) if (y > i) edgeMap += (i, y) -> (edgeMap.getOrElse((i, y), 0) + 1)
    }))

    ssc.start() //avvia lo stream e la computazione dei tweet
    Thread.sleep(if (args(6).equals("TypeRun1")) timeRun(0).toLong else timeRun(1).toLong) //setta il tempo di esecuzione
    ssc.stop(true, true) //ferma lo StreamingContext
  }

  /**
    * crea lo stream per scaricare i tweet applicando o meno un filtro
    *
    * @param ssc
    * @param args
    * @param pathFilter
    * @return
    */
  def downloadTweet(ssc: StreamingContext, args: Array[String], pathFilter: String): ReceiverInputDStream[Status] = {
    //leggo dai parametri passati dall'utente le 4 chiavi twitter
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret) = args.take(4)
    var filters = readFile(pathFilter).map(t => " " + t + " ")
    //crea la variabile di configurazione della richiesta popolandola con le chiavi di accesso e le Info dell'Api
    val confBuild = new ConfigurationBuilder
    confBuild.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerKeySecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setTweetModeExtended(true)
      .setIncludeMyRetweetEnabled(false)
      .setUserStreamRepliesAllEnabled(false)
    val authorization = new OAuthAuthorization(confBuild.build) //crea struttura di autenticazione

    //crea lo stream per scaricare i tweet applicando o meno un filtro
    if (filters.length > 0) TwitterUtils.createStream(ssc, Some(authorization), filters) else TwitterUtils.createStream(ssc, Some(authorization))
  }

  /**
    *
    * @param filename
    * @param mapSerialize
    */
  def serializeMap(filename: String, mapSerialize: Map[String, Int], div:Int): Map[String, Int] = {
    var mapToSerialize = mapSerialize
    val fileCountHashtag = readFile(filename).map(t => t.split("="))
    var countHashtag: Int = 0
    if (!(fileCountHashtag(0).length < 2)) {
      for (a <- fileCountHashtag) mapToSerialize += a(0) -> ((mapToSerialize.getOrElse(a(0), 0) + a(1).toInt)/div)
    }
    var text = ""
    for (hashtag <- mapToSerialize) {
      text += hashtag._1 + "=" + hashtag._2.toString + "\n"
    }
    writeFile(filename, text)
    mapToSerialize
  }

  /**
    *
    * @param filename
    * @return
    */
  def readFile(filename: String): Array[String] = {
    val hadoopPath = new Path(filename)
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

  /**
    *
    * @param filename
    * @param text
    */
  def writeFile(filename: String, text: String): Unit = {
    val hadoopPath = new Path(filename)
    val outputPath: FSDataOutputStream = hadoopPath.getFileSystem(new Configuration()).create(hadoopPath)
    val wrappedStream = outputPath.getWrappedStream
    for (i <- text) {
      wrappedStream.write(i.toInt)
    }
    wrappedStream.close()
  }

  /**
    *
    * @return
    */
  def getTopHashtag: String = {
    val orderHashtag = hashtagCounterMap.toSeq.sortWith(_._2 > _._2).map(t => t._1).toArray
    var topHashtag = ""
    for (i <- 0 to orderHashtag.length * percent / 100) topHashtag += orderHashtag(i) + "\n"
    topHashtag
  }

  /**
    *
    * @param pathOutput
    */
  def graphComputation(pathOutput: String): Unit = {
    var orderKnots: Map[String, Int] = scala.collection.immutable.Map[String, Int]()
    val numberHashtag = hashtagCounterMap.size
    var count = 0
    var textBubbleChart = "var dataset = {\n    \"children\": ["
    var textGraph = "var dataset ={\n  \"nodes\": ["
    for (i <- hashtagCounterMap) {
      count += 1
      orderKnots += i._1 -> count
      textBubbleChart += "\n        {\n            \"name\": \"" + i._1
      textBubbleChart += "\",\n            \"count\": " + i._2.toString
      textBubbleChart += "\n        }"

      textGraph += "\n    {\n      \"name\": \"" + i._1
      textGraph += "\",\n      \"group\": " + hashtagSentimentMap.getOrElse(i._1, 2)
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
    for (i <- edgeMap.filter(_._2 > thresholdLink)) {
      val x1 = orderKnots.getOrElse(i._1._1, 1) - 1
      val x2 = orderKnots.getOrElse(i._1._2, 1) - 1
      count += 1
      textGraph += "\n    {\n      \"source\": " + x1
      textGraph += ",\n      \"target\": " + x2
      textGraph += ",\n      \"weight\": " + i._2
      textGraph += "\n    }"
      if (count != numberEdge) textGraph += ","
    }
    textGraph += "\n  ]\n};"
    writeFile(pathOutput + "datiGraph.js", textGraph)
  }
}