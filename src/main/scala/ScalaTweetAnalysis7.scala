import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.annotation.tailrec
import org.apache.log4j.Logger
import org.apache.log4j.Level


object ScalaTweetAnalysis7 {
  var hashtagCounterMap: Map[String, Int] = scala.collection.immutable.Map[String, Int]()
  var hashtagSentimentMap: Map[String, Long] = scala.collection.immutable.Map[String, Long]()
  var edgeMap: Map[(String, String), Long] = scala.collection.immutable.Map[(String, String), Long]()
  val percent: Int = 30

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
    val sparkConf = new SparkConf() //configura spark
    sparkConf.setAppName("ScalaTweetAnalysis7").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    downloadTweet(sc, args) //avvia il download
  }

  /**
    *
    * @param sc   SparkContext
    * @param args consumerKey, consumerKeySecret, accessToken, accessTokenSecret, pathInput, pathOutput, numRun
    */
  def downloadTweet(sc: SparkContext, args: Array[String]) = {
    //leggo dai parametri passati dall'utente le 4 chiavi twitter
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret, pathInput, pathOutput, numRun) = args.take(7)
    //crea il contesto di streaming con un intervallo di 15 secondi
    val ssc = new StreamingContext(sc, Seconds(10))
    //leggo il nome del file ed estrapolo gli hashtag da ricercare
    val pathFilter = if (numRun.equals("Run1")) pathInput + "HashtagRun1" else pathInput + "HashtagRun2"
    var filters = readFile(pathFilter).map(t => " " + t + " ")
    var timeRun = readFile(pathInput + "Time").map(t => t.split("=")(1))
    //crea la variabile di configurazione della richiesta popolandola con le chiavi di accesso e le Info dell'Api
    val confBuild = new ConfigurationBuilder
    confBuild.setDebugEnabled(true)
      .setTweetModeExtended(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerKeySecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setIncludeMyRetweetEnabled(false)
      .setUserStreamRepliesAllEnabled(false)
    val authorization = new OAuthAuthorization(confBuild.build) //crea struttura di autenticazione

    //crea lo stream per scaricare i tweet applicando o meno un filtro
    val tweetsDownload = if (filters.length > 0) TwitterUtils.createStream(ssc, Some(authorization), filters) else TwitterUtils.createStream(ssc, Some(authorization))
    val tweetFilter = tweetsDownload.filter(_.getLang() == "en")

    //crea un rdd dove ad ogni tweet è associato un oggetto contenente le sue info
    val tweetEdit = tweetFilter.map(t => (t, t.getText)) //coppie (t._1, t._2) formate dall'intero tweet (_1) e il suo testo (_2)
      //      (t, if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText)) //coppie (t._1, t._2) formate dall'intero tweet (_1) e il suo testo (_2)
      .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
      .map(t => TweetStruc.tweetStuct(t._1.getId, t._2, t._1.getUser.getScreenName, t._1.getCreatedAt.toInstant.toString, t._1.getLang)) //crea la struttura del tweet

    val spark = SparkSession
      .builder
      .appName("twitter trying")
      .getOrCreate()

    val data = tweetEdit.map(t => for (a <- t._4.split(" ")) if (!a.equals("")) hashtagCounterMap += a -> (hashtagCounterMap.getOrElse(a, 0) + 1))
    data.foreachRDD { rdd => rdd.collect() }
    if (!numRun.equals("Run1")) {

      tweetEdit.foreachRDD { rdd =>
        import spark.implicits._
        val dataFrame = rdd.toDF("id", "text", "sentiment", "hashtags", "userMentioned", "user", "createAt", "language")
        //        dataFrame.select("hashtags").show()

        dataFrame.createOrReplaceTempView("dataFrame")
        val text2 = spark.sql("SELECT hashtags, sentiment FROM dataFrame")
        //        text2.show(50, false)
        for (a <- hashtagCounterMap) {
          //          println("ioconto")
          //          println(hashtagCounterMap.size)
          val listPositionsApostrofo = indexesOf(a._1, "'")
          var keyParsedA = a._1
          for (c <- listPositionsApostrofo) {
            keyParsedA = a._1.patch(c, "'", 0)
          }

          val sentiment = spark.sql("SELECT SUM(sentiment) FROM dataFrame WHERE hashtags LIKE '% " + keyParsedA + " %'")
          var sum2: Long = 0
          val isNull = sentiment.head().anyNull
          if (!isNull) {
            sum2 = sentiment.head.getLong(0)
          }
          val sum3: Long = hashtagSentimentMap.getOrElse((a._1), 0)
          val sum4 = sum3 + sum2

          hashtagSentimentMap += (a._1) -> sum4
          //          println("chiave " + keyParsedA)
          //          sentiment.show(50, false)
          var bool = false
          for (b <- hashtagCounterMap) {
            if (bool) {
              val listPositionsApostrofo = indexesOf(b._1, "'")
              var keyParsedB = b._1
              for (c <- listPositionsApostrofo) {
                keyParsedB = b._1.patch(c, "'", 0)
              }
              val links = spark.sql("SELECT COUNT(id) FROM dataFrame WHERE hashtags LIKE '% " + keyParsedA + " %' AND hashtags LIKE '% " + keyParsedB + " %'")

              //              links.show(50, false)
              val tipo2: Long = links.head().getLong(0)

              if (tipo2 != 0) {
                val tipo3: Long = edgeMap.getOrElse((a._1, b._1), 0)
                val tipo4 = tipo3 + tipo2
                edgeMap += (a._1, b._1) -> tipo4
              }
            }
            if (a._1.equals(b._1)) { //per evitare ripetizioni tuple con gli elementi invertiti
              bool = true
            }
          }
        }
        rdd.collect()
      }
    }

    ssc.start() //avvia lo stream e la computazione dei tweet

    //setta il tempo di esecuzione altrimenti scaricherebbe tweet all'infinito
    // todo Exception in thread "streaming-job-executor-0" java.lang.Error: java.lang.InterruptedException
    ssc.awaitTerminationOrTimeout(if (numRun.equals("Run1")) timeRun(0).toLong else timeRun(1).toLong)

    ssc.stop() //ferma lo StreamingContext

    println(hashtagCounterMap)
    println(edgeMap)
    println(hashtagSentimentMap)

    if (numRun.equals("Run1"))
      writeFile(pathOutput + "HashtagRun2", getTopHashtag())
    else
      graphComputation(pathOutput)
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
  def writeFile(filename: String, text: String) = {
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
  def getTopHashtag(): String = {
    val orderHashtag = hashtagCounterMap.toSeq.sortWith(_._2 > _._2).map(t => t._1).toArray
    var topHashtag = ""
    for (i <- 0 until orderHashtag.length * percent / 100) topHashtag += orderHashtag(i) + "\n"
    topHashtag
  }

  /**
    *
    * @param source
    * @param target
    * @param index
    * @param withinOverlaps
    * @return
    */
  def indexesOf(source: String, target: String, index: Int = 0, withinOverlaps: Boolean = false): List[Int] = {
    @tailrec def recursive(indexTarget: Int, accumulator: List[Int]): List[Int] = {
      val position = source.indexOf(target, indexTarget)
      if (position == -1) accumulator
      else
        recursive(position + (if (withinOverlaps) 1 else target.size), position :: accumulator)
    }
    recursive(index, Nil).reverse
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
      textGraph += "\",\n      \"group\": " + hashtagSentimentMap.getOrElse(i._1, 3) //todo 3 ipotesi mixed
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
      val x1 = orderKnots.getOrElse(i._1._1, 1) -1
      val x2 = orderKnots.getOrElse(i._1._2, 1) -1
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


/*
tweetsDownload.foreachRDD { rdd =>
  rdd.map(t => (
    t, //tweet
    if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText // testo del tweet
  ))
    .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
    .map(t => TweetStruc.tweetStuctString(t._1.getId, t._2, t._1.getUser.getScreenName, t._1.getCreatedAt.toInstant.toString, t._1.getLang)) //crea la struttura del tweet
    .saveAsTextFile("OUT/tweets") //salva su file i tweet "OUT/tweets"
  //        .persist()
}

tweetEdit.foreachRDD { rdd => rdd.saveAsTextFile(pathOutput) } //salva su file i tweet

tweetsDownload.map(t => (t, if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText))//coppie (t._1, t._2) formate dall'intero tweet (_1) e il suo testo (_2)
  .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
  .map(t => TweetStruc.tweetStuctString(t._1.getId, t._2, t._1.getUser.getScreenName, t._1.getCreatedAt.toInstant.toString, t._1.getLang)) //crea la struttura del tweet
  .foreachRDD { rdd => rdd.saveAsTextFile("OUT/tweets") } //salva su file i tweet



val tweetFilter = tweetsDownload.filter(t => if (t.getRetweetedStatus != null) t.getRetweetedStatus.getInReplyToUserId() == -1 else t.getInReplyToUserId() == -1)

*/

//todo tweet in risposta senza hashtag vengono comunque presi