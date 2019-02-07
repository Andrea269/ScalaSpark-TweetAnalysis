import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object ScalaTweetAnalysis7 {
  var hashtagCounterMap: Map[String, Int] = scala.collection.immutable.Map[String, Int]()
  var edgeMap: Map[(String, String), Long] = scala.collection.immutable.Map[(String, String), Long]()
  val percent: Int= 30
  /**
    *
    * @param args consumerKey consumerKeySecret accessToken accessTokenSecret filter [Optional]
    */
  def main(args: Array[String]) {
    if (args.length < 7) {
      //controlla che almeno le 4 chiavi di accesso siano state passate come parametro al programma
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret> [<filters>]")
      System.exit(1)
    }
    val sparkConf = new SparkConf() //configura spark
    sparkConf.setAppName("ScalaTweetAnalysis7").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //avvia il download
    downloadTweet(sc, args)
  }

  /**
    *
    * @param sc SparkContext
    * @param args consumerKey, consumerKeySecret, accessToken, accessTokenSecret, pathInput, pathOutput, numRun
    */
  def downloadTweet(sc: SparkContext, args: Array[String]) = {
    //leggo dai parametri passati dall'utente le 4 chiavi twitter
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret, pathInput, pathOutput, numRun) = args.take(7)
    //crea il contesto di streaming con un intervallo di 15 secondi
    val ssc = new StreamingContext(sc, Seconds(10))
    //leggo il nome del file ed estrapolo gli hashtag da ricercare
    val pathFilter= if(numRun.equals("Run1")) pathInput+"HashtagRun1" else pathInput+"HashtagRun2"
    var filters = readFile(pathFilter).map(t => " "+t+" ")
    var timeRun = readFile(pathInput+"Time").map(t => t.split("=")(1))
    //crea la variabile di configurazione della richiesta popolandola con le chiavi di accesso
    val confBuild = new ConfigurationBuilder
    confBuild.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerKeySecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)
    //crea struttura di autenticazione
    val authorization = new OAuthAuthorization(confBuild.build)
    //crea lo stream per scaricare i tweet applicando o meno un filtro
    val tweetsDownload = if (filters.length > 0) TwitterUtils.createStream(ssc, Some(authorization), filters) else TwitterUtils.createStream(ssc, Some(authorization))
    //crea un rdd dove ad ogni tweet è associato un oggetto contenente le sue info

    val tweetEdit=tweetsDownload.map(t => (t, if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText)) //coppie (t._1, t._2) formate dall'intero tweet (_1) e il suo testo (_2)
      .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
      .map(t => TweetStruc.tweetStuct(t._1.getId, t._2, t._1.getUser.getScreenName, t._1.getCreatedAt.toInstant.toString, t._1.getLang)) //crea la struttura del tweet

    val spark = SparkSession
      .builder
      .appName("twitter trying")
      .getOrCreate()


    val data= tweetEdit.map(t => for (a <- t._4.split(" ")) if(!a.equals("")) hashtagCounterMap+= a -> (hashtagCounterMap.getOrElse(a, 0) + 1) )
    data.foreachRDD{rdd => rdd.collect()}
    if(!numRun.equals("Run1")){
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)

      tweetEdit.foreachRDD { rdd =>
        import spark.implicits._
        val dataFrame = rdd.toDF("id", "text", "sentiment", "hashtags", "userMentioned", "user", "createAt", "language")
        //      dataFrame.select("hashtags").show()

        dataFrame.createOrReplaceTempView("dataFrame")
        val text2 = sqlContext.sql("SELECT hashtags FROM dataFrame")
        text2.show()
        for (a <- hashtagCounterMap) {
          for(b <-hashtagCounterMap) {
            if (!a._1.equals(b._1)) {
              val links = sqlContext.sql("SELECT COUNT(id) FROM dataFrame WHERE hashtags LIKE '% " + a._1 + " %' AND hashtags LIKE '% " + b._1 + " %'")
              val tipo2: Long = links.head().getLong(0)
              //println("test: " + a._1 + " " + b._1 + " " + tipo)
              if(tipo2 != 0) edgeMap += (a._1, b._1) -> tipo2
            }
          }
          println(a._1)
          val teenagers = sqlContext.sql("SELECT COUNT(id) FROM dataFrame WHERE hashtags LIKE '% " + a._1 + " %'")
          teenagers.show()
          println("be oh")
          val tipo: Long = teenagers.head().getLong(0)
          println("count: " + tipo)
        }

        val countTweet = dataFrame.count()
        println("\n\n\n\nNumero Tweet " + countTweet +  "\n\n\n")
      }
    }

    //avvia lo stream e la computazione dei tweet
    ssc.start()
    //setta il tempo di esecuzione altrimenti scaricherebbe tweet all'infinito
    ssc.awaitTerminationOrTimeout(if(numRun.equals("Run1")) timeRun(0).toLong else  timeRun(1).toLong)

    println("eee")
    println(hashtagCounterMap)
    println(edgeMap)
    //ssc.awaitTerminationOrTimeout(300000) //5 min

//    for ((k,v) <- hashtagCounterMap) println(s"key: $k, value: $v")
    if(numRun.equals("Run1"))
      writeFile(pathOutput+"HashtagRun2", getTopHashtag())
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
    val wrappedStream= inputStream.getWrappedStream

    var textFile: String =""
    var tempInt= wrappedStream.read()
    do{
      textFile+=tempInt.toChar
      tempInt= wrappedStream.read()
    }while(!tempInt.equals(-1))

    wrappedStream.close()

    textFile.split("\n")
  }

  /**
    *
    * @param filename
    * @param text
    */
  def writeFile(filename: String, text: String)= {
    val hadoopPath = new Path(filename)
    val outputPath: FSDataOutputStream = hadoopPath.getFileSystem(new Configuration()).create(hadoopPath)
    val wrappedStream= outputPath.getWrappedStream

    for(i<- text){
      wrappedStream.write(i.toInt)
    }
    wrappedStream.close()
  }

  /**
    *
    * @return
    */
  def getTopHashtag():String= {
    val orderHashtag=hashtagCounterMap.toSeq.sortWith(_._2 > _._2).map(t => t._1).toArray
    var topHashtag=""
    for(i<- 0 until orderHashtag.length * percent / 100) topHashtag+= orderHashtag(i) + "\n"

    topHashtag
  }

  /**
    *
    * @param pathOutput
    */
  def graphComputation(pathOutput: String): Unit = {
    var orderKnots: Map[String, Int] = scala.collection.immutable.Map[String, Int]()

    val numberHashtag=hashtagCounterMap.size
    var count= 0
    var textBubbleChart="var dataset = {\n    \"children\": ["
    var textGraph="{\n  \"nodes\": ["
    for (i <- hashtagCounterMap) {
      count+=1
      orderKnots+= i._1-> count
      textBubbleChart+="\n        {\n            \"name\": \""+i._1
      textBubbleChart+="\",\n            \"count\": "+i._2.toString
      textBubbleChart+="\n        }"

      textGraph+="\n    {\n      \"name\": \""+i._1
      textGraph+="\",\n      \"group\": "+count//i._2._2.toString
      textGraph+="\n    }"

      if(count!=numberHashtag) {
        textBubbleChart+=","
        textGraph+=","
      }
    }
    textBubbleChart+="\n    ]\n};"
    writeFile(pathOutput+"datiBubbleChart.js", textBubbleChart)

    textGraph+="\n  ],\n  \"links\": ["


    val numberEdge=edgeMap.size
    count= 0
    for (i <- edgeMap) {
      val x1=orderKnots.getOrElse(i._1._1, 0)
      val x2=orderKnots.getOrElse(i._1._2, 0)
      count+=1
      textGraph+="\n    {\n      \"source\": "+x1
      textGraph+=",\n      \"target\": "+x2
      textGraph+=",\n      \"weight\": "+i._2
      textGraph+="\n    }"
      if(count!=numberEdge) textGraph+=","
    }
    textGraph+="\n  ]\n}"
    writeFile(pathOutput+"datiGraph.json", textGraph)
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

*/

//todo tweet in risposta senza hashtag vengono comunque presi