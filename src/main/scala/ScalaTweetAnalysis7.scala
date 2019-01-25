import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.io.Source



object ScalaTweetAnalysis7 {

  val pathAbsolute="gs:\\\\"

  /**
    *
    * @param args consumerKey consumerKeySecret accessToken accessTokenSecret filter [Optional]
    */
  def main(args: Array[String]) {
    if (args.length < 4) {
      //controlla che almeno le 4 chiavi di accesso siano state passate come parametro al programma
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret> [<filters>]")
      System.exit(1)
    }
    val sparkConf = new SparkConf() //configura spark
    sparkConf.setAppName("ScalaTweetAnalysis7").setMaster("local[*]")

    //avvia il download e il salvataggio dei tweet
    downloadTweet(sparkConf, args)
  }

  /**
    *
    * @param sparkConf variabile di configurazione di spark
    * @param args      consumerKey consumerKeySecret accessToken accessTokenSecret filter [Optional]
    */
  def downloadTweet(sparkConf: SparkConf, args: Array[String]): Unit = {
    //leggo dai parametri passati dall'utente le 4 chiavi twitter
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret, pathInput, pathOutput) = args.take(6)

    //leggo il nome del file ed estrapolo gli hashtag da ricercare
    var filters = readFile(pathAbsolute+pathInput)
    //filters.foreach(println)


    //crea il contesto di streaming con un intervallo di 15 secondi
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //crea la variabile di configurazione della richiesta popolandola con le chiavi di accesso
    val confBuild = new ConfigurationBuilder
    confBuild.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerKeySecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)
    //crea struttura di autenticazione
    val authorization = new OAuthAuthorization(confBuild.build)
    //crea lo stream per scaricare i tweet applicando o meno un filtro
    val tweetsDownload = if (filters.length > 0) TwitterUtils.createStream(ssc, Some(authorization), filters) else TwitterUtils.createStream(ssc, Some(authorization))
    //crea un rdd dove ad ogni tweet è associato un oggetto contenente le sue info

    val spark = SparkSession
      .builder
      .appName("twitter trying")
      .getOrCreate()

    val tweetEdit=tweetsDownload.map(t => (t, if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText)) //coppie (t._1, t._2) formate dall'intero tweet (_1) e il suo testo (_2)
      .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
      .map(t => TweetStruc.tweetStuct(t._1.getId, t._2, t._1.getUser.getScreenName, t._1.getCreatedAt.toInstant.toString, t._1.getLang)) //crea la struttura del tweet

    tweetEdit.foreachRDD { rdd =>
      import spark.implicits._
      val dataFrame = rdd.toDF("id", "text", "sentiment", "hashtags", "userMentioned", "user", "createAt", "language")
      val countTweet = dataFrame.count()
      println("\n\n\n\nNumero Tweet " + countTweet + "\n\n\n")
    }

    tweetEdit.foreachRDD { rdd => rdd.saveAsTextFile(pathAbsolute+pathOutput) } //salva su file i tweet


    //avvia lo stream e la computazione dei tweet
    ssc.start()
    //setta il tempo di esecuzione altrimenti scaricherebbe tweet all'infinito
    ssc.awaitTerminationOrTimeout(21000) //1 min
    //ssc.awaitTerminationOrTimeout(300000) //5 min
  }


  def readFile(filename: String): Array[String] = {
    val bufferedSource = Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toArray
    bufferedSource.close
    lines
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
*/

/*
tweetsDownload.map(t => (t, if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText))//coppie (t._1, t._2) formate dall'intero tweet (_1) e il suo testo (_2)
  .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
  .map(t => TweetStruc.tweetStuctString(t._1.getId, t._2, t._1.getUser.getScreenName, t._1.getCreatedAt.toInstant.toString, t._1.getLang)) //crea la struttura del tweet
  .foreachRDD { rdd => rdd.saveAsTextFile("OUT/tweets") } //salva su file i tweet

*/

//todo tweet in risposta senza hashtag vengono comunque presi