import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object ScalaTweetAnalysis7 {
  /**
    *
    * @param args
      * consumerKey
      * consumerKeySecret
      * accessToken
      * accessTokenSecret
      * filter [Optional]
    */
  def main(args: Array[String]) {
    if (args.length < 4) {//controlla che almeno le 4 chiavi di accesso siano state passate come parametro al programma
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret> [<filters>]")
      System.exit(1)
    }
    val sparkConf = new SparkConf()//configura spark
    sparkConf.setAppName("ScalaTweetAnalysis7").setMaster("local[*]")

    //avvia il download e il salvataggio dei tweet
    downloadTweet(sparkConf, args)
//        println(TweetStruc.tweetStuct(12345,"ciao bella gente #ci #ff #e", "c", "sad"))
  }

  /**
    *
    * @param sparkConf
    * @param args
    */
  def downloadTweet(sparkConf: SparkConf, args: Array[String]): Unit = {
    //leggo dai parametri passati dall'utente le 4 chiavi twitter
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret) = args.take(4)
    //leggo dai parametri passati dall'utente i filtri da applicare al downloaddei tweet
    val filters = args.takeRight(args.length - 4)
    //crea il contesto di streaming con un intervallo di 15 secondi
    val ssc = new StreamingContext(sparkConf, Seconds(15))
    //crea la variabile di configurazione della richiesta popolandola con le chiavi di accesso
    val confBuild = new ConfigurationBuilder
    confBuild.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerKeySecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

    val authorization = new OAuthAuthorization(confBuild.build)//crea struttura di autenticazione
    val tweetsDownload = TwitterUtils.createStream(ssc, Some(authorization), filters) //crea lo stream per scaricare i tweet
    val filterTweetsLan = tweetsDownload.filter(_.getLang() == "en") //filtra solo i tweet in lingua inglese todo

    //    filterTwee/tsLan.saveAsTextFiles("OUT/ALTROtweets")

    filterTweetsLan.foreachRDD { rdd => //crea rdd con coppie formate da id del tweet e una mappa con le sue info
      rdd.map(t => (
        t, //id del tweet
        if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText //t._2
      ))
        .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
        .map(t => TweetStruc.tweetStuct(t._1.getId, t._2, t._1.getUser.getScreenName, t._1.getCreatedAt.toInstant.toString))
        .saveAsTextFile("OUT/tweets") //salva su file i tweet
      //        .persist()
    }

    ssc.start()//avvia lo stream e la computazione dei tweet
    //setta il tempo di esecuzione altrimenti scaricherebbe tweet all'infinito
    ssc.awaitTerminationOrTimeout(35000)
    //        ssc.awaitTerminationOrTimeout(300000) //2 min
  }
}

//todo libreria grafica