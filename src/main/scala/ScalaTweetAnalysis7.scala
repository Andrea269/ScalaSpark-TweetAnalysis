import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object ScalaTweetAnalysis7 {
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
    * @param args consumerKey consumerKeySecret accessToken accessTokenSecret filter [Optional]
    */
  def downloadTweet(sparkConf: SparkConf, args: Array[String]): Unit = {
    //leggo dai parametri passati dall'utente le 4 chiavi twitter
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret) = args.take(4)
    //leggo dai parametri passati dall'utente i filtri da applicare al downloaddei tweet
    val filters = args.takeRight(args.length - 4)
    //crea il contesto di streaming con un intervallo di 15 secondi
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //crea la variabile di configurazione della richiesta popolandola con le chiavi di accesso
    val confBuild = new ConfigurationBuilder
    confBuild.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerKeySecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)
    //crea struttura di autenticazione
    val authorization = new OAuthAuthorization(confBuild.build)
    //crea lo stream per scaricare i tweet applicando o meno un filtro
    val tweetsDownload = if (args.length != 4) TwitterUtils.createStream(ssc, Some(authorization), filters) else TwitterUtils.createStream(ssc, Some(authorization))
    //crea un rdd dove ad ogni tweet è associato un oggetto contenente le sue info
    tweetsDownload.foreachRDD { rdd =>
      rdd.map(t => (
        t, //tweet
        if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText // testo del tweet
      ))
        .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
        .map(t => TweetStruc.tweetStuct(t._1.getId, t._2, t._1.getUser.getScreenName, t._1.getCreatedAt.toInstant.toString, t._1.getLang)) //crea la struttura del tweet
        .saveAsTextFile("OUT/tweets") //salva su file i tweet
      //        .persist()
    }
    //avvia lo stream e la computazione dei tweet
    ssc.start()
    //setta il tempo di esecuzione altrimenti scaricherebbe tweet all'infinito
    ssc.awaitTerminationOrTimeout(30000) //1 min
    //            ssc.awaitTerminationOrTimeout(300000) //5 min
  }
}