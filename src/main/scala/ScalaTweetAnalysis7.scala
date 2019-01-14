import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object ScalaTweetAnalysis7 {
  /**
    * I parametri passati devono essere separati da una tabulazione
    * È obbligatorio passare le 4 chiavi di accesso per far eseguire l'applicativo
    * consumerKey
    * consumerKeySecret
    * accessToken
    * accessTokenSecret
    *
    * Dopo aver inserito le chiavi è possibile passare anche un insieme di stringhe
    * rappresentanti i filtri da applicare al download dei tweet
    *
    * @param args
    */
  def main(args: Array[String]) {

    //controlla che almeno le 4 chiavi di accesso siano state passate come parametro al programma
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret> [<filters>]")
      System.exit(1)
    }

    //configura spark
    val sparkConf = new SparkConf()
    sparkConf.setAppName("ScalaTweetAnalysis7").setMaster("local[*]")

    //avvia il download e il salvataggio dei tweet

        downloadTweet(sparkConf, args)
//    println(TweetStruc.tweetStuct(12345,"ciao bella gente #ci #ff #e", "c", "sad"))
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

    //crea struttura di autenticazione
    val authorization = new OAuthAuthorization(confBuild.build)

    //crea lo stream per scaricare i tweet
    val tweetsDownload = TwitterUtils.createStream(ssc, Some(authorization), filters)
    //filtra solo i tweet in lingua inglese
    val filterTweetsLan = tweetsDownload.filter(_.getLang() == "en")

    filterTweetsLan.saveAsTextFiles("OUT/ALTROtweets")

    filterTweetsLan.foreachRDD { rdd => //crea rdd con coppie formate da id del tweet e una mappa con le sue info
      rdd.map(t => (
        t.getId,//id del tweet
        (
          //testo del tweet condizione necessaria poichè se retweet il testo viene troncato
          if (t.getRetweetedStatus != null) t.getRetweetedStatus.getText else t.getText,//t._2._1
          t.getUser.getScreenName,//t._2._2
          t.getCreatedAt.toInstant.toString//t._2._3
//          ,Option(t.getGeoLocation).map(geo => {s"${geo.getLatitude},${geo.getLongitude}"})//t._2._4
//          ,t.getRetweetCount//t._2._5
        )
      ))
        .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
//        .map(t => (t._1,
//
//        Map(
//          "sentiment" -> computesSentiment(t._2._1.toString),//calcola e aggiunge alla struttura il sentimento del testo del tweet
//          "text" -> t._2._1.toString,
//          "hashtags" -> extractHashtags(t._2._1.toString),//estrae e aggiunge alla struttura gli hashtag del testo del tweet
//          "user" -> t._2._2,
//          "created_at" -> t._2._3
////          ,"location" -> t._2._4
////          ,"retweet" -> t._2._5
//        )
//      ))
        .map(t => TweetStruc.tweetStuct(t._1, t._2._1, t._2._2, t._2._3))
        .saveAsTextFile("OUT/tweets") //salva su file i tweet
      //        .persist()
    }

    //avvia lo stream e la computazione dei tweet
    ssc.start()

    //setta il tempo di esecuzione altrimenti scaricherebbe tweet all'infinito
    ssc.awaitTerminationOrTimeout(35000)
//        ssc.awaitTerminationOrTimeout(300000) //2 min
  }
}

//todo libreria grafica

//todo computazione cumulativa aggiornata ad ogni download
//todo count