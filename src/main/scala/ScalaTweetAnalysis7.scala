import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import twitter4j.Status
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import java.io._

import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.collection.mutable.Map



object ScalaTweetAnalysis7 {
  var hashtagCounterMap = scala.collection.immutable.Map[String, Int]()
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

    val filters = args.takeRight(args.length - 4)

    //leggo dai parametri passati dall'utente le 4 chiavi twitter
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret) = args.take(4)
    //leggo dai parametri passati dall'utente i filtri da applicare al downloaddei tweet

    //crea il contesto di streaming con un intervallo di 15 secondi
    val ssc = new StreamingContext(sparkConf, Seconds(15))
    //crea la variabile di configurazione della richiesta popolandola con le chiavi di accesso
    val confBuild = new ConfigurationBuilder
    confBuild.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerKeySecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

    val authorization: OAuthAuthorization = new OAuthAuthorization(confBuild.build)//crea struttura di autenticazione

    val tweetsDownload = TwitterUtils.createStream(ssc, Some(authorization), filters) //crea lo stream per scaricare i tweet

    val spark = SparkSession
      .builder
      .appName("twitter trying")
      .getOrCreate()

      //avvia il download e il salvataggio dei tweet
      downloadTweet(sparkConf, args, filters, authorization, ssc, tweetsDownload)

    println(hashtagCounterMap)

  }

  /**
    *
    * @param sparkConf
    * @param args
    */
  def downloadTweet(sparkConf: SparkConf, args: Array[String], filters: Array[String], authorization: OAuthAuthorization, ssc: StreamingContext, tweetsDownload: ReceiverInputDStream[Status]): Unit = {

    /*
    val data = tweetsDownload.map {status =>
      //val places = status.getPlace
      val id = status.getUser.getId
      val date = status.getUser.getCreatedAt.toString()
      val user = status.getUser.getName()
      //val place = places.getCountry()

      //(id,date,user,place)
      (id,date,user)
    }
    */
      val data = tweetsDownload.map {status =>

        val text = status.getText
        println("inizio\n" + text + "\nfine")

        val hashtags = extractHashtags(status.getText)
      for (a <- hashtags) {
          hashtagCounterMap += a -> (hashtagCounterMap.getOrElse(a, 0) + 1)
      }

        hashtagCounterMap.foreach(t => {

        })

      (hashtags, text)
    }


    val spark = SparkSession
      .builder
      .appName("twitter trying")
      .getOrCreate()

    /*
    data.foreachRDD{rdd =>
      import spark.implicits._
      val int = rdd.toDF("text", "hashtags").count()
      println("daje counta " + int)
    }
*/
    //promemoria: se si mettono più di un foreachRDD la map giustamente la fa più volte e quindi ad esempio si sballa il numero di tweets con determinati hashtag
    //si può risovlere ad esempio mettendo di fare il conto solo una volta (usando un bool per controllare se il conto è stato fatto)
    //quindi la prima volta che si fa il map fa anche il conto e riempie la map che conta i twitter, se servono altri foreachRDD, entrrenanno nella data.map ma senza aggiornare la map con gli hashtag
    data.foreachRDD{rdd =>
     rdd.collect().foreach(println)
    }


    /*// Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
    .readStream
    .option("sep", ";")
    .schema(userSchema)      // Specify schema of the csv files
    .csv("/path/to/directory")    // Equivalent to format("csv").load("/path/to/directory")
    */

   // 1 to 6 foreach
  //  { _ =>
      ssc.start()//avvia lo stream e la computazione dei tweet
      //setta il tempo di esecuzione altrimenti scaricherebbe tweet all'infinito
      ssc.awaitTerminationOrTimeout(30000)
      //        ssc.awaitTerminationOrTimeout(120000) //2 min
  //  }

    //for ((k,v) <- hashtagCounterMap) println(s"key: $k, value: $v")
  }

  private def extractHashtags(input: String): Array[String] = {
    extractText(input, "#")
  }

  private def extractText(input: String, heading: String): Array[String] = {
    if (input == null) Array(" ") else
      input.split("\\s+") //(' ')|('\n')|('\t')|('\r')
        .filter(p => p(0).toString.equals(heading) && p.length > 1) //seleziona gli hashtag
  }

}

//todo libreria grafica