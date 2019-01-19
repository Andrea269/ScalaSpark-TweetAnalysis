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

    val data = tweetsDownload.map {status =>
      //val places = status.getPlace
      val id = status.getUser.getId
      val date = status.getUser.getCreatedAt.toString()
      val user = status.getUser.getName()
      //val place = places.getCountry()

      //(id,date,user,place)
      (id,date,user)
    }
    val spark = SparkSession
      .builder
      .appName("twitter trying")
      .getOrCreate()

    data.foreachRDD{rdd =>
      import spark.implicits._
      val int = rdd.toDF("id","date","user","place").count()
      println(int)
    }

    data.foreachRDD{rdd =>
     rdd.collect.foreach(println)
    }


    /*// Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
    .readStream
    .option("sep", ";")
    .schema(userSchema)      // Specify schema of the csv files
    .csv("/path/to/directory")    // Equivalent to format("csv").load("/path/to/directory")
    */

    ssc.start()//avvia lo stream e la computazione dei tweet
    //setta il tempo di esecuzione altrimenti scaricherebbe tweet all'infinito
    ssc.awaitTerminationOrTimeout(10000)
    //        ssc.awaitTerminationOrTimeout(120000) //2 min
  }
}

//todo libreria grafica