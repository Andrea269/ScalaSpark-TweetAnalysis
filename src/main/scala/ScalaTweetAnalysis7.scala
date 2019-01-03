
import java.util.Properties

import Sentiment.Sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.collection.convert.wrapAll._

object ScalaTweetAnalysis7 {
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret> [<filters>]")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
    sparkConf.setAppName("ScalaTweetAnalysis7").setMaster("local[3]")

    downloadTweet(sparkConf, args)
  }

  def downloadTweet(sparkConf: SparkConf, args: Array[String]): Unit = {
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    filters.foreach(t => ("#" + t, "@" + t))

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

//    filterTweetsLan.saveAsTextFiles("OUT/tweets", "json")

//    filterTweetsLan.persist()

    filterTweetsLan.foreachRDD { rdd => //crea rdd con triple formate da id del tweet, sentimento e mappa con le sue info
      rdd.map(t => (t.getId,
        Map(
          "text" -> t.getText, // parte del testo dei tweet viene troncato
          "user" -> t.getUser.getScreenName,
          "created_at" -> t.getCreatedAt.toInstant.toString,
          "location" -> Option(t.getGeoLocation).map(geo => {s"${geo.getLatitude},${geo.getLongitude}"}),
          "retweet" -> t.getRetweetCount,
          "hashtags" -> t.getHashtagEntities.map(_.getText) //if vuoto modificare


        )
      ))
        .groupByKey().map(t => (t._1, t._2.reduce((x, y) => x))) //elimina ripetizione tweet
        .map(t=> (t._1, computesSentiment(t._2.get("text").toString), t._2 ) )//calcola e aggiunge alla struttura il sentimento del testo del tweet
        .saveAsTextFile("OUT/tweets")//salva su file i tweet
//        .persist()
    }


    //origin
//        filterTweetsLan.foreachRDD{rdd =>
//          rdd.map(t => {
//            Map(
//              "id"-> t.getId,
//              "user"-> t.getUser.getScreenName,
//              "created_at" -> t.getCreatedAt.toInstant.toString,
//              "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
//              "text" -> t.getText,
//              "sentiment" -> computesSentiment(t.getText),
//              "hashtags" -> t.getHashtagEntities.map(_.getText),//if vuoto modificare
//              "retweet" -> t.getRetweetCount
//            )
//          })
//            .saveAsTextFile("OUT/tweets")
//          //        .persist()
//        }

    //avvia lo stream e la computazione dei tweet
    ssc.start()

    //setta il tempo di esecuzione altrimenti scaricherebbe tweet all'infinito
    ssc.awaitTerminationOrTimeout(60000)
//    ssc.awaitTerminationOrTimeout(120000) //2 min
  }

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  def computesSentiment(input: String): Sentiment = Option(input) match {
    case Some(text) if !text.isEmpty => extractSentiment(text)
    case _ => throw new IllegalArgumentException("input can't be null or empty")
  }

  private def extractSentiment(text: String): Sentiment = {
    val (_, sentiment) = extractSentiments(text)
      .maxBy { case (sentence, _) => sentence.length }
    sentiment
  }

  def extractSentiments(text: String): List[(String, Sentiment)] = {
    val annotation: Annotation = pipeline.process(text)
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
    sentences
      .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
      .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }
}