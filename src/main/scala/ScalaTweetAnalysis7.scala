
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import java.util.Properties
import scala.collection.convert.wrapAll._
import Sentiment.Sentiment
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

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

  def downloadTweet(sparkConf: SparkConf, args: Array[String]): Unit ={
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Create the context with a 5 second batch size

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val confBuild= new ConfigurationBuilder
    confBuild.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerKeySecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)
    val authorization=new OAuthAuthorization(confBuild.build)

    val tweetsDownload= TwitterUtils.createStream(ssc, Some(authorization))
    val filterTweetsLan= tweetsDownload.filter(_.getLang() == "en")

    filterTweetsLan.foreachRDD{(rdd, time) =>
      rdd.map(t => {
        Map(
          "id"-> t.getId,
          "user"-> t.getUser.getScreenName,
          "created_at" -> t.getCreatedAt.toInstant.toString,
          "location" -> Option(t.getGeoLocation).map(geo => { s"${geo.getLatitude},${geo.getLongitude}" }),
          "text" -> t.getText,
          "hashtags" -> t.getHashtagEntities.map(_.getText),//if vuoto modificare
          "retweet" -> t.getRetweetCount
        )
      })
        .saveAsTextFile("OUT/tweets")
//        .persist()
    }
//      filterTweetsLan.saveAsTextFiles("OUT/tweets", "json")

    ssc.start()
    ssc.awaitTerminationOrTimeout(7000)
  }

  val props = new Properties()
  props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

  def mainSentiment(input: String): Sentiment = Option(input) match {
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
      .map { case (sentence, tree) => (sentence.toString,Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
      .toList
  }
}