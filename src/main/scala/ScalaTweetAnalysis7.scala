
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

object ScalaTweetAnalysis7 {

  def main(args: Array[String]) {
    downloadTweet(args)

  }

  def downloadTweet(args: Array[String]): Unit ={
    if (args.length < 4) {
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret> [<filters>]")
      System.exit(1)
    }
    val Array(consumerKey, consumerKeySecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Create the context with a 5 second batch size
    val sparkConf = new SparkConf()
    sparkConf.setAppName("ScalaTweetAnalysis7").setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val confBuild= new ConfigurationBuilder
    confBuild.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerKeySecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)
    val authorization=new OAuthAuthorization(confBuild.build)
    val tweetsDownload= TwitterUtils.createStream(ssc, Some(authorization))
    val filterTweetsLan= tweetsDownload.filter(_.getLang() == "en")
    filterTweetsLan.saveAsTextFiles("OUT/tweets", "json")
    ssc.start()
    ssc.awaitTerminationOrTimeout(24000)
  }



}