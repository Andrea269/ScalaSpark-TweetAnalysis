import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

import Sentiment.Sentiment
import scala.collection.convert.wrapAll._


object TweetStruc {
  private var tweet: TweetClass = null

  /**
    *
    * @return object tweet
    */
  def getTweet: TweetClass= tweet

  /**
    *
    * @param idT
    * @param textT
    * @param userT
    * @param createdT
    * @return
    */
  def tweetStuct(idT: Long, textT: String, userT: String, createdT: String): String = {
    tweet = new TweetClass(idT, textT, userT, createdT)
    this.toString("Tweet:")
  }

  /**
    *
    * @return
    */
  override def toString: String = tweet.toString

  /**
    *
    * @param heading
    * @return
    */
  def toString(heading: String): String = heading+this.toString

  /**
    *
    * @param idT
    * @param textT
    * @param userT
    * @param createdT
    */
  class TweetClass(idT: Long, textT: String, userT: String, createdT: String) {
    private val id: Long = idT

    private val textTweet: String = textT.toString
println(textTweet)
    private val sentimentTweet: String = textTweet
//    private val sentimentTweet: Sentiment = computesSentiment(textTweet)

    //calcola il sentimento del testo del tweet
    private val hashtags: Array[String] = extractHashtags(textT.toString)
    //estrae gli hashtag del testo del tweet
    private val user: String = userT.toString
    private val created_at: String = createdT.toString

    def getId:Long=id
    def getText:String=textTweet

    def getSentiment:String=sentimentTweet
//    def getSentiment:Sentiment=sentimentTweet

    def getHashtags:Array[String]=hashtags
    def getUser:String=user
    def getCreated_at:String=created_at

    //sentiment
    private val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
    private val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

    /**
      *
      * @param input
      * @return
      */
    private def computesSentiment(input: String): Sentiment = Option(input) match {
      case Some(text) if !text.isEmpty => extractSentiment(text)
      case _ => throw new IllegalArgumentException("input can't be null or empty")
    }

    /**
      *
      * @param text
      * @return
      */
    private def extractSentiment(text: String): Sentiment = {
      val (_, sentiment) = extractSentiments(text)
        .maxBy { case (sentence, _) => sentence.length }
      sentiment
    }

    /**
      *
      * @param text
      * @return
      */
    private def extractSentiments(text: String): List[(String, Sentiment)] = {
      val annotation: Annotation = pipeline.process(text)
      val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      sentences
        .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
        .map { case (sentence, tree) => (sentence.toString, Sentiment.toSentiment(RNNCoreAnnotations.getPredictedClass(tree))) }
        .toList
    }


    /**
      *
      * @param input
      * @return
      */
    private def extractHashtags(input: String): Array[String] = {
      if (input == null) Array(" ") else
        input.split("\\s+") //(' ')|('\n')|('\t')|('\r')
          .filter(p => p(0).toString.equals("#") && p.length > 1) //seleziona gli hashtag
    }

    /**
      *
      * @return
      */
    override def toString: String = {
      "ID->"+id + ", Text->" + textTweet + ", Sentiment->" + sentimentTweet +
        ", Hashtag->" + hashtags.foldLeft("")((x, y) => x + " " + y) + ", User->" + user + ", Time->" + created_at
    }
  }

}