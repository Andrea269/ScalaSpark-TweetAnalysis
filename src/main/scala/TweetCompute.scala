/**
  * Fasano Domenico & Pascali Andrea - University of Bologna
  * Project for "Scala and Cloud Programming"
  * Tweet Analysis - Creation of a graph of correlated hashtags given a hashtag
  *
  * The object contains a public definition, TweetComputeSample, that, given a a string, extracts the hashtags and the sentiment of that string.
  * The sentiment is calculated using the StanfordCoreNLP library
  */

import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.convert.wrapAll._

object TweetCompute {
  /**
    * Given a a string, it extracts the hashtags and the sentiment of that string.
    * Also print on the console the text under computation to have a feedback of the streaming of tweets during the execution.
    *
    * @param textT: A string containing english language and hashtags
    * @return A tuple made of 1) an array made of hashtags found in the parameter; 2)a value from 1 to 5 that represents the sentiment according to the following enumeration:
    *         1: Very Negative
    *         2: Negative
    *         3: Neutral
    *         4: Positive
    *         5: Very positive
    *
    */
  def TweetComputeSample(textT: String): (Array[String], Int) = {
    val sentimentTweet: Int = if (textT != null && textT != " ") computesSentiment(cleanText(textT.toString)) else 3 //To an empty string the neutral sentiment value is assigned
    println(textT) //Print the text of the tweet under computation
    (extractHashtags(textT.toString), sentimentTweet)
  }

  /**
    * It recognize the hastags that appears in a tweet
    *
    * @param input A string containing english language and hashtags
    * @return hashtag: An array of the hashtags found in the in the parameter
    */
  private def extractHashtags(input: String): Array[String] = {
    var hashtag = if (input == null) Array(" ") else
      input.split("\\s+") //the regex splits when found the following symbols(' ')|('\n')|('\t')|('\r')
        .filter(p => p(0).toString.equals("#") && p.length > 1) //extract hashtags
    for (i <- hashtag.indices) {
      hashtag(i) = "#" + hashtag(i).split("[^\\w']+")(1) //accept only letters, numbers and the apostrophe
    }
    hashtag
  }

  /**
    * It's useful for improving the sentiment analysis of a string, to delete the "noise" in that string
    *
    * @param input A string containing english language and hashtags
    * @return The same parameter string but without eventual links, hashtags and mentioned users
    */
  private def cleanText(input: String): String = {
    if (input == null) " " else
      input.split("\\s+") //the regex splits when found the following symbols(' ')|('\n')|('\t')|('\r')
        .filterNot(p => p.length > 4 && p.take(4).toString.equals("http"))
        .filterNot(p => p.length>1 && p(0).toString.equals("@"))
        .foldLeft("")((x, y) => x + " " + y)
  }

  /**
    * Use the StanfordCoreNLP library to extract the sentiment value
    *
    * @param input A string containing english language and hashtags
    * @return
    */

  def computesSentiment(input: String): Int = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

    def ExecutionComputesSentiment(input: String): Int = Option(input) match {
      case Some(text) if !text.isEmpty => extractSentiment(text)
      case _ => throw new IllegalArgumentException("input can't be null or empty")
    }

    def extractSentiment(text: String): Int = {
      val (_, sentiment) = extractSentiments(text)
        .maxBy { case (sentence, _) => sentence.length }
      sentiment
    }

    def extractSentiments(text: String): List[(String, Int)] = {
      val annotation: Annotation = pipeline.process(text)
      val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
      sentences
        .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
        .map { case (sentence, tree) => (sentence.toString, RNNCoreAnnotations.getPredictedClass(tree)) }
        .toList
    }

    ExecutionComputesSentiment(input)
  }
}
