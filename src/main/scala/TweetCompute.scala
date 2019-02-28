import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import scala.collection.convert.wrapAll._

object TweetCompute {
  /**
    *
    * @param textT
    */
  def TweetComputeSample(textT: String): (Array[String], Int) = {
    val sentimentTweet: Int = if (textT != null && textT != " ") computesSentiment(cleanText(textT.toString)) else 2 //calcola il sentimento del testo del tweet
    (extractHashtags(textT.toString), sentimentTweet)
  }

  /**
    *
    * @param input
    * @return
    */
  private def extractHashtags(input: String): Array[String] = {
    var hashtag = if (input == null) Array(" ") else
      input.split("\\s+") //(' ')|('\n')|('\t')|('\r')
        .filter(p => p(0).toString.equals("#") && p.length > 1) //seleziona gli hashtag
    for (i <- hashtag.indices) {
      hashtag(i) = "#" + hashtag(i).split("[^\\w']+")(1) //accetta solo lettere e numeri + l'apostrofo
    }
    hashtag
  }

  /**
    * elimina link, hashtag e utenti menzionati
    *
    * @param input
    * @return
    */
  private def cleanText(input: String): String = {
    if (input == null) " " else
      input.split("\\s+") //(' ')|('\n')|('\t')|('\r')
        .filterNot(p => p.length > 4 && p.take(4).toString.equals("http"))
        //          .filterNot(p => p.length>1 && p(0).toString.equals("#"))
        //          .filterNot(p => p.length>1 && p(0).toString.equals("@"))
        .foldLeft("")((x, y) => x + " " + y)
  }


  def computesSentiment(input: String): Int = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

    /**
      *
      * @param input
      * @return
      */
    def ExecutionComputesSentiment(input: String): Int = Option(input) match {
      case Some(text) if !text.isEmpty => extractSentiment(text)
      case _ => throw new IllegalArgumentException("input can't be null or empty")
    }

    /**
      *
      * @param text
      * @return
      */
    def extractSentiment(text: String): Int = {
      val (_, sentiment) = extractSentiments(text)
        .maxBy { case (sentence, _) => sentence.length }
      sentiment
    }

    /**
      *
      * @param text
      * @return
      */
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
