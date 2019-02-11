object TweetStruc {
  private var tweet: TweetClass = _
  var count= 0

  /**
    *
    * @return object tweet
    */
  def getTweet: TweetClass = tweet

  /**
    *
    * @param idT
    * @param textT
    * @param userT
    * @param createdT
    * @return
    */
  def tweetStuct(idT: Long, textT: String, userT: String, createdT: String, langT: String):
  (Long, String, Int, String, String, String, String, String) = {
    tweet = new TweetClass(idT, textT, userT, createdT, langT)
    (tweet.getId, tweet.getText, tweet.getSentiment, tweet.getHashtags, tweet.getUserMentioned, tweet.getUser, tweet.getCreated_at, tweet.getLanguage)
  }


  def tweetStuctString(idT: Long, textT: String, userT: String, createdT: String, langT: String):  String = {
    tweet = new TweetClass(idT, textT, userT, createdT, langT)
    tweet.toString("Tweet: ")
  }
  /**
    *
    * @param heading
    * @return
    */
  def toString(heading: String): String = tweet.toString(heading)

  /**
    *
    * @param idT
    * @param textT
    * @param userT
    * @param createdT
    */
  class TweetClass(idT: Long, textT: String, userT: String, createdT: String, langT: String) {

    import java.util.Properties

    import edu.stanford.nlp.ling.CoreAnnotations
    import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
    import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
    import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

    import scala.collection.convert.wrapAll._

    private val id: Long = idT
    private val textTweet: String = textT.toString
    private val hashtags: Array[String] = extractHashtags(textT.toString) //estrae gli hashtag del testo del tweet
    private val listUserMentioned: Array[String] = userMentioned(textT.toString)
    private val user: String = userT.toString
    private val creationDate: Map[String, String] = RefactoringDate(createdT.toString)
    private val lang: String = langT.toString

//    val sentimentTweet: Int =if(lang=="en" && textTweet!= null && textTweet!= " ") computesSentiment(cleanText(textT.toString)) else 2 //calcola il sentimento del testo del tweet
    val sentimentTweet: Int =if(textTweet!= null && textTweet!= " ") computesSentiment(cleanText(textT.toString)) else 2 //calcola il sentimento del testo del tweet
    def getId: Long = id

    def getText: String = textTweet

    def getSentiment: Int = sentimentTweet

    def getHashtags: String = " " + hashtags.foldLeft("")((x, y) => x + " " + y) + " " //mettiamo gli spazi alla fine e all'inizio per fare la query SQL like

    def getUserMentioned: String = listUserMentioned.foldLeft("")((x, y) => x + " " + y)

    def getUser: String = user

    def getCreated_at: String = creationDate.toString()

    def getLanguage: String = langT

    /**
      * Formato data: 2019-01-20T09:00:13Z
      * @param date
      * @return
      */
    def RefactoringDate(date: String):  Map[String, String] = {

      var returnDate: Map[String, String] =null
      val splitDate= date.substring(0, date.length-1).split("T")
      val splitDay= splitDate(0).split("-")
      val splitTime= splitDate(1).split(":")
      if(splitDay!=null && splitTime!=null && splitDay.length==3 && splitTime.length==3){
        returnDate=Map(
          "Year"->splitDay(0),
          "Month"->splitDay(1),
          "Day"->splitDay(2),
          "Hours"->splitTime(0),
          "Minutes"->splitTime(1),
          "Seconds"->splitTime(2)
        )
      }
      returnDate
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

    /**
      *
      * @param input
      * @return
      */
    private def extractHashtags(input: String): Array[String] = {
      extractText(input, "#")
    }

    /**
      *
      * @param input
      * @return
      */
    private def userMentioned(input: String): Array[String] = {
      extractText(input, "@")
    }


    /**
      *
      * @param input
      * @param heading
      * @return
      */
    private def extractText(input: String, heading: String): Array[String] = {
      if (input == null) Array(" ") else
        input.split("\\s+") //(' ')|('\n')|('\t')|('\r')
          .filter(p => p(0).toString.equals(heading) && p.length > 1) //seleziona gli hashtag
    }

    /**
      * elimina link, hashtag e utenti menzionati
      * @param input
      * @return
      */
    private def cleanText(input: String): String = {
      if (input == null) " " else
        input.split("\\s+") //(' ')|('\n')|('\t')|('\r')
          .filterNot(p => p.length>4 && p.take(4).toString.equals("http"))
          //          .filterNot(p => p.length>1 && p(0).toString.equals("#"))
          //          .filterNot(p => p.length>1 && p(0).toString.equals("@"))
          .foldLeft("")((x,y)=>x + " " + y)
    }

    /**
      *
      * @param heading
      * @return
      */
    def toString(heading: String): String = heading +
      "ID->" + id + "\n"+
      "Text->" + textTweet + "\n"+
      "Sentiment->" + sentimentTweet + "\n"+
      "Hashtag->" + hashtags.foldLeft("")((x, y) => x + " " + y) + "\n"+
      "UserMentioned->" + listUserMentioned.foldLeft("")((x, y) => x + " " + y) + "\n"+
      "User->" + user + "\n"+
      "Time->" + creationDate + "\n"+
      "Language->"+lang+"\n\n\n"
  }

}