object Sentiment extends Enumeration {
  type Sentiment = Value
  val POSITIVE, NEGATIVE, VERY_POSITIVE, VERY_NEGATIVE, NEUTRAL = Value

  /**
    *
    * @param sentiment
    * @return
    */
  def toSentiment(sentiment: Int): Sentiment = sentiment match {
    case x if x == 0 => Sentiment.VERY_NEGATIVE
    case x if x == 1 => Sentiment.NEGATIVE
    case 2 => Sentiment.NEUTRAL
    case x if x == 3 => Sentiment.POSITIVE
    case x if x == 4 => Sentiment.VERY_POSITIVE
  }
}