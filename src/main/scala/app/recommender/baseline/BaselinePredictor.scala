package app.recommender.baseline

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

class BaselinePredictor() extends Serializable {

  private var globalAvgRating: Double = 0.0 // global_avg_rating
  private var userAvgRating: scala.collection.Map[Int, Double] = null // (user_id, avg_rating)
  private var avgMovieNormalizedDeviation: scala.collection.Map[Int, Double] = null // (title_id, avg_normalized_deviation)

  private def scale(x: Double, avg: Double): Double = {
    if (x > avg) 5.0 - avg
    else if (x < avg) avg - 1.0
    else 1.0
  }

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // (user_id, title_id, old_rating, rating, timestamp)

    globalAvgRating = ratingsRDD.map(x => x._4).sum() / ratingsRDD.count() // global_avg_rating
    val avgUserRating = ratingsRDD.map(x => (x._1, x._4)).groupByKey().map(x => (x._1, x._2.sum / x._2.size))
      .persist() // (user_id, avg_rating)
    avgMovieNormalizedDeviation = ratingsRDD.map(x => (x._1, (x._2, x._4)))  // (user_id, (title_id, rating))
      .join(avgUserRating).map(x => (x._2._1._1, (x._2._1._2, x._2._2))) // (title_id, (rating, user_avg_rating))
      .map(x => (x._1, (x._2._1 - x._2._2) / scale(x._2._1, x._2._2))) // (title_id, normalized_deviation)
      .groupByKey().map(x => (x._1, x._2.sum / x._2.size)) // (title_id, avg_normalized_deviation)
      .collectAsMap() // (title_id, avg_normalized_deviation)
    userAvgRating = avgUserRating.collectAsMap()
    avgUserRating.unpersist()
  }

  def predict(userId: Int, movieId: Int): Double = {
    val userRating = userAvgRating.get(userId) // user_avg_rating
    if (userRating.isEmpty) return globalAvgRating // user has no ratings
    val movieAvgNormalizedDeviation = avgMovieNormalizedDeviation.get(movieId) // avg_normalized_deviation
    if (movieAvgNormalizedDeviation.isEmpty) return userRating.get // movie has no ratings

    userRating.get + movieAvgNormalizedDeviation.get * scale(userRating.get + movieAvgNormalizedDeviation.get, userRating.get)  // predicted_rating
  }
}
