package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index) // lookup table for nearest neighbors
  private val collaborativePredictor =
    new CollaborativeFiltering(10, 0.1, 0, 4) // parameters for collaborative filtering
  collaborativePredictor.init(ratings) // initialize the predictor

  private val baselinePredictor = new BaselinePredictor() // baseline predictor
  baselinePredictor.init(ratings) // initialize the predictor

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {

    val genres = nn_lookup.lookup(sc.parallelize(List(genre))).collect() // (genre)

    val moviesAlreadySeen = ratings.filter(x => x._1 == userId).map(x => x._2).collect().toSet // (title_id)

     genres // (genre, List((hash, title_id, genre)))
       .flatMap(x => x._2) // (title_id, title, genre)
       .filter(x => !moviesAlreadySeen.contains(x._1)) // (title_id, title, genre)
       .map(x => (x._1, baselinePredictor.predict(userId, x._1))) // (title_id, rating)
       .sortBy(x => -x._2) // (title_id, rating)
       .take(K).toList
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val genres = nn_lookup.lookup(sc.parallelize(List(genre))).collect() // (genre)

    val moviesAlreadySeen = ratings.filter(x => x._1 == userId).map(x => x._2).collect().toSet // (title_id)

     genres // (genre, List((hash, title_id, genre)))
      .flatMap(x => x._2) // (hash, title_id, genre)
      .filter(x => !moviesAlreadySeen.contains(x._1)) // (title_id, title, genre)
      .map(x => (x._1, collaborativePredictor.predict(userId, x._1))) // (title_id, rating)
      .sortBy(x => -x._2) // (title_id, rating)
      .take(K).toList
  }
}
