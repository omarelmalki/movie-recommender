package app.recommender.collaborativeFiltering


import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model: MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // (user_id, title_id, old_rating, rating, timestamp)
    val ratings = ratingsRDD.map(x => Rating(x._1, x._2, x._4))

    // Rating prediction model
    model = new ALS()
      .setRank(rank)
      .setIterations(maxIterations)
      .setLambda(regularizationParameter)
      .setBlocks(n_parallel)
      .setSeed(seed)
      .run(ratings)
  }

  def predict(userId: Int, movieId: Int): Double = {
    model.predict(userId, movieId) // predicted rating
  }


}
