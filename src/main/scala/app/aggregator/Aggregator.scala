package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state: RDD[(Int, ((String, List[String]), (Double, Int)))]= null
  private var partitioner: HashPartitioner = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    val ratingsRDD = ratings.map(x => (x._2, (x._4, 1))) // (title_id, (rating, 1))
    val titlesRDD = title.map(x => (x._1, (x._2, x._3))) // (title_id, (title, keywords))

    val joinedRDD = titlesRDD.leftOuterJoin(ratingsRDD) // (title_id, ((title, keywords), (rating, 1)))
        .map(x => (x._1, (x._2._1, x._2._2.getOrElse((0.0, 0))))) // (title_id, ((title, keywords), (rating, 1)))

    partitioner = new HashPartitioner(joinedRDD.getNumPartitions)

    state = joinedRDD.reduceByKey(partitioner, (x, y) => (x._1, (x._2._1 + y._2._1, x._2._2 + y._2._2)))
      .partitionBy(partitioner).persist(MEMORY_AND_DISK) // (title_id, ((title, keywords), (sum_rating, count_rating)))
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = state.map(x => if (x._2._2._2 != 0) (x._2._1._1, x._2._2._1 / x._2._2._2) else (x._2._1._1, 0.0))

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    val ratingsRDD = state
      .map(x => (x._2._1._2, if (x._2._2._2 != 0) x._2._2._1 / x._2._2._2 else 0.0)) // (keywords, avg_rating)
      .filter(x => keywords.forall(x._1.contains)) // (keywords, avg_rating)
      .map(x => if (x._2 != 0.0) (x._2, 1) else (0.0, 0)) // (avg_rating, 1)
      .persist(MEMORY_AND_DISK) // (avg_rating, 1)

    if (ratingsRDD.isEmpty()) {
      ratingsRDD.unpersist() // unpersist ratingsRDD
      -1.0 // no such titles exist
    } else {
      val sum = ratingsRDD.reduce((a, b) => (a._1 + b._1, a._2 + b._2)) // sum of all avg_rating
      ratingsRDD.unpersist() // unpersist ratingsRDD
      if (sum._2 == 0){
        0.0
      } else {
        sum._1 / sum._2 // avg_rating
      }// avg_rating
    }
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val deltaRDD = sc.parallelize(delta_).map(x => (x._2, if (x._3.isEmpty) (x._4, 1) else (x._4 - x._3.get, 0))) // (title_id, (rating, 1 if new rating, 0 if old rating))
      .reduceByKey(partitioner, (x, y) => (x._1 + y._1, x._2 + y._2)) // (title_id, (new_sum_rating, new_count_rating)

    val newState = state.leftOuterJoin(deltaRDD) // (title_id, ((title, (sum_rating, count_rating)), (new_sum_rating, new_count_rating)))
      .map(x => (x._1, (x._2._1._1, (x._2._1._2._1 + x._2._2.getOrElse((0.0, 0))._1, x._2._1._2._2 + x._2._2.getOrElse((0.0, 0))._2)))) // (title_id, (title, (sum_rating + new_sum_rating, count_rating + new_count_rating)))
      .partitionBy(partitioner)
      .persist(MEMORY_AND_DISK)

    state.unpersist() // unpersist old state
    state = newState // update state
  }
}
