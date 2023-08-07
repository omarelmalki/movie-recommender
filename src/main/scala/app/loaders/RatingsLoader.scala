package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val file = new File(getClass.getResource(path).getFile).getPath

    sc.textFile(file)
      .map(x => x.split('|'))
      .map(x => if (x.length >= 5) (x(0).toInt, x(1).toInt, Some(x(2).toDouble), x(3).toDouble, x(4).toInt) // support files with an old rating column
      else (x(0).toInt, x(1).toInt, None, x(2).toDouble, x(3).toInt))
      .persist() // (user_id, title_id, old_rating, rating, timestamp)
  }
}