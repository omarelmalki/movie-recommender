package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val file = new File(getClass.getResource(path).getFile).getPath

    sc.textFile(file)
      .map(x => x.split('|'))
      .map(x => (x(0).toInt,
        x(1).stripPrefix("\"").stripSuffix("\""),
        x.slice(2,x.length).toList.map(x => x.stripPrefix("\"").stripSuffix("\""))))
      .persist() // (title_id, title, keywords)
  }
}

