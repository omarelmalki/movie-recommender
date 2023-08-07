package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime

class SimpleAnalytics() extends Serializable {

  // (user_id, title_id, old_rating, rating, timestamp)
  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null
  private var titlesGroupedById: RDD[(Int, (String, List[String]))] = null
  private var ratingsGroupedByYearByTitle: RDD[(Int, Map[Int, Iterable[(Int, Int, Option[Double], Double, Int)]])] = null
  private var mostRatedMovieEachYear: RDD[(Int, (Int, (String, List[String])))] = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {

    // groupby movies by id
    val moviesById = movie.map { case (movieId, title, keywords) => (movieId, (title, keywords)) }

    // pre-process ratings RDD by grouping it first by year and then within each group,
    // further creating groups by movie ID, and then persists for future analytical operations.
    val ratingsByYear = ratings
      .map { case (userId, movieId, oldRating, rating, timestamp) =>
        (new DateTime(timestamp * 1000L).getYear, (userId, movieId, oldRating, rating, timestamp)) }
      .groupByKey()
      .map { case (year, movieRatings) => (year, movieRatings.groupBy(x => x._2)) }

    // Initialize the partitioners
    ratingsPartitioner = new HashPartitioner(ratingsByYear.getNumPartitions)
    moviesPartitioner = new HashPartitioner(moviesById.getNumPartitions)

    // You need to use a HashPartitioner to partition your processed RDDs, and then persist it in the variables titlesGroupedById and ratingsGroupedByYearByTitle
    titlesGroupedById = moviesById.partitionBy(moviesPartitioner).persist(MEMORY_AND_DISK)
    ratingsGroupedByYearByTitle = ratingsByYear.partitionBy(ratingsPartitioner).persist(MEMORY_AND_DISK)

    mostRatedMovieEachYear = ratingsGroupedByYearByTitle
      .map { case (year, movieRatings) =>
        (year, movieRatings.map { case (movieId, ratings) => (movieId, ratings.size) }
          .reduce((a, b) => if (a._2 > b._2 || (a._2 == b._2 && a._1 >= b._1)) a else b)) } // (year, (movieId, ratingCount))
      .map { case (year, (movieId, _)) => (movieId, year) } // (movieId, year)
      .join(titlesGroupedById) // (movieId, (year, (title, keywords)))
      .partitionBy(ratingsPartitioner)
      .persist(MEMORY_AND_DISK)
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    // return an RDD of pairs (year, number of ratings in that year)
    ratingsGroupedByYearByTitle
        .map { case (year, movieRatings) => (year, movieRatings.values.size) }
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    // return an RDD of pairs (year, movie name). If there is a tie for the most popular movie, return the movie with the greatest movie id only
    mostRatedMovieEachYear.map { case (movieId, (year, (title, _))) => (year, title) } // (year, title)
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    mostRatedMovieEachYear.map { case (movieId, (year, (_, genres))) => (year, genres) } // (year, genres)
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    val ratingsByGenreOccurence = mostRatedMovieEachYear
      .map { case (movieId, (year, (_, genres))) => (year, genres) } // (year, genres)
      .flatMap { case (year, genres) => genres.map(genre => (genre, year)) } // (genre, year)
      .groupByKey() // (genre, Iterable(year))
      .map { case (genre, years) => (genre, years.size) } // (genre, yearCount)
      .persist()

    val mostRatedGenreAllTime = ratingsByGenreOccurence
      .reduce((a,b) => if (a._2 > b._2 || (a._2 == b._2 && a._1.compareTo(b._1) <= 0.0)) a else b) // sort by yearCount descending, then genre ascending
    val leastRatedGenreAllTime = ratingsByGenreOccurence
      .reduce((a,b) => if (a._2 < b._2 || (a._2 == b._2 && a._1.compareTo(b._1) <= 0.0)) a else b) // sort by yearCount ascending, then genre ascending

    ratingsByGenreOccurence.unpersist()

    (leastRatedGenreAllTime, mostRatedGenreAllTime)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    // return an RDD of movie names which are in the supplied genres
    val reqGenres = requiredGenres.collect().toList
    movies
      .filter { case (movieId, title, genres) => genres.intersect(reqGenres).nonEmpty } // (movieId, title, genres)
      .map { case (movieId, title, genres) => title } // title
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    // return an RDD of movie names which are in the supplied genres
    val broadcastedGenres = broadcastCallback(requiredGenres) // Broadcast[List[String]]
    movies
      .filter { case (movieId, title, genres) => genres.intersect(broadcastedGenres.value).nonEmpty } // (movieId, title, genres)
      .map { case (movieId, title, genres) => title } // title
  }
}
