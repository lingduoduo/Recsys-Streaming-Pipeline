package com.demo.util

/** Pure 3-level movie category derivations, shared by the movie-category report.
  *
  * Scala port of `services/python-modeling/movie_categories.py`. From a movie's genres
  * (comma-joined string, as stored in Redis `movie:{id}:features`) + release year:
  *   l1 = genre family (broad)            e.g. "SciFi&Fantasy"
  *   l2 = primary (first) genre           e.g. "Sci-Fi"
  *   l3 = primary genre x release decade  e.g. "Sci-Fi·2010s"
  */
object MovieCategories {

  /** l1 genre family. */
  val GenreFamily: Map[String, String] = Map(
    "Action" -> "Action&Adventure", "Adventure" -> "Action&Adventure",
    "War" -> "Action&Adventure", "Western" -> "Action&Adventure",
    "Sci-Fi" -> "SciFi&Fantasy", "Fantasy" -> "SciFi&Fantasy", "Animation" -> "SciFi&Fantasy",
    "Drama" -> "Drama&Romance", "Romance" -> "Drama&Romance", "Musical" -> "Drama&Romance",
    "Comedy" -> "Comedy", "Children" -> "Comedy",
    "Crime" -> "Crime&Thriller", "Thriller" -> "Crime&Thriller", "Mystery" -> "Crime&Thriller",
    "Film-Noir" -> "Crime&Thriller", "Horror" -> "Crime&Thriller",
    "Documentary" -> "Other"
  )

  private def asList(genres: String): Seq[String] =
    Option(genres).toSeq.flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty)

  def primaryGenre(genres: String): String = asList(genres).headOption.getOrElse("unknown")

  def familyOf(genre: String): String = GenreFamily.getOrElse(genre, "Other")

  def decade(year: String): String =
    scala.util.Try(year.trim.toInt).toOption.map(y => s"${(y / 10) * 10}s").getOrElse("unknown")

  def l1(genres: String): String = familyOf(primaryGenre(genres))

  def l2(genres: String): String = primaryGenre(genres)

  def l3(genres: String, year: String): String = s"${primaryGenre(genres)}·${decade(year)}"
}
