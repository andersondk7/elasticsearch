package adc.tutorial.elasticsearch.model

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.sksamuel.elastic4s.{Hit, HitReader, Indexable}
import com.sksamuel.elastic4s.http.search.SearchHit
import play.api.libs.functional.syntax._
import play.api.libs.json._

/**
  * Represents movie data
  */
case class Movie(id: Long
                 , imdbId: String = ""
                 , title: String = ""
                 , language: String = ""
                 , homepage: String = ""
                 , overview: String = ""
                 , popularity: Double = 0.0
                 , releaseDate: LocalDate = LocalDate.of(1999, 9, 9)
                 , revenue: Long = 0
                 , runtime: Int = 0
                 , tagLine: String = ""
                 , productionCompanies: List[String] = List()
                 , voteAvg: Double = 0.0
                 , voteCount: Int = 0
                 , genres: List[String] = List()) {

}

object Movie {
  val ymdFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def fromMovieDbJson(js: JsValue) = {
    Movie(
      id = js("id").as[Long]
      , imdbId = js("imdb_id").as[String]
      , title = js("original_title").as[String]
      , language = js("original_language").as[String]
      , homepage = js("homepage").as[String]
      , overview = js("overview").as[String]
      , popularity = js("popularity").as[Double]
      , releaseDate = LocalDate.parse(js("release_date").as[String], ymdFormatter)
      , revenue = js("revenue").as[Long]
      , runtime = js("runtime").as[Int]
      , tagLine = js("tagline").as[String]
      , productionCompanies = (js \ "production_companies" \\ "name").map(_.as[String]).toList
      , voteAvg = js("vote_average").as[Double]
      , voteCount = js("vote_count").as[Int]
      , genres = (js \ "genres" \\ "name").map(_.as[String]).toList
    )
  }

  val idKey = "movie_id"
  val imdbKey = "imdb_id"
  val titleKey = "title"
  val languageKey = "language"
  val homepageKey = "homepage"
  val overviewKey = "overview"
  val popularityKey = "popularity"
  val releaseDateKey = "release_date"
  val revenueKey = "revenue"
  val runtimeKey = "runtime"
  val tagLineKey = "tag_line"
  val productionCompaniesKey = "productionCompanies"
  val voteAverageKey = "vote_average"
  val voteCountKey = "vote_count"
  val genresKey = "genres"

  implicit val reader: Reads[Movie] = (
    (JsPath \ idKey).read[Long] and
      (JsPath \ imdbKey).read[String] and
      (JsPath \ titleKey).read[String] and
      (JsPath \ languageKey).read[String] and
      (JsPath \ homepageKey).read[String] and
      (JsPath \ overviewKey).read[String] and
      (JsPath \ popularityKey).read[Double] and
      (JsPath \ releaseDateKey).read[String] and
      (JsPath \ revenueKey).read[Long] and
      (JsPath \ runtimeKey).read[Int] and
      (JsPath \ tagLineKey).read[String] and
      (JsPath \ productionCompaniesKey).read[List[String]] and
      (JsPath \ voteAverageKey).read[Double] and
      (JsPath \ voteCountKey).read[Int] and
      (JsPath \ genresKey).read[List[String]]
    ) ((id, imdb, title, language, homepage, overview, popularity, release, revenue, runtime, tag, production, va, vc, g)
  =>
    Movie(id = id
      , imdbId = imdb
      , title = title
      , language = language
      , homepage = homepage
      , overview = overview
      , popularity = popularity
      , releaseDate = LocalDate.parse(release, ymdFormatter)
      , revenue = revenue
      , tagLine = tag
      , runtime = runtime
      , productionCompanies = production
      , voteAvg = va
      , voteCount = vc
      , genres = g
    )
  )

  implicit def fromJson(js: JsValue): Movie = Movie(
    id = js(idKey).as[Long]
    , imdbId = js(imdbKey).as[String]
    , title = js(titleKey).as[String]
    , language = js(languageKey).as[String]
    , homepage = js(homepageKey).as[String]
    , overview = js(overviewKey).as[String]
    , popularity = js(popularityKey).as[Double]
    , releaseDate = LocalDate.parse(js(releaseDateKey).as[String], ymdFormatter)
    , revenue = js(revenueKey).as[Long]
    , tagLine = js(tagLineKey).as[String]
    , runtime = js(runtimeKey).as[Int]
    , productionCompanies = js(productionCompaniesKey).as[List[String]]
    , voteAvg = js(voteAverageKey).as[Double]
    , voteCount = js(voteCountKey).as[Int]
    , genres = js(genresKey).as[List[String]]
  )

  implicit def toJson(movie: Movie): JsValue = Json.obj(
    idKey -> movie.id
    , imdbKey -> movie.imdbId
    , titleKey -> movie.title
    , languageKey -> movie.language
    , homepageKey -> movie.homepage
    , overviewKey -> movie.overview
    , popularityKey -> movie.popularity
    , releaseDateKey -> ymdFormatter.format(movie.releaseDate)
    , revenueKey -> movie.revenue
    , runtimeKey -> movie.runtime
    , tagLineKey -> movie.tagLine
    , productionCompaniesKey -> movie.productionCompanies
    , voteAverageKey -> movie.voteAvg
    , voteCountKey -> movie.voteCount
    , genresKey -> movie.genres
  )

  implicit val writer: Writes[Movie] = new Writes[Movie] {
    def writes(movie: Movie): JsValue = Movie.toJson(movie)
  }

  implicit def fromSearchHit(sh: SearchHit): Movie = fromJson(Json.parse(sh.sourceAsString))
  implicit def fromSearchHitOption(sho: Option[SearchHit]): Option[Movie] = sho.map[Movie](h => h)

  // needed for elastic search
  implicit object MovieIndexable extends Indexable[Movie] {
    override def json(m: Movie): String = {
      val s = Movie.toJson(m).toString()
//      println(s)
      s
    }
  }

  implicit object MovieHitReader extends HitReader[Movie] {
    override def read(hit: Hit): Either[Throwable, Movie] = {
      Right(Movie.fromJson(Json.parse(hit.sourceAsString)))
    }
  }

}
