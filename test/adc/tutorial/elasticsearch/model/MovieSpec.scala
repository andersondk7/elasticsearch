package adc.tutorial.elasticsearch.model

import org.scalatest.{FunSpec, Matchers}
import play.api.libs.json.{JsValue, Json}
import java.time.LocalDate

class MovieSpec extends FunSpec with Matchers {
  import Movie._
  val movie = Movie(
    id=1
    , imdbId="imdb7"
    , title="A movie title"
    , language="en"
    , homepage = "http://movie.home.page"
    , overview="as seen from a high place"
    , releaseDate=LocalDate.of(1997, 3,12)
    , revenue=1234567
    , runtime=123
    , tagLine="some text"
    , voteCount=45600
    , genres=List( "Comedy", "Drama", "Action")
    , productionCompanies=List("Paramount", "Miramax")
    , popularity = 11.6
    , voteAvg= 6.8
  )

  val gumpJson =
    """
      |{"adult":false,"backdrop_path":"/ctOEhQiFIHWkiaYp7b0ibSTe5IL.jpg","belongs_to_collection":null,"budget":55000000,"genres":[{"id":35,"name":"Comedy"},{"id":18,"name":"Drama"},{"id":10749,"name":"Romance"}],"homepage":"","id":13,"imdb_id":"tt0109830","original_language":"en","original_title":"Forrest Gump","overview":"A man with a low IQ has accomplished great things in his life and been present during significant historic events - in each case, far exceeding what anyone imagined he could do. Yet, despite all the things he has attained, his one true love eludes him. 'Forrest Gump' is the story of a man who rose above his challenges, and who proved that determination, courage, and love are more important than ability.","popularity":10.7281,"poster_path":"/yE5d3BUhE8hCnkMUJOo1QDoOGNz.jpg","production_companies":[{"name":"Paramount Pictures","id":4}],"production_countries":[{"iso_3166_1":"US","name":"United States of America"}],"release_date":"1994-07-06","revenue":677945399,"runtime":142,"spoken_languages":[{"iso_639_1":"en","name":"English"}],"status":"Released","tagline":"The world will never be the same, once you've seen it through the eyes of Forrest Gump.","title":"Forrest Gump","video":false,"vote_average":8.199999999999999,"vote_count":7208}
    """.stripMargin

  describe("a movie") {
    it ("should read from the json from TheMovieDB.org") {
      val gump = Movie.fromMovieDbJson(Json.parse(gumpJson))
      gump.id shouldBe 13
      gump.imdbId shouldBe "tt0109830"
      gump.language shouldBe "en"
      gump.homepage shouldBe ""
      gump.overview shouldBe "A man with a low IQ has accomplished great things in his life and been present during significant historic events - in each case, far exceeding what anyone imagined he could do. Yet, despite all the things he has attained, his one true love eludes him. 'Forrest Gump' is the story of a man who rose above his challenges, and who proved that determination, courage, and love are more important than ability."
      gump.popularity shouldBe 10.7281
      gump.releaseDate shouldBe LocalDate.of(1994,7, 6)
      gump.revenue shouldBe 677945399
      gump.runtime shouldBe 142
      gump.tagLine shouldBe "The world will never be the same, once you've seen it through the eyes of Forrest Gump."
      gump.voteAvg shouldBe 8.2
      gump.voteCount shouldBe 7208

      gump.productionCompanies shouldBe List("Paramount Pictures")
      gump.genres shouldBe List("Comedy", "Drama", "Romance")
    }

    it("should read and write json using reads and writes") {
      val json = Json.toJson[Movie](movie)
//      println(s"json: ${Json.prettyPrint(json)}")
      val m = Json.fromJson[Movie](json).get
      m shouldBe movie
    }

    it("should read and write json using implict conversion") {
      val json:JsValue = movie
      val m:Movie = json
      m shouldBe movie
    }
  }

}
