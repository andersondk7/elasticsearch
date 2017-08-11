package adc.tutorial.elasticsearch.services


import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import scala.io.{BufferedSource, Source => IOSource}
import java.time.LocalDate

import adc.tutorial.elasticsearch.model.Movie
import akka.actor.ActorSystem
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape, Graph}
import akka.{Done, NotUsed}
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.indexes.IndexDefinition

import scala.concurrent.ExecutionContext
//import com.sksamuel.elastic4s.TcpClient
//import org.elasticsearch.common.settings.Settings
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.search.SearchResponse

import com.typesafe.config.ConfigFactory

import org.scalatest.{FunSpec, Matchers}
import play.api.libs.json.JsValue
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.util.Try

class DataSinkServiceSpec extends FunSpec with Matchers {

  implicit val actorSystem = ActorSystem("DataSinkServiceSpec")
  implicit val materializer = ActorMaterializer()

  private val wSClient = StandaloneAhcWSClient()
  private val config = ConfigFactory.load()
  private val apiKey = config.getString("api.key") // your key
  private val url = config.getString("api.url") // https://api.themoviedb.org/3/movie
  private val delay = 5 seconds

  private val sourceService = new DataSourceService(url, apiKey, wSClient)
  private val sinkService = new DataSinkService
  private val movie = Movie(
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

  describe("tcp client") {
    // be sure to import the dsl
//    import com.sksamuel.elastic4s.ElasticDsl._
//
//    it("should query, using tcp") {
//
//          val settings = Settings
//            .builder
//            .put("cluster.name", "elsearch")
//            .put("client.transport.sniff", true)
//            .build()
//          val uri = ElasticsearchClientUri("localhost", 9300)
//          val tcpClient = TcpClient.transport(settings, uri)
//          val result = tcpClient.execute {
//            search("movies").matchQuery("year", "1972")
//          }.await
//          tcpClient.close()
//        }
  }

  describe("http client") {

    it ("should index a single movie, using http") {
      val httpClient = HttpClient(ElasticsearchClientUri("localhost", 9200))
      val response = Await.result(httpClient.execute {
        indexInto("movies" / "movie").source(movie)
      }, 10 seconds)
      println(s"result:  $response")
      httpClient.close()
    }
    it ("should index a bunch of movies, using http") {
      val httpClient = HttpClient(ElasticsearchClientUri("localhost", 9200))
      val source = IOSource.fromFile(DataSourceService.movieJsonFileName)
      val movies: Iterator[Movie] = sourceService.moviesFromFile(source, 1, 600)
      println(s"indexing....")
      val results: List[Future[BulkResponse]] = movies.grouped(10).map(m => {
        val bulkCommand: List[IndexDefinition] = movies
          .map(index("movies", "movie").source[Movie](_))
          .toList
        httpClient.execute { bulk (bulkCommand) }
      }).toList
      val result: List[BulkResponse] = Await.result(Future.sequence(results).mapTo[List[BulkResponse]], 1 minute)
      val success: Boolean = result.forall(_.hasFailures)
      val time: Long = result.foldLeft(0L) ( (s, br) => s + br.took)

      println(s"success: $success, time: $time")
      source.close
      httpClient.close()
    }
    it("should query, using http") {
      val httpClient = HttpClient(ElasticsearchClientUri("localhost", 9200))
      val request: Future[SearchResponse] = httpClient.execute {
          search("movies").matchQuery("imdb_id", "imdb7")
        }

      val result: Option[Movie] = Await.result(request, delay).hits.hits.headOption// .map(h => Movie.fromJson(Json.parse(h.sourceAsString))
      httpClient.close()
      result.get shouldBe movie
    }
  }
  describe("a DataSinkService file sink") {

    it("should write data to file") {
      val fileName = "/tmp/sinkService.jsv"
      if (Files.exists(Paths.get(fileName))) Files.delete(Paths.get(fileName))
      Files.exists(Paths.get(fileName)) shouldBe false
      val writer = new PrintWriter(fileName)

      val graph = GraphDSL.create(sinkService.toFile(writer)) {implicit builder: GraphDSL.Builder[Future[Done]] => s =>

        val flow: Flow[JsValue, Movie, NotUsed] = Flow[JsValue].map[Movie](j => Movie.fromMovieDbJson(j))
        sourceService.fromFile(10, 3) ~> flow ~> s
        ClosedShape
      }
      Await.result(RunnableGraph.fromGraph(graph).run, 15 minutes)
      writer.close()
      Files.exists(Paths.get(fileName)) shouldBe true
    }
  }

  describe("a DataSinkService elasticsearch sink") {
    val max = 20
    def buildGraph(sink: Sink[Movie, Future[Int]]): Graph[ClosedShape, Future[Int]] = {
      var count = 0
      val offset = 3
      GraphDSL.create(sink) {
        implicit builder: GraphDSL.Builder[Future[Int]] => s =>
          val movieCountFlow: Flow[JsValue, Movie, NotUsed] = Flow[JsValue].map[Movie](j => {
            val m = Movie.fromMovieDbJson(j)
            count = count + 1
            println(s"parsed id:${m.id} - $count")
            m
          })
          sourceService.fromIterator(max) ~> movieCountFlow ~> s
          ClosedShape
      }
    }
    it ("should write data to elasticsearch using an http client") {
      val httpClient = HttpClient(ElasticsearchClientUri("localhost", 9200))
      val graph = buildGraph(sinkService.toElasticSearch(httpClient))
      val result = Await.result(RunnableGraph.fromGraph(graph).run, 20 seconds)
      println(s"done after $result")
      httpClient.close()
      println(s"closed")
      result shouldBe max
    }

    it ("should read data from file") {
      val graph = buildGraph(sinkService.toCount)
      val result = Await.result(RunnableGraph.fromGraph(graph).run, 20 seconds)
      println(s"done after $result")
      result shouldBe max
    }
  }

}
