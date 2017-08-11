package adc.tutorial.elasticsearch.services

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

import adc.tutorial.elasticsearch.model.Movie
import akka.actor.ActorSystem
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape, Graph}
import akka.{Done, NotUsed}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}
import play.api.libs.json.{JsValue, Json}
import play.api.libs.ws.ahc.StandaloneAhcWSClient

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.postfixOps

class DataSourceServiceSpec extends FunSpec with Matchers {

  implicit val actorSystem = ActorSystem("DataSourceServiceSpec")
  implicit val materializer = ActorMaterializer()

  val wSClient = StandaloneAhcWSClient()

  private val config = ConfigFactory.load()
  private val envVars = System.getenv()
  private val apiKeyName = config.getString("api.key")  // name of environment value of the key
  private val apiKey = System.getenv(apiKeyName)  // your key
  private val url = config.getString("api.url") // https://api.themoviedb.org/3/movie
  private val delay = 5 seconds

  private val service = new DataSourceService(url, apiKey, wSClient)

  describe("DataSourceService") {
    it ("should read ids from the file") {
      val ids: List[Int] = service.ids(10, 25)
      ids.head shouldBe 15 // based on the contents of the movie.ids, which skips numbers...
      ids.size shouldBe 25
    }
    it ("should call the web") {
      val json = Await.result(service.webCall(13).mapTo[JsValue], delay)
      println(s"json: \n${Json.prettyPrint(json)}\n")
    }
    it ("should print json from web to std out") {
      val graph: Graph[ClosedShape, Future[Done]] = GraphDSL.create(Sink.ignore) {
        implicit builder: GraphDSL.Builder[Future[Done]] => s =>
          implicit val ec:ExecutionContext = actorSystem.dispatcher // to handle futures...
          val toStdOut: Flow[JsValue, Unit, NotUsed] = Flow[JsValue].map(jsv => println(s"${Json.prettyPrint(jsv)}\n"))
          service.fromWeb(10, 3) ~> toStdOut ~> s
          ClosedShape
        }
      Await.result(RunnableGraph.fromGraph(graph).run, 20 seconds)
    }
    it("should retrieve json from file") {
      val graph: Graph[ClosedShape, Future[Done]] = GraphDSL.create(Sink.ignore) {
        implicit builder: GraphDSL.Builder[Future[Done]] => s =>
          implicit val ec:ExecutionContext = actorSystem.dispatcher // to handle futures...
          val movieFlow: Flow[JsValue, Movie, NotUsed] = Flow[JsValue].map(Movie.fromMovieDbJson(_))
          val idFlow: Flow[Movie, String, NotUsed] = Flow[Movie].map(_.id.toString)
          val toStdOut: Flow[String, Unit, NotUsed] = Flow[String].map(println(_))
          service.fromFile(3, 50) ~> movieFlow ~> idFlow ~> toStdOut ~> s
          ClosedShape
      }
      Await.result(RunnableGraph.fromGraph(graph).run, 20 seconds)

    }
    it ("should append json from web to a file") {
      val fileName = "/tmp/movie.json"
      if (Files.exists(Paths.get(fileName))) Files.delete(Paths.get(fileName))
      Files.exists(Paths.get(fileName)) shouldBe false
      val writer = new PrintWriter(fileName)

      val graph: Graph[ClosedShape, Future[Done]] =
        GraphDSL.create(Sink.ignore) { implicit builder: GraphDSL.Builder[Future[Done]] => s =>
          val toFile: Flow[JsValue, Unit, NotUsed] = Flow[JsValue].map(jsv => {
            try {
              val id = jsv("id")
            }catch {
              case _: Throwable => println(s"no id in ${Json.prettyPrint(jsv)}")
            }
            writer.append(s"$jsv\n")
          })
          service.fromWeb(6200, 3) ~> toFile ~> s
          ClosedShape
        }
      Await.result(RunnableGraph.fromGraph(graph).run, 15 minutes)
      writer.close()
      Files.exists(Paths.get(fileName)) shouldBe true
    }
  }

}
