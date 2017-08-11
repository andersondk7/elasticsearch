package adc.tutorial.elasticsearch.services

import java.io.Writer

import adc.tutorial.elasticsearch.model.Movie
import akka.actor.{ActorRef, ActorSystem}
import akka.{Done, NotUsed}
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.bulk.BulkCompatibleDefinition
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.bulk.BulkResponseItem
import com.sksamuel.elastic4s.streams.{BulkIndexingSubscriber, RequestBuilder, ResponseListener, SubscriberConfig}
import play.api.libs.json.JsValue
import com.sksamuel.elastic4s.streams.ReactiveElastic._

import scala.concurrent.{Future, Promise}

class DataSinkService {

  def toFile(writer: Writer): Sink[Movie, Future[Done]] = Flow[Movie]
      .map[JsValue](m => Movie.toJson(m))
    .map(j => writer.append(s"${j.toString}\n"))
    .toMat(Sink.ignore)(Keep.right)

  def toElasticSearch(client: HttpClient)(implicit actorSystem: ActorSystem): Sink[Movie, Future[Int]] = {
    implicit val ec = actorSystem.dispatcher
    var count = 0
    val p = Promise[Int]()

    implicit val movieImporter = new RequestBuilder[Movie] {
      import com.sksamuel.elastic4s.http.ElasticDsl._
      def request(movie: Movie): BulkCompatibleDefinition = {
        count = count + 1
        println(s"inserting ${movie.id} -> ${movie.title} - $count")
        index("movies", "movie").source[Movie](movie)
      }
    }
    val whenComplete: () => Unit = () => {
      if (p.isCompleted) {
        println(s"subscriber: complete after failure")
      }
      else {
        p.success(count)
        println(s"subscriber: all done")
      }
    }
    val whenError: (Throwable) => Unit = (t:Throwable) => {
//      p.failure(t)
//      p.success(count) // this is as far as we get
      println(s"subscriber: error at count $count --> $t")
    }

    val subscriber = client.subscriber[Movie](
                                              batchSize=5
                                              , concurrentRequests = 2
                                              , completionFn = whenComplete
                                              , errorFn =  whenError
                                             )


    Sink.fromSubscriber(subscriber).mapMaterializedValue(m=> p.future)
  }

  def toCount()(implicit actorSystem: ActorSystem): Sink[Movie, Future[Int]] = {
    Sink.fold(0){ (s,m) => s+1}
  }

  def toCountActor()(implicit actorSystem: ActorSystem): Sink[Movie, Future[Int]] = {
    val p = Promise[Int]()
    Sink.actorRefWithAck[Movie](
      ref = actorSystem.actorOf(CounterSinkActor.props(p))
      , onInitMessage=StreamMessages.Init
      , ackMessage=StreamMessages.Ack
      , onCompleteMessage=StreamMessages.Complete
    ).mapMaterializedValue(m => {
      p.future
    })
  }

  def toIndexer(uri: ElasticsearchClientUri)(implicit actorSystem: ActorSystem): Sink[Movie, Future[Int]] = {
    val p = Promise[Int]()
    Sink.actorRefWithAck[Movie](
      ref = actorSystem.actorOf(SingleIndexerSinkActor.props(uri, p))
      , onInitMessage=StreamMessages.Init
      , ackMessage=StreamMessages.Ack
      , onCompleteMessage=StreamMessages.Complete
    ).mapMaterializedValue(m => {
      p.future
    })
  }
}

object DataSinkService {
}


