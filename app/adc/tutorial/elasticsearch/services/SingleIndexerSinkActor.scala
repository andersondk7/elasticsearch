package adc.tutorial.elasticsearch.services

import adc.tutorial.elasticsearch.model.Movie
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.index.IndexResponse

import scala.concurrent.{Future, Promise}

class SingleIndexerSinkActor(client: HttpClient, complete: Promise[Int]) extends Actor with ActorLogging {
  import StreamMessages._
  import SingleIndexerSinkActor._

  implicit val ec = context.dispatcher

  override def unhandled(message: Any): Unit = {
    log.error(s"received ${message.getClass.getName}")
  }

  // ********************************************************
  // initial state
  // ********************************************************
  override def receive: Receive = {

    case _: Init.type =>
      context.become(processingStream(complete, 0))
      sender() ! Ack
  }

  // ********************************************************
  // processing stream data ...
  // ********************************************************
  def processingStream(p: Promise[Int], total: Int): Receive = {

    case movie: Movie =>
      val upstream = sender()
      val execution: Future[ExecutionResponse] = client.execute { indexInto("movies" / "movie").source(movie) }.map (r => ExecutionResponse(r, upstream, movie))
      context.become(waitingOnES(p, total))
      pipe(execution) to self

    case _: Complete.type =>
      p.success(total)
      context.stop(self)
  }

  // ********************************************************
  // waiting for ElasticSearch to complete the index...
  // ********************************************************
  def waitingOnES(p: Promise[Int], total: Int, streamComplete: Boolean = false): Receive = {

    case ExecutionResponse(response, upstream, movie) => // index completed
      val newTotal = total+1
      log.info(s"indexed ${movie.id}, count: $newTotal")
      if (!streamComplete) { // do the next element (when it comes)
        context.become(processingStream(p, newTotal))
        upstream ! Ack
      }
      else { // no more elements, we're done
        p.success(newTotal)
        context.stop(self)
      }

    case _: Complete.type =>
      context.become(waitingOnES(p, total, streamComplete=true))
  }

  override def postStop(): Unit = {
    client.close()
  }
}

object SingleIndexerSinkActor {
  def props(uri: ElasticsearchClientUri, p: Promise[Int]) = Props(classOf[SingleIndexerSinkActor], HttpClient(uri), p)
  case class ExecutionResponse(response: IndexResponse, upstream: ActorRef, movie: Movie)
}
