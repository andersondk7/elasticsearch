package adc.tutorial.elasticsearch.services

import adc.tutorial.elasticsearch.model.Movie
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.indexes.IndexDefinition

import scala.concurrent.{Future, Promise}

class BulkIndexerSinkActor(
                          client: HttpClient
                          , complete: Promise[Int]
                          , bufferSize: Int
                          ) extends Actor with ActorLogging {
  import StreamMessages._
  import BulkIndexerSinkActor._
  implicit val ec = context.dispatcher

  override def unhandled(message: Any): Unit = {
    log.error(s"received ${message.getClass.getName}")
  }

  // ********************************************************
  // initial state
  // ********************************************************
  override def receive: Receive = {

    case _: Init.type =>
      context.become(processingStream(complete, List[Movie](), 0))
      sender() ! Ack
  }

  // ********************************************************
  // processing stream data ...
  // ********************************************************
  def processingStream(p: Promise[Int], toBeProcessed: List[Movie], totalProcessed: Int): Receive = {

    case movie: Movie =>
      val updatedBuffer = movie :: toBeProcessed
      log.info(s"index ${movie.id}, pending: ${updatedBuffer.size}/$bufferSize, total: $totalProcessed")
      if (updatedBuffer.size < bufferSize) {
        log.info("continuing")
        context.become(processingStream(p, updatedBuffer, totalProcessed+1))
        sender() ! Ack
      }
      else {
        val upstream = sender()
        val bulkCommand: List[IndexDefinition] = updatedBuffer.map(m => index("movies", "movie").withId(m.id).source[Movie](m))
        log.info(s"sending bulk request with ${bulkCommand.size} inserts")
        val execution: Future[ExecutionResponse] = client.execute { bulk (bulkCommand) }.map (r => ExecutionResponse(r, upstream))
        pipe(execution) to self
        context.become(waitingOnES(p, totalProcessed+1))
      }

    case _: Complete.type =>
      if (toBeProcessed.nonEmpty) {
        val upstream = sender()
        val bulkCommand: List[IndexDefinition] = toBeProcessed.map(index("movies", "movie").source[Movie](_))
        val execution: Future[ExecutionResponse] = client.execute { bulk (bulkCommand) }.map(r => ExecutionResponse(r, upstream))
        context.become(waitingOnES(p, totalProcessed, streamComplete = true))
      }
      p.success(totalProcessed)
      context.stop(self)
  }

  // ********************************************************
  // waiting for ElasticSearch to complete the index...
  // ********************************************************
  def waitingOnES(p: Promise[Int], totalProcessed: Int, streamComplete: Boolean = false): Receive = {

    case ExecutionResponse(response, upstream) => // index completed

      val successes = response.successes
      val failures = response.failures
      log.info(s"completed with ${response.items.size}")
      val (remaining, newTotal) = if (response.hasFailures) {
        val reasons: List[String] = response.failures.map(i => s"$i.status} : ${i.error}").toList
        log.error(s"could not index because ${reasons.mkString("\n")} ")
        (List(), totalProcessed) // drop them for now
            // todo: implement try again, rather than just drop,
            // need to know how to map those that failed to those that were sent
      }
      else {
//        val successCount = response.successes.size
        val successCount = bufferSize
        log.info(s"bulk request with $successCount took ${response.took} milliseconds")
        (List(), totalProcessed)
      }
      if (!streamComplete) { // do the next element (when it comes)
        log.info(s"resuming with a new total: $newTotal")
        context.become(processingStream(p, remaining, newTotal))
        upstream ! Ack
      }
      else { // no more elements, we're done
        p.success(newTotal)
        context.stop(self)
      }

    case _: Complete.type =>
      context.become(waitingOnES(p, totalProcessed, streamComplete=true))
  }

  override def postStop(): Unit = {
    client.close()
  }

}

object BulkIndexerSinkActor {
  def props(
           uri: ElasticsearchClientUri
           , p: Promise[Int]
           , bufferSize: Int = 10
           ) = Props(classOf[BulkIndexerSinkActor], HttpClient(uri), p, bufferSize)
  case class ExecutionResponse(response: BulkResponse, upstream: ActorRef)
}