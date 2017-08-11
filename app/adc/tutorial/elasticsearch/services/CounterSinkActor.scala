package adc.tutorial.elasticsearch.services

import akka.actor.{Actor, ActorLogging, Props}

import scala.concurrent.Promise

class CounterSinkActor(complete: Promise[Int]) extends Actor with ActorLogging {
  import StreamMessages._ // messages from stream...

  // initial state
  override def receive: Receive = {
    case _: Init.type =>
      println(s"init")
      context.become(counting(complete, 0))
      sender() ! Ack
  }

  // counting state
  def counting(p: Promise[Int], total: Int): Receive = {
    case _: Complete.type =>
      println(s"completed with count $total")
      p.success(total)
      context.stop(self)

    case x =>
      println(s"counting ${total+1}")
      context.become(counting(p, total+1))
      sender() ! Ack
  }

  // completed state
  def finished(result: Promise[Int]): Receive = {
    case m => super.unhandled(m)
  }
}

object CounterSinkActor {
  def props(p: Promise[Int]) = Props(classOf[CounterSinkActor], p)
}
