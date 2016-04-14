package com.cof.kafka

import akka.stream.scaladsl._
import akka.stream._
import org.apache.kafka.clients.consumer.ConsumerRecord
import akka.actor._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps

// Reads from topic "lowercaseStrings", processes messages and commits offset into kafka after processing.
// This provides at-least-once delivery guarantee. Also, shows how to perform graceful shutdown.
//

import java.util.concurrent.atomic.AtomicInteger
object LateConsumer {
  var count = 0
  def reset() = count = 0
  def syncInc() = this.synchronized {
    count += 1
  }
}
import LateConsumer._

case class LateConsumerFlow[V](host: String, group: String, topic: String, partitions: List[Int] = List(0)) {

  val late = LateKafka[V](
    host,
    group,
    topic,
    (new org.apache.kafka.common.serialization.StringDeserializer).asInstanceOf[org.apache.kafka.common.serialization.Deserializer[V]],
    partitions
  )
  def stop() = late.stop()

  def consume(id: Int, num: Int)(implicit m: ActorMaterializer, as: ActorSystem) {

    implicit val t = Timeout(10 seconds)

    var now: Long = 0L
    var done = false

    val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[akka.NotUsed] =>
      import GraphDSL.Implicits._
      type In = ConsumerRecord[Array[Byte], V]

      val src = late.source
      val commit = Flow[In].map { msg =>
        if (!done) {
          late.commit(msg)
          syncInc()
          if (count == num) {
            done = true
            println(s"[$id] Time ($count): " + (count / ((System.currentTimeMillis() - now) / 1000)) + " TPS")
          }
        }
      }
      val work = Flow[In].map { i => i } // a dummy step where real "work" would happen

      src ~> work ~> commit ~> Sink.ignore
      ClosedShape
    })
    now = System.currentTimeMillis()
    graph.run()

    // Wait for a while for work to finish.  In a real (non-test) app, this would run forever.
    while (count < num)
      Thread.sleep(1000)
    Thread.sleep(10000)
    late.done
    Thread.sleep(10000)
  }
}