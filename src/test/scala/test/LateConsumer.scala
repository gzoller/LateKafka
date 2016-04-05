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
  var count = 0 //new AtomicInteger(0)
  def reset() = count = 0 //count.set(0) 
  def syncInc() = this.synchronized {
    count += 1
  }
}
import LateConsumer._

case class LateConsumer[V](partitions: List[Int] = List(0)) {

  val late = LateKafka[V](
    "192.168.99.100:9092",
    "group1",
    "lowercaseStrings",
    (new org.apache.kafka.common.serialization.StringDeserializer).asInstanceOf[org.apache.kafka.common.serialization.Deserializer[V]],
    partitions
  )
  def stop() = {
    println("Stopping @ count " + count)
    late.stop()
  }

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
          // if (count % 1000 == 0) println(s"Commit [$id]: $count")
          late.commit(msg)
          // val c = count.incrementAndGet()
          syncInc()
          // println("Message: "+i.value)
          if (count == num) {
            done = true
            println(s"[$id] Time ($count): " + (System.currentTimeMillis() - now))
            late.done
          }
        }
      }
      val show = Flow[In].map { i =>
        // Thread.sleep(1000)
        // if (count % 1000 == 0)
        // println(s"SHOW [$id]: " + i)
        i
      }

      src ~> show ~> commit ~> Sink.ignore
      ClosedShape
    })
    now = System.currentTimeMillis()
    graph.run()
    Thread.sleep(45000)
    println(s"Done running @ $count...waiting a while")
  }
}