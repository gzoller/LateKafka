package co.blocke
package latekafka

import akka.stream.scaladsl._
import akka.stream._
import org.apache.kafka.clients.consumer.ConsumerRecord
import akka.actor._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.concurrent.LinkedBlockingQueue

// Reads from topic "lowercaseStrings", processes messages and commits offset into kafka after processing.
// This provides at-least-once delivery guarantee. Also, shows how to perform graceful shutdown.
//

object Aggregator {
  val total = new LinkedBlockingQueue[Int]()
  def reset() = total.clear
}
import Aggregator._

case class SpeedGraph(host: String, groupId: String, topic: String, properties: Map[String, String] = Map.empty[String, String]) extends GraphHolder[String] {

  val deserializer = new org.apache.kafka.common.serialization.StringDeserializer

  val count = new LinkedBlockingQueue[Int]()
  var x = 0

  var lastCommit: ConsumerRecord[Array[Byte], String] = null

  val flow = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[akka.NotUsed] =>
    import GraphDSL.Implicits._
    type In = ConsumerRecord[Array[Byte], String]

    late = LateKafka[String](
      host,
      groupId,
      topic,
      deserializer,
      properties
    )

    val src = late.source
    val commit = Flow[In].map { msg =>
      late.commit(msg)
      total.add(1)
      count.add(1)
      lastCommit = msg
    }
    val work = Flow[In].map { i =>
      x = x + 1
      i
    }

    src ~> work ~> commit ~> Sink.ignore

    ClosedShape
  })
}

class SpeedActor(num: Int, graph: GraphHolder[String])(implicit materializer: ActorMaterializer) extends FlowActor[String](graph) {

  private var now = 0L

  override def run() = akka.NotUsed // disable auto-run

  private var savedSender: ActorRef = null

  val id = scala.util.Random.nextInt(100)

  override def receive() = {
    case num: Int =>
      graph.flow.run()
      now = System.currentTimeMillis()
      context.system.scheduler.scheduleOnce(500 milliseconds, self, "check")
      savedSender = sender

    case "check" =>
      if (total.size < num)
        context.system.scheduler.scheduleOnce(500 milliseconds, self, "check")
      else {
        if (graph.asInstanceOf[SpeedGraph].late != null) graph.asInstanceOf[SpeedGraph].late.done
        val ms = System.currentTimeMillis() - now
        println("Processed: " + graph.asInstanceOf[SpeedGraph].count.size + " --> " + total.size)
        savedSender ! (graph.asInstanceOf[SpeedGraph].count.size / (ms / 1000.0)).toInt
      }
  }
}