package co.blocke
package latekafka

import akka.stream.scaladsl._
import akka.stream._
import org.apache.kafka.clients.consumer.ConsumerRecord
import akka.actor._
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps

case class Racer(entry: Long)

case class TimerFlow(host: String, group: String, topic: String, props: Map[String, String] = Map.empty[String, String]) {

  val late = LateKafka[String](
    host,
    group,
    topic,
    new org.apache.kafka.common.serialization.StringDeserializer,
    props
  )
  def stop() = late.stop()

  def consume(id: Int, num: Int)(implicit m: ActorMaterializer, as: ActorSystem) = {

    implicit val t = Timeout(10 seconds)

    val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[akka.NotUsed] =>
      import GraphDSL.Implicits._
      type In = ConsumerRecord[Array[Byte], String]

      val src = late.source
      val commit = Flow[In].map { msg =>
        late.commit(msg)
        msg
      }
      val timeit = Flow[In].map { i =>
        val now = System.currentTimeMillis
        println("Racer Time: " + (now - i.value.toLong))
      }

      src ~> commit ~> timeit ~> Sink.ignore
      ClosedShape
    })
    val now = System.currentTimeMillis()
    graph.run()

    // Wait for a while for work to finish.  In a real (non-test) app, this would run forever.
    println("Working...")
    Thread.sleep(10000)
    println("Waiting to finish...")
    late.done
    Thread.sleep(2000)
    late.stop
  }
}