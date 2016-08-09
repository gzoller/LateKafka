package co.blocke
package latekafka

import akka.actor.Actor
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.RunnableGraph
import org.apache.kafka.common.serialization.Deserializer

trait GraphHolder[V] {
  val host: String
  val groupId: String
  val topic: String
  val deserializer: Deserializer[V]
  val properties: Map[String, String]
  val flow: RunnableGraph[akka.NotUsed]

  var late: LateKafka[V] = null

  def stop() = if (late != null) late.stop()
}

// NOTE: We must create the LateKafka object *inside* its own thread!  So pass in all the needed
// parameters (in GraphHolder) and create it once the actor has started and called run().
class FlowActor[V](graph: GraphHolder[V])(implicit materializer: ActorMaterializer) extends Actor {

  def run() = graph.flow.run()

  override def postStop() {
    graph.stop()
  }

  def receive: Actor.Receive = Actor.ignoringBehavior

  run() // just go, by default
}