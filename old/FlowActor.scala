package co.blocke
package latekafka

import akka.actor.Actor
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.RunnableGraph

trait GraphHolder {
  val late: LateKafka[_]
  val flow: RunnableGraph[akka.NotUsed]
}

class FlowActor(graph: GraphHolder)(implicit materializer: ActorMaterializer) extends Actor {

  def run() = graph.flow.run()

  override def postStop() {
    graph.late.stop()
  }

  def receive: Actor.Receive = Actor.ignoringBehavior

  run() // just go, by default
}