package co.blocke
package latekafka

import akka.stream.scaladsl._
import akka.stream._
import org.apache.kafka.clients.consumer.ConsumerRecord

case class FilterGraph(host: String, groupId: String, topic: String, properties: Map[String, String] = Map.empty[String, String]) extends GraphHolder[Message[MessagePayload]] {

  val deserializer = new MessageDeserializer

  val flow = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[akka.NotUsed] =>
    import GraphDSL.Implicits._
    type In = ConsumerRecord[Array[Byte], Message[MessagePayload]]

    late = LateKafka[Message[MessagePayload]](
      host,
      groupId,
      topic,
      deserializer,
      properties
    )

    val src = late.source
    val commit = Flow[In].map { msg =>
      late.commit(msg)
      msg
    }
    val filter = builder.add(Partition[In](2, (crec) => { if (crec.value.payload.toString.startsWith("G")) 0 else 1 }))
    val merge = builder.add(Merge[In](2))
    val sayYes = Flow[In].map { i => println("Yes, " + i.value.payload); i }
    val sayNo = Flow[In].map { i => println("No, " + i.value.payload); i }

    // format: OFF

    src ~> filter ~> sayYes ~> merge ~> commit ~> Sink.ignore
           filter ~> sayNo  ~> merge

    // format: ON
    ClosedShape
  })
}