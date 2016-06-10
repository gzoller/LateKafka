package co.blocke
package latekafka

import java.util.concurrent.LinkedBlockingQueue
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, Deserializer }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{ ConsumerRecord, KafkaConsumer, OffsetAndMetadata }
import scala.concurrent.Promise
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import scala.collection.mutable.{ Map => MMap }
import co.blocke.scalajack._
import scala.reflect.runtime.universe.TypeTag

case class JsonDeserializer[V]()(implicit tag: TypeTag[V]) extends Deserializer[V] {
  private val sj = ScalaJack()

  var toggle = false

  def close() {}
  def configure(configs: java.util.Map[String, _], isKey: Boolean) {}
  def deserialize(topic: String, data: Array[Byte]): V = sj.read[V](new String(data))
}

case class KafkaThread[V](
    host:       String,
    groupId:    String,
    topic:      String,
    properties: Map[String, String]
)(implicit tag: TypeTag[V]) extends Runnable {

  private val cmds = new LinkedBlockingQueue[AnyRef]()
  private val commits = new LinkedBlockingQueue[AnyRef]()
  private var running = true
  private val deserializer = JsonDeserializer[V]()

  def run() {
    val consumer = new KafkaConsumer[Array[Byte], V](
      (properties ++ Map(
        "bootstrap.servers" -> host,
        "enable.auto.commit" -> "false",
        "auto.commit.interval.ms" -> "1000",
        "auto.offset.reset" -> "earliest",
        "group.id" -> groupId
      )),
      new ByteArrayDeserializer,
      deserializer
    )
    consumer.subscribe(List(topic))

    // Record-keeping
    val lastRecord = MMap.empty[Int, Long] // partition# -> offset

    while (running) {
      // Check for waiting commit orders
      Option(commits.poll(0, TimeUnit.MILLISECONDS)).map { _ =>
        lastRecord.map {
          case (partition, offset) =>
            consumer.commitAsync(java.util.Collections.singletonMap(
              new TopicPartition(topic, partition),
              new OffsetAndMetadata(offset + 1)
            ), null)
        }
        lastRecord.clear
      }

      // Polling the blocking queue feeding this thread commands...not Kafka here
      cmds.poll(100, TimeUnit.MILLISECONDS) match {
        case cr: ConsumerRecord[_, _] =>
          lastRecord.get(cr.partition) match {
            case Some(lrOffset) if (lrOffset >= cr.offset) => // do nothing
            case _                                         => lastRecord.put(cr.partition, cr.offset)
          }
        case p: Promise[_] =>
          p.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success(consumer.poll(100).iterator)
        case null => // do nothing...try again
      }
    }
    consumer.close()
  }

  def !(a: AnyRef) = cmds.put(a)
  def !!(a: AnyRef) = commits.put(a)
  def stop() = {
    running = false
  }
}