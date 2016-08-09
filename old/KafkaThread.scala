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

case class KafkaThread[V](
    host:         String,
    groupId:      String,
    topic:        String,
    deserializer: Deserializer[V],
    properties:   Map[String, String]
) extends Runnable {

  private val cmds = new LinkedBlockingQueue[AnyRef]()
  private var commits = false
  private var running = true

  val delay = 100

  def run() {
    val consumer = new KafkaConsumer[Array[Byte], V](
      (Map(
        "bootstrap.servers" -> host,
        "enable.auto.commit" -> "false",
        "group.id" -> groupId
      ) ++ properties),
      new ByteArrayDeserializer,
      deserializer
    )
    consumer.subscribe(List(topic))

    // Record-keeping
    val lastRecord = MMap.empty[Int, Long] // partition# -> offset

    while (running) {
      // Check for waiting commit orders
      if (commits) {
        lastRecord.map {
          case (partition, offset) =>
            consumer.commitAsync(java.util.Collections.singletonMap(
              new TopicPartition(topic, partition),
              new OffsetAndMetadata(offset + 1)
            ), null)
        }
        lastRecord.clear
        commits = false
      }

      // Polling the blocking queue feeding this thread commands...not Kafka here
      cmds.poll(delay, TimeUnit.MILLISECONDS) match {
        case cr: ConsumerRecord[_, _] =>
          lastRecord.get(cr.partition) match {
            case Some(lrOffset) if (lrOffset >= cr.offset) => // do nothing
            case _                                         => lastRecord.put(cr.partition, cr.offset)
          }
        case p: Promise[_] =>
          p.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success(consumer.poll(delay).iterator)
        case null => // do nothing...try again
      }
    }
    consumer.close()
  }

  def !(a: AnyRef) = cmds.put(a)
  def !!(a: AnyRef) = commits = true
  def stop() = {
    running = false
  }
}
