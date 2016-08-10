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
  private var cbWaiting = 0

  def run() {

    val consumer = new KafkaConsumer[Array[Byte], V](
      (Map(
        "bootstrap.servers" -> host,
        "enable.auto.commit" -> "false",
        "auto.commit.interval.ms" -> "1000",
        "group.id" -> groupId
      ) ++ properties),
      new ByteArrayDeserializer,
      deserializer
    )
    consumer.subscribe(List(topic))

    def dummyPoll() {
      consumer.pause(consumer.assignment)
      consumer.poll(0)
      consumer.resume(consumer.assignment)
    }

    // Record-keeping
    val lastRecord = MMap.empty[Int, Long] // partition# -> offset

    while (running) {
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

      cmds.poll(100, TimeUnit.MILLISECONDS) match {
        case null =>

        case cr: ConsumerRecord[_, _] => // soft-commit
          lastRecord.get(cr.partition) match {
            case Some(lrOffset) if (lrOffset >= cr.offset) => // do nothing... already processed a higher offset
            case _ =>
              lastRecord.put(cr.partition, cr.offset)
          }

        case p: Promise[_] => // request for more data from queue
          p.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success(consumer.poll(100).iterator)

        // Need a "fake", no-data, poll in order to lock in assignments for this consumer or
        // its possible some partitions won't be properly committed.
        case "dummy" => dummyPoll()
      }
    }
    consumer.close()
  }

  def !(a: AnyRef) = cmds.add(a)
  def !!(a: AnyRef) = commits = true
  def stop() = running = false
}
