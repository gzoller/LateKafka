package co.blocke
package latekafka

import java.util.concurrent.LinkedBlockingQueue
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, Deserializer }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{ OffsetCommitCallback, ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata }
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

    case class CommitCB() extends OffsetCommitCallback {
      def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], exception: Exception) = {
        cbWaiting = cbWaiting - 1
        if (running == false && cbWaiting == 0) println("ALL DONE!!!!")
      }
    }

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

    def dummyPoll() {
      consumer.pause(consumer.assignment)
      consumer.poll(100)
      consumer.resume(consumer.assignment)
    }

    // Need a "fake", no-data, poll in order to lock in assignments for this consumer or
    // its possible some partitions won't be properly committed.
    // dummyPoll()

    // Record-keeping
    val lastRecord = MMap.empty[Int, Long] // partition# -> offset

    while (running) {
      cmds.poll(20, TimeUnit.MILLISECONDS) match {
        case null =>
          lastRecord.map {
            case (partition, offset) =>
              cbWaiting = cbWaiting + 1
              consumer.commitAsync(java.util.Collections.singletonMap(
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1)
              ), CommitCB())
          }
          lastRecord.clear

        case cr: ConsumerRecord[_, _] => // soft-commit
          lastRecord.get(cr.partition) match {
            case Some(lrOffset) if (lrOffset >= cr.offset) => // do nothing... already processed a higher offset
            case _ =>
              // println("POST: " + cr.partition + ":" + cr.offset)
              lastRecord.put(cr.partition, cr.offset)
          }

        case p: Promise[_] => // request for more data from queue
          p.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success(consumer.poll(20).iterator)
      }
    }
    consumer.close()
  }

  def !(a: AnyRef) = cmds.add(a)
  def !!(a: AnyRef) = commits = true
  def stop() = {
    running = false
  }
}
