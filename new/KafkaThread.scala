package co.blocke
package latekafka

import java.util.concurrent.LinkedBlockingQueue
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, Deserializer }
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{ ConsumerRecord, ConsumerRecords, KafkaConsumer, OffsetAndMetadata }
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

    def dummyPoll() {
      consumer.pause(consumer.assignment)
      consumer.poll(100)
      consumer.resume(consumer.assignment)
    }

    // Need a "fake", no-data, poll in order to lock in assignments for this consumer or
    // its possible some partitions won't be properly committed.
    dummyPoll()

    // Record-keeping
    val lastRecord = MMap.empty[Int, Long] // partition# -> offset
    // var lastPull: Iterator[ConsumerRecord[Array[Byte], V]] = null
    // var lastPromise: Promise[_] = null

    while (running) {
      cmds.poll(20, TimeUnit.MILLISECONDS) match {
        case null =>
          lastRecord.map {
            case (partition, offset) =>
              // println("XMIT: " + partition + ":" + offset)
              consumer.commitAsync(java.util.Collections.singletonMap(
                new TopicPartition(topic, partition),
                new OffsetAndMetadata(offset + 1)
              ), null)
          }
          lastRecord.clear

        // TRY #2
        // if (lastPromise != null) {
        //   val poll = consumer.poll(50).iterator
        //   if (!poll.isEmpty) {
        //     lastPromise.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success(poll)
        //     lastPromise = null
        //   }
        // }

        // TRY #1
        // if (lastPull == null) {
        //   lastPull = consumer.poll(0).iterator
        //   if (lastPull.isEmpty) lastPull = null
        // }
        // if (lastPromise != null && lastPull != null) {
        //   lastPromise.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success(lastPull)
        //   lastPromise = null
        //   lastPull = null
        // }
        case cr: ConsumerRecord[_, _] => // soft-commit
          lastRecord.get(cr.partition) match {
            case Some(lrOffset) if (lrOffset >= cr.offset) => // do nothing... already processed a higher offset
            case _ =>
              // println("POST: " + cr.partition + ":" + cr.offset)
              lastRecord.put(cr.partition, cr.offset)
          }
        case p: Promise[_] => // request for more data from queue
          p.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success(consumer.poll(20).iterator)
        // lastPromise = p
      }
    }

    // Check for waiting commit orders
    // if (commits) {
    //   commits = false
    // }

    // Polling the blocking queue feeding this thread commands...not Kafka here
    // cmds.poll({ if (lastPull == null) 0 else 100 }, TimeUnit.MILLISECONDS) match {
    // var trip = 100
    // while (trip > 0) { // put a cap on inbound messages before taking a break to do some other work
    //   trip = trip - 1
    // cmds.poll() match {
    // case "dummy" =>
    //   dummyPoll()
    // case null =>
    //   trip = 0 // nothing waiting... go do some other work
    // }
    // }
    // }
    consumer.close()
  }

  def !(a: AnyRef) = cmds.add(a)
  def !!(a: AnyRef) = commits = true
  def stop() = {
    running = false
  }
}
