package com.cof.kafka

import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, Deserializer, StringDeserializer }
import java.util.concurrent.LinkedBlockingQueue
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{ ConsumerRebalanceListener, OffsetCommitCallback, ConsumerRecord, KafkaConsumer, OffsetAndMetadata }
import scala.concurrent.Promise
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import scala.concurrent.Await
import akka.stream.scaladsl.Source
import java.util.Collection
// import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{ Map => MMap }

object Alignment {
  def registerTopic(topic: String, partitions: List[Int]) = {
    if (!latest.get(topic).isDefined) latest.put(topic, MMap.empty[Int, Long])
    partitions.map(p => latest(topic).put(p, 0L))
  }

  val latest = MMap.empty[String, MMap[Int, Long]] // Map[Topic,Map[Partition#,lastOffset]]
  val scoop = scala.collection.mutable.Map(
    "lowercaseStrings-0" -> 0, //scala.collection.mutable.ListBuffer.fill(250000)(0),
    "lowercaseStrings-1" -> 0, //scala.collection.mutable.ListBuffer.fill(250000)(0),
    "lowercaseStrings-2" -> 0, //scala.collection.mutable.ListBuffer.fill(250000)(0),
    "lowercaseStrings-3" -> 0 //scala.collection.mutable.ListBuffer.fill(250000)(0)
  )
}
import Alignment._

case class OffCB() extends OffsetCommitCallback {
  def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], ex: Exception) {
    if (ex != null) {
      print(s"ERROR [$offsets]:  ")
      ex.printStackTrace()
    }
    /*
    offsets.map {
      case (k, v) =>
        val z = k.topic() + "-" + k.partition()
        if (ex != null)
          Z.synchronized {
            if (scoop(z) == 0) ex.printStackTrace()
            scoop.put(z, scoop(z) + 1)
            // scoop(z)(v.offset.toInt) = scoop(z)(v.offset.toInt) + 1
            // else {
            //   println("Boom: " + ex)
            //   scoop(z)(v.offset.toInt) = -999
            // }
          }
    }
    */
  }
}

case class KafkaThread[V](
    host:         String,
    groupId:      String,
    topic:        String,
    deserializer: Deserializer[V],
    partitions:   List[Int],
    properties:   Map[String, String]
) extends Runnable {

  private val q = new LinkedBlockingQueue[AnyRef]()
  private var running = true
  private val cb = OffCB()

  def run() {
    println("Bootstrap: " + host)
    val consumer = new KafkaConsumer[Array[Byte], V](
      (properties ++ Map(
        "bootstrap.servers" -> host,
        "enable.auto.commit" -> "false",
        "auto.commit.interval.ms" -> "1000",
        "auto.offset.reset" -> "earliest",
        // "session.timeout.ms" -> "60000",
        "group.id" -> groupId
      )),
      new ByteArrayDeserializer,
      deserializer
    )
    consumer.subscribe(List(topic), new ConsumerRebalanceListener() {
      def onPartitionsRevoked(partitions: Collection[TopicPartition]) { println("Revoked: " + partitions) }
      def onPartitionsAssigned(partitions: Collection[TopicPartition]) { println("Assigned: " + partitions) }
    })
    // consumer.assign(partitions.map(p => new TopicPartition(topic, p)))

    while (running) {
      q.poll(100, TimeUnit.MILLISECONDS) match { // Polling my blocking queue...not Kafka here
        case cr: ConsumerRecord[_, _] =>
          // println("==== Value: " + cr.value() + "  ===== Partition: " + cr.partition() + " ====== Offset: " + cr.offset())
          val off = cr.offset() + 1
          val offsets = java.util.Collections.singletonMap(
            new TopicPartition(cr.topic(), cr.partition()),
            new OffsetAndMetadata(off)
          )
          // println("   Update!: " + offsets)
          // consumer.commitSync(offsets)
          consumer.commitAsync(offsets, cb)
        case p: Promise[_] =>
          p.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success(consumer.poll(100).iterator)
        case null => // do nothing...try again
      }
    }
    println("::: Consumer Stopped :::")
    consumer.close()
  }

  def !(a: AnyRef) = q.add(a)
  def stop() = {
    running = false
  }
}