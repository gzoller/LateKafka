package com.cof.kafka

import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, Deserializer, StringDeserializer }
import java.util.concurrent.LinkedBlockingQueue
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.{ OffsetCommitCallback, ConsumerRecord, KafkaConsumer, OffsetAndMetadata }
import scala.concurrent.Promise
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._
import scala.concurrent.Await
import akka.stream.scaladsl.Source

case class OffCB() extends OffsetCommitCallback {
  def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], ex: Exception) {}
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

  // var c = 0

  def run() {
    val consumer = new KafkaConsumer[Array[Byte], V](
      (properties ++ Map(
        "bootstrap.servers" -> host,
        "enable.auto.commit" -> "false",
        "auto.offset.reset" -> "earliest",
        "group.id" -> groupId
      )),
      new ByteArrayDeserializer,
      deserializer
    )
    // consumer.subscribe(List(topic))
    consumer.assign(partitions.map(p => new TopicPartition(topic, p)))

    while (running) {
      q.poll(100, TimeUnit.MILLISECONDS) match {
        case cr: ConsumerRecord[_, _] =>
          val offsets = java.util.Collections.singletonMap(
            new TopicPartition(cr.topic(), cr.partition()),
            new OffsetAndMetadata(cr.offset())
          )
          // c += 1
          // if (c % 10000 == 0) println("Size: " + q.size) //print('X')
          // println("COMMIT: " + cr)
          // consumer.commitSync(offsets)
          consumer.commitAsync(offsets, cb)
        case p: Promise[_] =>
          p.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success {
            val z = consumer.poll(100)
            if (z.size > 0) println("Pulled " + z.size)
            z.iterator
          }
        // p.asInstanceOf[Promise[Iterator[ConsumerRecord[Array[Byte], V]]]].success(consumer.poll(100).iterator)
        case null => // do nothing...try again
      }
    }
  }

  def !(a: AnyRef) = q.add(a)
  def stop() = running = false
}