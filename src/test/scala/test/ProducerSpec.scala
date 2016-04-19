package com.cof.kafka

import org.scalatest._
import org.apache.kafka.clients.consumer.{ ConsumerRebalanceListener, OffsetCommitCallback, ConsumerRecord, KafkaConsumer, OffsetAndMetadata }
import scala.collection.JavaConversions._
import org.apache.kafka.common.TopicPartition
import java.util.Collection

class ProducerSpec() extends FunSpec with Matchers {
  def timeme(label: String, fn: () => Unit) {
    val now = System.currentTimeMillis()
    fn()
    val later = System.currentTimeMillis()
    println(s"$label: " + (later - now) / 1000.0 + " ms")
  }

  val host = "192.168.99.100:9092"
  val topic = "lowercaseStrings"
  val group = "group1"
  val num = 1200000

  describe("Kafka Producer Must...") {
    it("Single Producer") {
      val p = new LateProducer()
      timeme(s"Populate $num", () => {
        p.populate(num, host, topic)
      })
      partitionInfo(topic)
    }
    ignore("Single Consumer - Single Commit") {
      val consumer = new KafkaConsumer[Array[Byte], String](
        (Map.empty[String, String] ++ Map(
          "bootstrap.servers" -> host,
          "enable.auto.commit" -> "false",
          "auto.commit.interval.ms" -> "1000",
          "auto.offset.reset" -> "earliest",
          "group.id" -> group
        )),
        new org.apache.kafka.common.serialization.ByteArrayDeserializer,
        new org.apache.kafka.common.serialization.StringDeserializer
      )
      consumer.subscribe(List(topic), new ConsumerRebalanceListener() {
        def onPartitionsRevoked(partitions: Collection[TopicPartition]) { println("Revoked: " + partitions) }
        def onPartitionsAssigned(partitions: Collection[TopicPartition]) { println("Assigned: " + partitions) }
      })
      var done = false
      var c = 0
      timeme(s"Consume $num", () => {
        while (!done) {
          val i = consumer.poll(100).iterator
          var off = 0L
          while (i.hasNext()) {
            val cr = i.next()
            off = cr.offset() + 1
            consumer.commitAsync(java.util.Collections.singletonMap(
              new TopicPartition(cr.topic(), cr.partition()),
              new OffsetAndMetadata(off)
            ), new OffsetCommitCallback {
              def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], ex: Exception) {
                c += 1
                if (ex != null) {
                  print(s"ERROR [$offsets]:  ")
                  ex.printStackTrace()
                }
                if (c == num) done = true
              }
            })
          }
        }
      })
      partitionInfo(topic)
    }

    it("Single Consumer - Batch Commit") {
      val consumer = new KafkaConsumer[Array[Byte], String](
        (Map.empty[String, String] ++ Map(
          "bootstrap.servers" -> host,
          "enable.auto.commit" -> "false",
          "auto.commit.interval.ms" -> "1000",
          "auto.offset.reset" -> "earliest",
          "group.id" -> group
        )),
        new org.apache.kafka.common.serialization.ByteArrayDeserializer,
        new org.apache.kafka.common.serialization.StringDeserializer
      )
      consumer.subscribe(List(topic), new ConsumerRebalanceListener() {
        def onPartitionsRevoked(partitions: Collection[TopicPartition]) { println("Revoked: " + partitions) }
        def onPartitionsAssigned(partitions: Collection[TopicPartition]) { println("Assigned: " + partitions) }
      })
      var done = false
      var c = 0
      timeme(s"Consume $num", () => {
        while (!done) {
          val i = consumer.poll(100).iterator
          var off = 0L
          while (i.hasNext()) {
            val cr = i.next()
            off = cr.offset() + 1
            c += 1
          }
          consumer.commitAsync()
          if (c == num) done = true
        }
      })
      partitionInfo(topic)
    }
  }

  def partitionInfo(topic: String) = {
    kafka.admin.ConsumerGroupCommand.main(Array("--describe", "--group", group, "--bootstrap-server", "192.168.99.100:9092", "--new-consumer"))
  }
}