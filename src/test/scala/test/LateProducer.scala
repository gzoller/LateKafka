package com.cof.kafka

import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.{ ProducerRecord, KafkaProducer, Callback, RecordMetadata }
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils

case class LateProducer() {

  def populate(num: Int, host: String, topic: String) {

    // Create a topic named "myTopic" with 8 partitions and a replication factor of 3
    val numPartitions = 4
    val replicationFactor = 1
    val topicConfig = new java.util.Properties
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000

    val zkClient = ZkUtils(host.replaceFirst("9092", "2181"), sessionTimeoutMs, connectionTimeoutMs, false)
    try {
      AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, topicConfig)
    } catch {
      case k: kafka.common.TopicExistsException => // do nothing...topic exists
    }

    val props = Map(
      "bootstrap.servers" -> host,
      "key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    println("::: Sending :::")
    val p = new KafkaProducer[Array[Byte], String](props)
    (1 to num).foreach { i =>
      println("  ::: Writing " + i + " ::::")
      p.send(new ProducerRecord[Array[Byte], String](topic, s"msg-$i"), new Callback() {
        def onCompletion(metadata: RecordMetadata, e: Exception) {
          if (e != null)
            e.printStackTrace()
          println("The offset of the record we just sent is: " + metadata)
        }
      })
    }
    println("::: Done Sending - Sleeping :::")
    Thread.sleep(5000)
    println("preclose")
    try {
      p.close(10000, java.util.concurrent.TimeUnit.MILLISECONDS)
    } catch {
      case t: Throwable => t.printStackTrace()
    }
    println("postclose")

    println("Population complete: " + num)
  }
}