package com.cof.kafka

import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.{ ProducerRecord, KafkaProducer }
import kafka.admin.AdminUtils
// import org.I0Itec.zkclient.ZkClient
import kafka.utils.ZkUtils

case class LateProducer() {

  val LCS = "lowercaseStrings"

  def populate(num: Int) {

    // Create a topic named "myTopic" with 8 partitions and a replication factor of 3
    val topicName = LCS
    val numPartitions = 4
    val replicationFactor = 1
    val topicConfig = new java.util.Properties
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000

    val zkClient = ZkUtils("192.168.99.100:2181", sessionTimeoutMs, connectionTimeoutMs, false)
    AdminUtils.createTopic(zkClient, topicName, numPartitions, replicationFactor, topicConfig)

    val props = Map(
      "bootstrap.servers" -> "192.168.99.100:9092",
      "key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val p = new KafkaProducer[Array[Byte], String](props)
    var part = 0
    (1 to num).foreach { i =>
      p.send(new ProducerRecord[Array[Byte], String](LCS, s"msg-$i"))
      part += 1
      if (part == 4) part = 0
    }
    p.close()

    println("Population complete: " + num)
  }
}