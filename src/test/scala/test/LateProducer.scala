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
    zkClient.close()

    Thread.sleep(2000)

    val props = Map(
      "bootstrap.servers" -> host,
      "key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val p = new KafkaProducer[Array[Byte], String](props)
    (1 to num).foreach { i =>
      // println(s">>> Sending $i")
      p.send(new ProducerRecord[Array[Byte], String](topic, s"msg-$i")) /*, new Callback() {
        def onCompletion(metadata: RecordMetadata, exception: java.lang.Exception) {
          println("----------------------------------------")
          println("Generated: " + metadata)
          println("Exception: " + exception)
        }
      })*/
    }
    p.flush()
    p.close()
    println("Population complete: " + num)
  }
}

/* OUTPUT:

[pool-10-thread-4-ScalaTest-running-KafkaSpec] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka version : 0.10.1.0-SNAPSHOT
[pool-10-thread-4-ScalaTest-running-KafkaSpec] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka commitId : 065ddf90195e0968
>>> Sending 1
[kafka-producer-network-thread | producer-1] WARN org.apache.kafka.clients.NetworkClient - Error while fetching metadata with correlation id 0 : {lowercaseStrings=LEADER_NOT_AVAILABLE}
----------------------------------------
Generated: null
Exception: org.apache.kafka.common.errors.TimeoutException: Failed to update metadata after 59856 ms.
>>> Sending 2
----------------------------------------
Generated: null
Exception: org.apache.kafka.common.errors.TimeoutException: Failed to update metadata after 60000 ms.
>>> Sending 3

*/ 