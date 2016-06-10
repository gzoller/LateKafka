package co.blocke
package latekafka

import scala.collection.JavaConversions._
import org.apache.kafka.clients.producer.{ ProducerRecord, KafkaProducer }
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.I0Itec.zkclient.exception.ZkMarshallingError
private object ZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes: Array[Byte]): Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}

case class LateProducer() {

  def populate(num: Int, kafkaHost: String, zookeeper: String, topic: String) {

    val numPartitions = 4
    val replicationFactor = 1
    val topicConfig = new java.util.Properties
    val sessionTimeoutMs = 10000
    val connectionTimeoutMs = 10000

    val zkClient = ZkUtils(zookeeper, sessionTimeoutMs, connectionTimeoutMs, false)
    try {
      AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, topicConfig)
    } catch {
      case k: kafka.common.TopicExistsException => // do nothing...topic exists
      case t: Throwable                         => println("Boom: " + t)
    }
    zkClient.close()

    Thread.sleep(2000)

    val props = Map(
      "bootstrap.servers" -> kafkaHost,
      "key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    val p = new KafkaProducer[Array[Byte], String](props)
    (1 to num).foreach { i =>
      p.send(new ProducerRecord[Array[Byte], String](topic, s"""{"name":"Fido","id":$i}""")) // Structure message matching Pet object!
    }
    p.flush()
    p.close()
    println("Population complete: " + num)
  }
}
