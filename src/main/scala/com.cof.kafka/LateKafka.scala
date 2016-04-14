package com.cof.kafka

import akka.stream.scaladsl.Source
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.concurrent.{ Promise, Await }
import scala.concurrent.duration._

case class LateKafka[V](
    host:         String,
    groupId:      String,
    topic:        String,
    deserializer: Deserializer[V],
    partitions:   List[Int]           = List(0),
    properties:   Map[String, String] = Map.empty[String, String]
) extends Iterator[ConsumerRecord[Array[Byte], V]] {

  type REC = ConsumerRecord[Array[Byte], V]
  type ITER_REC = Iterator[REC]

  private val t = KafkaThread[V](host, groupId, topic, deserializer, partitions, properties)
  private var i: ITER_REC = null
  private var hasMore = true

  new java.lang.Thread(t).start
  Thread.sleep(500)
  fill()

  def done() = hasMore = false
  def stop() = t.stop()

  def hasNext = hasMore
  def next() = {
    while (i.isEmpty || !i.hasNext)
      fill()
    i.next
  }

  def commit(cr: REC) = t ! cr
  def source = Source.fromIterator(() => this)

  private def fill() = {
    val p = Promise[ITER_REC]()
    val f = p.future
    t ! p
    i = Await.result(f, Duration.Inf).asInstanceOf[ITER_REC]
  }
}