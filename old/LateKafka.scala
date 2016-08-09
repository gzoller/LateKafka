package co.blocke
package latekafka

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
    properties:   Map[String, String] = Map.empty[String, String]
) extends Iterator[ConsumerRecord[Array[Byte], V]] {

  type REC = ConsumerRecord[Array[Byte], V]
  type ITER_REC = Iterator[REC]

  private val t = KafkaThread[V](host, groupId, topic, deserializer, properties)
  private val h = Heartbeat(t, 100L)
  private var i: ITER_REC = null
  private var hasMore = true

  new java.lang.Thread(t).start
  new java.lang.Thread(h).start
  Thread.sleep(500)

  def done() = hasMore = false
  def stop() = {
    t.stop()
    h.stop()
  }

  def hasNext = hasMore
  def next() = {
    while (i.isEmpty || !i.hasNext)
      fill()
    i.next
  }

  def commit(cr: REC) = t ! cr
  def source = {
    fill()
    Source.fromIterator(() => this)
  }

  private def fill() = {
    val p = Promise[ITER_REC]()
    val f = p.future
    t ! p
    i = Await.result(f, Duration.Inf).asInstanceOf[ITER_REC]
  }
}
