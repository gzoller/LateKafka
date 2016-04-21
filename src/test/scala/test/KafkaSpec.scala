package co.blocke
package latekafka

import org.scalatest._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }

class KafkaSpec() extends FunSpec with Matchers with BeforeAndAfterAll {
  def timeme(label: String, fn: () => Unit) {
    val now = System.currentTimeMillis()
    fn()
    val later = System.currentTimeMillis()
    println(s"$label: " + (later - now) / 1000.0)
  }

  val host = "192.168.99.100:9092"
  val topic = "lowercaseStrings"
  val group = "group1"

  implicit val as = ActorSystem("ReactiveKafka")
  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(as)
      .withAutoFusing(false)
      .withInputBuffer(32, 32)
  )

  override def afterAll() {
    as.shutdown()
  }

  describe("Kafka Must...") {
    it("Is fast") {
      val num = 1000000
      (new LateProducer()).populate(num, host, topic)
      partitionInfo(topic)

      println("Consuming...")
      LateConsumer.reset()
      val c = LateConsumerFlow[String](host.replaceFirst("2181", "9092"), group, topic)
      val f = Future(c.consume(1, num))
      val tps = Await.result(f, 15.seconds)
      println(tps + " TPS")
      groupInfo("group1")
      c.stop()
    }

    it("Multiplexes") {
      val num = 1000000

      (new LateProducer()).populate(num, host, topic)

      println("Consuming...")
      LateConsumer.reset()
      val c1 = LateConsumerFlow[String](host.replaceFirst("2181", "9092"), group, topic)
      val c2 = LateConsumerFlow[String](host.replaceFirst("2181", "9092"), group, topic)
      val c3 = LateConsumerFlow[String](host.replaceFirst("2181", "9092"), group, topic)
      val c4 = LateConsumerFlow[String](host.replaceFirst("2181", "9092"), group, topic)
      val clist = List(c1, c2, c3, c4)
      val f = Future.sequence(clist.zipWithIndex.map { case (c, i) => Future(c.consume(i, num)) })
      val tps = Await.result(f, 40.seconds)
      println(tps + " TPS")
      groupInfo("group1")
      clist.foreach(l => l.stop())

      println("Done")
    }
  }

  def partitionInfo(topic: String) =
    kafka.tools.GetOffsetShell.main(Array("--topic", topic, "--broker-list", "192.168.99.100:9092", "--time", "-1"))
  def groupInfo(group: String) =
    kafka.admin.ConsumerGroupCommand.main(Array("--describe", "--group", group, "--bootstrap-server", "192.168.99.100:9092", "--new-consumer"))
}