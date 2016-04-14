package com.cof.kafka

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
    /*
    it("Is fast") {
      val num = 1000000
      (new LateProducer()).populate(num)
      // println("Check lag...")
      Thread.sleep(1000)

      println("Consuming...")
      val late = LateKafka[String](
        "192.168.99.100:9092",
        "group1",
        "lowercaseStrings",
        new org.apache.kafka.common.serialization.StringDeserializer
      )
      val c1 = LateConsumer(late)
      val f1 = Future(c1.consume(1, num))
      Await.result(f1, 60.seconds)
      late.stop

      println("Done")
    }
    */

    it("Multiplexes") {
      val num = 6 //00000

      (new LateProducer()).populate(num, host, topic)

      /*
      println("Consuming...")
      LateConsumer.reset()
      val c1 = LateConsumerFlow[String](host.replaceFirst("2181", "9092"), group, topic, List(0, 1))
      val f1 = Future(c1.consume(0, num))
      Await.result(f1, 90.seconds)
      Thread.sleep(10000)
      */

      println("Consuming...")
      LateConsumer.reset()
      val c1 = LateConsumerFlow[String](host.replaceFirst("2181", "9092"), group, topic, List(0, 1))
      val c2 = LateConsumerFlow[String](host.replaceFirst("2181", "9092"), group, topic, List(2, 3))
      val clist = List(c1, c2) //, c3, c4)
      println("Running...")
      val f = Future.sequence(clist.zipWithIndex.map { case (c, i) => Future(c.consume(i, num)) })
      Await.result(f, 90.seconds)
      Thread.sleep(10000)
      clist.foreach(l => l.stop())

      println("Done")
    }
  }
}