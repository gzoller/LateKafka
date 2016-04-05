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
      val num = 10000
      (new LateProducer()).populate(num)
      Thread.sleep(1000)

      println("Consuming...")
      LateConsumer.reset()
      val c1 = LateConsumer[String](List(0))
      val c2 = LateConsumer[String](List(1))
      println("Running...")
      val f1 = Future(c1.consume(1, num))
      val f2 = Future(c2.consume(2, num))
      Await.result(f1, 60.seconds)
      Await.result(f2, 60.seconds)
      c1.stop()
      c2.stop()

      println("Done")
    }
  }
}