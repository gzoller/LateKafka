package com.cof.kafka

import org.scalatest._

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

  describe("Kafka Producer Must...") {
    it("Single Producer") {
      val num = 4 //1000000
      val p = new LateProducer()
      timeme(s"Populate $num", () => {
        p.populate(num, host, topic)
      })
      partitionInfo(topic)
    }
  }

  def partitionInfo(topic: String) = {
    kafka.tools.GetOffsetShell.main(Array("--broker-list", host, "--time", "-1", "--topic", topic))
  }
}