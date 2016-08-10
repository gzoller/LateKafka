package co.blocke
package latekafka

case class Heartbeat[V](kt: KafkaThread[V], delayMS: Long) extends Runnable {

  private var running = true

  def run() {
    var clock = 0L
    while (running) {
      Thread.sleep(delayMS)
      kt !! "c"

      clock = clock + delayMS
      if (clock >= 3000)
        kt ! "dummy"
    }
  }
  def stop() = running = false
}