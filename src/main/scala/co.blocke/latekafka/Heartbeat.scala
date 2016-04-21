package co.blocke
package latekafka

case class Heartbeat[V](kt: KafkaThread[V], delayMS: Long) extends Runnable {

  private var running = true

  def run() {
    while (running) {
      Thread.sleep(delayMS)
      kt !! "c"
    }
  }
  def stop() = running = false
}