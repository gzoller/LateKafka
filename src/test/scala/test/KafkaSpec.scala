package co.blocke
package latekafka

import org.scalatest._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }
import scala.sys.process._
import scala.util.Try

class KafkaSpec() extends FunSpec with Matchers with BeforeAndAfterAll {
  def timeme(label: String, fn: () => Unit) {
    val now = System.currentTimeMillis()
    fn()
    val later = System.currentTimeMillis()
    println(s"$label: " + (later - now) / 1000.0)
  }

  val topic = "lowercaseStrings"
  val group = "group1"
  val pwd = System.getProperty("user.dir")
  val extraPath = s"$pwd/src/test/resources/extra"
  var wid = "" // world id
  var dip = "" // docker ip
  var kafkaHost = "" // kafka host
  var zooHost = "" // zookeeper host

  implicit val as = ActorSystem("ReactiveKafka")
  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(as)
      .withAutoFusing(false)
      .withInputBuffer(32, 32)
  )

  val props = Map("auto.offset.reset" -> "earliest")

  // Set server.propertise in /opt/kakfa in world server:
  //advertised.listeners=PLAINTEXTSASL://192.168.99.100:9092  (or whatever 9092 maps to!)
  //#advertised.listeners=PLAINTEXT://your.host.name:9092
  override def beforeAll() {
    wid = worldWait()
    dip = getDockerIP()
    val ips = List(9092, 2181).map(p => s"$dip:" + getDockerPort(wid, p))
    kafkaHost = ips(0)
    zooHost = ips(1)
    println("KAFKA HOST: " + kafkaHost)
    println("ZOO   HOST: " + zooHost)
    Thread.sleep(7000) // let it all come up and settle down in World server
  }

  override def afterAll() {
    as.shutdown()
    s"docker kill $wid".!!
  }

  describe("Kafka Must...") {
    it("It publishes") {
      val num = 10
      (new LateProducer()).populate(num, kafkaHost, zooHost, topic)
      println("-----------------------------")
      partitionInfo(topic)
    }
    it("Is fast") {
      val num = 1000000
      (new LateProducer()).populate(num, kafkaHost, zooHost, topic)
      partitionInfo(topic)

      println("Consuming...")
      LateConsumer.reset()
      val c = LateConsumerFlow[String](kafkaHost, group, topic, props)
      val f = Future(c.consume(1, num))
      val tps = Await.result(f, 15.seconds)
      println(tps + " TPS")
      groupInfo("group1")
      c.stop()
    }

    it("Multiplexes") {
      val num = 1000000

      (new LateProducer()).populate(num, kafkaHost, zooHost, topic)

      println("Consuming...")
      LateConsumer.reset()
      val c1 = LateConsumerFlow[String](kafkaHost, group, topic, props)
      val c2 = LateConsumerFlow[String](kafkaHost, group, topic, props)
      val c3 = LateConsumerFlow[String](kafkaHost, group, topic, props)
      val c4 = LateConsumerFlow[String](kafkaHost, group, topic, props)
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
    kafka.tools.GetOffsetShell.main(Array("--topic", topic, "--broker-list", kafkaHost, "--time", "-1"))
  def groupInfo(group: String) =
    kafka.admin.ConsumerGroupCommand.main(Array("--describe", "--group", group, "--bootstrap-server", kafkaHost, "--new-consumer"))
  def worldWait(limit: Int = 30) = {
    println("++ Starting the World")
    val worldId = s"$pwd/src/test/resources/startWorld.sh $extraPath".!!
    while (cheapGet(s"http://${getDockerIP()}:${getDockerPort(worldId, 80)}/status").isEmpty) { Thread.sleep(500) }
    println("++ World started")
    worldId
  }

  private def cheapGet(uri: String) = Try { scala.io.Source.fromURL(uri, "utf-8").mkString }.toOption
  /** Get a port mapping from Docker.  Must be called only from outside a Docker! Use portster inside. Basically here for testing. */
  private def getDockerPort(containerId: String, port: Int) = (s"docker port $containerId $port".!!).split(':')(1).trim
  /** Get the IP of the active Docker machine.  Must be called only from outside a Docker! Basically here for testing. */
  private def getDockerIP() = {
    val machineName = (Seq("sh", "-c", "docker-machine active 2>/dev/null").!!).trim
    (s"docker-machine ip $machineName".!!).trim
  }
}