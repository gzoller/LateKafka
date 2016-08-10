package co.blocke
package latekafka

import org.scalatest._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.pattern.ask
import akka.actor.{ Props, ActorSystem }
import akka.stream.{ ActorMaterializerSettings, ActorMaterializer }
import akka.util.Timeout
import scala.language.postfixOps
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
  val producer = LateProducer()

  implicit val as = ActorSystem("ReactiveKafka")
  implicit val m = ActorMaterializer(
    ActorMaterializerSettings(as)
      .withAutoFusing(false)
      .withInputBuffer(32, 32)
  )

  val props = Map("auto.offset.reset" -> "earliest")

  // Set server.propertise in /opt/kakfa in world server:
  override def beforeAll() {
    wid = worldWait()
    dip = getDockerIP()
    val ips = List(9092, 2181).map(p => s"$dip:" + getDockerPort(wid, p))
    kafkaHost = ips(0)
    zooHost = ips(1)
    println("KAFKA HOST: " + kafkaHost)
    println("ZOO   HOST: " + zooHost)
    Thread.sleep(9000) // let it all come up and settle down in World server
    producer.create(kafkaHost, zooHost, topic)
  }

  override def afterAll() {
    as.shutdown()
    s"docker kill $wid".!!
  }

  describe("Kafka Must...") {
    it("Is fast") {
      val num = 1000000
      producer.populate(num, topic)
      Thread.sleep(2000)
      partitionInfo(topic)

      println("Consuming...")
      Aggregator.reset()
      val sg = SpeedGraph(kafkaHost, "speed", topic, Map("auto.offset.reset" -> "earliest"))
      val sa = as.actorOf(Props(new SpeedActor(num, sg)), "sa")

      implicit val timeout = Timeout(55 seconds)
      val tps = Try { Await.result(sa ? num, 55.seconds) }.toOption.getOrElse(0)

      Thread.sleep(10000)
      println(tps + " TPS")
      groupInfo("speed")
      as.stop(sa)
      Thread.sleep(3000)
    }

    it("Multiplexes") {
      val num = 1000000

      println("Consuming...")

      Aggregator.reset()
      val c = (0 to 3).map { i =>
        println("Make " + i)
        val sg = SpeedGraph(kafkaHost, "multiplex", topic, Map("auto.offset.reset" -> "earliest"))
        as.actorOf(Props(new SpeedActor(num, sg)))
      }
      println("Let's get started...")
      implicit val timeout = Timeout(25 seconds)
      val futures = c.map(_ ? num)
      val f = Future.sequence(futures)

      val tps = Try { Await.result(f, 25.seconds) }.toOption.getOrElse(List(0, 0, 0, 0))
      Thread.sleep(10000)
      println(tps + " TPS")
      groupInfo("multiplex")
      c.foreach(as.stop(_))
      Thread.sleep(3000)
    }

    it("Can FanOut") {
      val fg = FilterGraph(kafkaHost, "filter", topic, Map())
      val fa = as.actorOf(Props(new FlowActor(fg)), "fa")
      Thread.sleep(3000)

      println("Posting")
      producer.enqueue(topic, Json.serializeToString[Message[MessagePayload]](Message(Map.empty[String, Any], MyThing("Gregory"))))
      producer.enqueue(topic, Json.serializeToString[Message[MessagePayload]](Message(Map.empty[String, Any], MyThing("David"))))
      producer.enqueue(topic, Json.serializeToString[Message[MessagePayload]](Message(Map.empty[String, Any], MyThing("Graham"))))
      producer.enqueue(topic, Json.serializeToString[Message[MessagePayload]](Message(Map.empty[String, Any], MyThing("Katie"))))
      producer.enqueue(topic, Json.serializeToString[Message[MessagePayload]](Message(Map.empty[String, Any], MyThing("Garth"))))

      println("Waiting...")
      Thread.sleep(5000)
      partitionInfo(topic)
      println("Time's up!")
      as.stop(fa)
      Thread.sleep(2000)
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