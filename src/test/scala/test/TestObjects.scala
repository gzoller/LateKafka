package co.blocke
package latekafka

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.util.UUID
import org.apache.kafka.common.serialization.{ Deserializer, StringDeserializer }
import co.blocke.scalajack.json.JsonKind
import co.blocke.scalajack.{ CustomReadRender, ScalaJack, VisitorContext }
import scala.reflect.runtime.universe.TypeTag

case class Message[T <: MessagePayload](
  id:        UUID,
  timestamp: OffsetDateTime,
  metadata:  Map[String, Any],
  payload:   T
)

object Message {
  def apply[T <: MessagePayload](metadata: Map[String, Any], payload: T): Message[MessagePayload] =
    Message(UUID.randomUUID(), OffsetDateTime.now, metadata, payload)
}

sealed trait MessagePayload {
  val name: String
  override def toString() = name
}

trait CommandPayload extends MessagePayload

trait EventPayload extends MessagePayload

case class UnknownPayload(name: String) extends MessagePayload

class MessageDeserializer extends Deserializer[Message[MessagePayload]] {
  private val stringDeserializer = new StringDeserializer

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = {
    stringDeserializer.configure(configs, isKey)
  }
  override def close(): Unit = {
    stringDeserializer.close()
  }
  override def deserialize(topic: String, data: Array[Byte]): Message[MessagePayload] = {
    Json.deserializeTo[Message[MessagePayload]](stringDeserializer.deserialize(topic, data))
  }
}

object Json {

  val offsetDateTimeHandler = CustomReadRender(
    {
      case (_: JsonKind, valueFromJson: String) ⇒
        OffsetDateTime.parse(valueFromJson, ISO_OFFSET_DATE_TIME)
    },
    {
      case (_: JsonKind, offsetDateTime: OffsetDateTime) ⇒
        s""""${offsetDateTime.format(ISO_OFFSET_DATE_TIME)}""""
    }
  )

  val visitorContext = VisitorContext(
    hintMap        = Map("default" → "kind"),
    customHandlers = Map(
      classOf[OffsetDateTime].getName → offsetDateTimeHandler
    ),
    parseOrElse    = Map(
      classOf[MessagePayload].getName → classOf[UnknownPayload].getName
    )
  )

  val scalaJack = ScalaJack[String]()

  def serializeToString[T](value: T)(implicit valueTypeTag: TypeTag[T]): String = {
    scalaJack.render[T](value, visitorContext)
  }

  def deserializeTo[T](json: String)(implicit valueTypeTag: TypeTag[T]): T = {
    scalaJack.read[T](json, visitorContext)
  }

}

case class MyThing(name: String) extends CommandPayload