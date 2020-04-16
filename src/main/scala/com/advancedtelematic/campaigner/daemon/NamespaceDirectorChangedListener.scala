package com.advancedtelematic.campaigner.daemon

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Status}
import akka.http.scaladsl.model.Uri
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging.kafka.JsonDeserializer
import com.advancedtelematic.libats.messaging_datatype.DataType.{DirectorV1, DirectorV2, DirectorVersion}
import com.advancedtelematic.libats.messaging_datatype.Messages.NamespaceDirectorChanged
import com.typesafe.config.Config
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.concurrent.Future

object NamespaceDirectorChangedListener {
  def props(config: Config, namespaceDirectorConfig: NamespaceDirectorConfig): Props =
    Props(new NamespaceDirectorChangedListener(config, namespaceDirectorConfig))

  def start(config: Config, namespaceDirectorConfig: NamespaceDirectorConfig)(implicit system: ActorSystem): ActorRef =
    system.actorOf(NamespaceDirectorChangedListener.props(config, namespaceDirectorConfig), "namespace-director-changed-listener")
}

class NamespaceDirectorConfig(val conf: Config) extends Settings {
  private val mapping = new ConcurrentHashMap[Namespace, DirectorVersion]()

  def set(ns: Namespace, version: DirectorVersion): Unit = {
    mapping.put(ns, version)
  }

  private def get(ns: Namespace): DirectorVersion = {
    mapping.getOrDefault(ns, DirectorV1)
  }

  def getUri(ns: Namespace): Uri = get(ns) match {
    case DirectorV1 => directorUri
    case DirectorV2 => directorV2Uri
  }
}

class NamespaceDirectorChangedListener(config: Config, namespaceDirectorConfig: NamespaceDirectorConfig) extends Actor with ActorLogging {
  import akka.pattern._
  import context.dispatcher

  implicit val mat = ActorMaterializer()

  import com.advancedtelematic.libats.messaging_datatype.Messages.namespaceDirectorChangedMessageLike
  import com.advancedtelematic.libats.messaging_datatype.MessageCodecs.namespaceDirectorChangedCodec

  override def preStart(): Unit = {
    val consumerSettings = ConsumerSettings(context.system, new ByteArrayDeserializer, new JsonDeserializer[NamespaceDirectorChanged](namespaceDirectorChangedCodec, throwException = true))
      // every instance always consumes whole topic, no commit, unique group id
      .withGroupId(s"ota-plus-server-${namespaceDirectorChangedMessageLike.streamName}-${UUID.randomUUID().toString}")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    context.become(start(consumerSettings))
  }

  private def start(consumerSettings: ConsumerSettings[Array[Byte], NamespaceDirectorChanged]): Receive = {
    val suffix = config.getString("messaging.kafka.topicSuffix")

    val flowResult: Future[Done] = Consumer
      .plainSource(consumerSettings, Subscriptions.topics(namespaceDirectorChangedMessageLike.streamName + "-" + suffix))
      .log("event")(log)
      .map { _.value() }
      .map { msg =>
        namespaceDirectorConfig.set(msg.namespace, msg.director)
        log.info("Set {} for {}", msg.director, msg.namespace)
      }
      .runWith(Sink.ignore)

    flowResult pipeTo self

    active
  }

  val active: Receive = {
    case Status.Failure(t) =>
      log.error(t, "Event stream failed.")
      throw t

    case Done =>
      throw new IllegalStateException("Event stream finished.")
  }

  override def receive: Receive = {
    case msg =>
      log.warning(s"Unexpected msg received: $msg")
      unhandled(msg)
  }
}
