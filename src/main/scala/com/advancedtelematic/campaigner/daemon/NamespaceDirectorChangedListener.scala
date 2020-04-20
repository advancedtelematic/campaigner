package com.advancedtelematic.campaigner.daemon

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, TimeoutException}

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
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.Future

object NamespaceDirectorChangedListener {
  import com.advancedtelematic.libats.messaging_datatype.Messages.namespaceDirectorChangedMessageLike
  import com.advancedtelematic.libats.messaging_datatype.MessageCodecs.namespaceDirectorChangedCodec

  private val _log = LoggerFactory.getLogger(this.getClass)

  def props(config: Config, namespaceDirectorConfig: NamespaceDirectorConfig): Props =
    Props(new NamespaceDirectorChangedListener(config, namespaceDirectorConfig))

  def start(config: Config, namespaceDirectorConfig: NamespaceDirectorConfig)(implicit system: ActorSystem): ActorRef =
    system.actorOf(NamespaceDirectorChangedListener.props(config, namespaceDirectorConfig), "namespace-director-changed-listener")

  def consumeAll(config: Config, namespaceDirectorConfig: NamespaceDirectorConfig, timeout: FiniteDuration = 5.seconds)
                (implicit mat: ActorMaterializer, system: ActorSystem): Future[Done] = {
    import system.dispatcher

    val flow =
      eventStream(config, consumerSettings(), namespaceDirectorConfig)
        .idleTimeout(timeout)
        .runWith(Sink.ignore)

    flow.recover {
      case _: TimeoutException =>
        _log.info("No more namespace change events after {}", timeout)
        Done
    }
  }

  protected def eventStream(config: Config, consumerSettings: ConsumerSettings[Array[Byte], NamespaceDirectorChanged], namespaceDirectorConfig: NamespaceDirectorConfig) = {
    val suffix = config.getString("messaging.kafka.topicSuffix")

    Consumer.plainSource(consumerSettings, Subscriptions.topics(namespaceDirectorChangedMessageLike.streamName + "-" + suffix))
      .log("event", _.value())
      .map { event =>
        val msg = event.value()
        namespaceDirectorConfig.set(msg.namespace, msg.director)
        _log.info("Set {} for {}", msg.director, msg.namespace)
      }

  }

  protected def consumerSettings()(implicit system: ActorSystem) = {
    ConsumerSettings(system, new ByteArrayDeserializer, new JsonDeserializer[NamespaceDirectorChanged](namespaceDirectorChangedCodec, throwException = true))
      // every instance always consumes whole topic, no commit, unique group id
      .withGroupId(s"ota-plus-server-${namespaceDirectorChangedMessageLike.streamName}-${UUID.randomUUID().toString}")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  }
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
  import NamespaceDirectorChangedListener._

  implicit val mat = ActorMaterializer()

  override def preStart(): Unit = {
    context.become(start(consumerSettings()(context.system)))
  }

  private def start(consumerSettings: ConsumerSettings[Array[Byte], NamespaceDirectorChanged]): Receive = {
    eventStream(config, consumerSettings, namespaceDirectorConfig)
      .runWith(Sink.ignore)
      .pipeTo(self)

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
