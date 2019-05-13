package com.advancedtelematic.campaigner.util

import com.advancedtelematic.libats.messaging.MessageBusPublisher
import com.advancedtelematic.libats.messaging_datatype.MessageLike
import scala.concurrent.{ExecutionContext, Future}

final class TestMessageBus extends MessageBusPublisher {
  var messages = Seq.empty[Any]

  override def publish[T](msg: T)(implicit ex: ExecutionContext, messageLike: MessageLike[T]): Future[Unit] = {
    messages = messages :+ msg
    Future.successful(())
  }
}
