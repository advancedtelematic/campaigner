package com.advancedtelematic.campaigner.client

import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.data.DataType.ExternalUpdateId
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId

import scala.concurrent.Future

trait Resolver {
  def availableUpdatesFor(devices: Seq[DeviceId]): Future[Seq[ExternalUpdateId]]
}

class NoOpResolver extends Resolver {
  override def availableUpdatesFor(devices: Seq[DeviceId]): Future[Seq[ExternalUpdateId]] =
    FastFuture.successful(Seq.empty)
}
