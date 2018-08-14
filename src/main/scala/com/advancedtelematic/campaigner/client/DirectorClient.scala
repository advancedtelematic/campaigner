package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType.Vin
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import scala.concurrent.{ExecutionContext, Future}

import shapeless._

trait Resolver {
  type SeqDevices = Seq[(DeviceId, Vin)]

  def findAffected(ns: Namespace, updateId: UpdateId, devices: SeqDevices): Future[SeqDevices]

  def setMultiUpdateTarget(ns: Namespace, update: UpdateId, devices: SeqDevices): Future[SeqDevices]
}

class DirectorResolver(directorClient: DirectorClient) extends Resolver  {

  override def findAffected(ns: Namespace, updateId: UpdateId, devices: SeqDevices): Future[SeqDevices] = {
    val ss  = devices match {
      case Inl(o) => o
      case _ => throw new IllegalArgumentException("")
    }

    ???
  }

  override def setMultiUpdateTarget(ns: Namespace, update: UpdateId, devices: SeqDevices): Future[SeqDevices] = ???
}

trait DirectorClient {
  def setMultiUpdateTarget(
    ns: Namespace,
    update: UpdateId,
    devices: Seq[DeviceId]): Future[Seq[DeviceId]]

  def findAffected(ns: Namespace, updateId: UpdateId, devices: Seq[DeviceId]): Future[Seq[DeviceId]]

  def cancelUpdate(
    ns: Namespace,
    devices: Seq[DeviceId]): Future[Seq[DeviceId]]

  def cancelUpdate(
    ns: Namespace,
    device: DeviceId): Future[Unit]
}

class DirectorHttpClient(uri: Uri)
    (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer)
    extends HttpClient("director", uri) with DirectorClient {

  import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
  import io.circe.syntax._

  override def setMultiUpdateTarget(
    ns: Namespace,
    update: UpdateId,
    devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {
    val path   = uri.path / "api" / "v1" / "admin" / "multi_target_updates" / update.show
    val entity = HttpEntity(ContentTypes.`application/json`, devices.asJson.noSpaces)
    val req    = HttpRequest(
      method = HttpMethods.PUT,
      uri    = uri.withPath(path),
      entity = entity
    )
    execHttp[Seq[DeviceId]](ns, req)
  }

  override def cancelUpdate(
    ns: Namespace,
    devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {

    val path   = uri.path / "api" / "v1" / "admin" / "devices" / "queue" / "cancel"
    val entity = HttpEntity(ContentTypes.`application/json`, devices.asJson.noSpaces)
    val req    = HttpRequest(
      method = HttpMethods.PUT,
      uri    = uri.withPath(path),
      entity = entity
    )
    execHttp[Seq[DeviceId]](ns, req)
  }

  override def cancelUpdate(
    ns: Namespace,
    device: DeviceId): Future[Unit] = {

    val path = uri.path / "api" / "v1" / "admin" / "devices" / device.show / "queue" / "cancel"
    val req  = HttpRequest(
      method = HttpMethods.PUT,
      uri    = uri.withPath(path),
    )
    execHttp[Unit](ns, req)
  }

  override def findAffected(ns: Namespace, updateId: UpdateId, devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {
    val path   = uri.path / "api" / "v1" / "admin" / "multi_target_updates" / updateId.show / "affected"
    val entity = HttpEntity(ContentTypes.`application/json`, devices.asJson.noSpaces)
    val req    = HttpRequest(
      method = HttpMethods.GET,
      uri    = uri.withPath(path),
      entity = entity
    )
    execHttp[Seq[DeviceId]](ns, req)
  }
}
