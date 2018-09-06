package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType.ExternalUpdateId
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.http.HttpOps.HttpRequestOps
import com.advancedtelematic.libats.http.ServiceHttpClient
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId

import scala.concurrent.Future

trait DirectorClient {

  def setMultiUpdateTarget(ns: Namespace, updateId: ExternalUpdateId, devices: Seq[DeviceId]): Future[Seq[DeviceId]]

  def findAffected(ns: Namespace, updateId: ExternalUpdateId, devices: Seq[DeviceId]): Future[Seq[DeviceId]]

  def cancelUpdate(
    ns: Namespace,
    devices: Seq[DeviceId]): Future[Seq[DeviceId]]

  def cancelUpdate(
    ns: Namespace,
    device: DeviceId): Future[Unit]
}

class DirectorHttpClient(uri: Uri, httpClient: HttpRequest => Future[HttpResponse])
    (implicit system: ActorSystem, mat: Materializer)
    extends ServiceHttpClient(httpClient) with DirectorClient {

  import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
  import io.circe.syntax._

  override def setMultiUpdateTarget(
    ns: Namespace,
    updateId: ExternalUpdateId,
    devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {
    val path   = uri.path / "api" / "v1" / "admin" / "multi_target_updates" / updateId.value
    val entity = HttpEntity(ContentTypes.`application/json`, devices.asJson.noSpaces)
    val req = HttpRequest(HttpMethods.PUT, uri.withPath(path), entity = entity).withNs(ns)
    execHttp[Seq[DeviceId]](req)()
  }

  override def cancelUpdate(
    ns: Namespace,
    devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {

    val path   = uri.path / "api" / "v1" / "admin" / "devices" / "queue" / "cancel"
    val entity = HttpEntity(ContentTypes.`application/json`, devices.asJson.noSpaces)
    val req = HttpRequest(HttpMethods.PUT, uri.withPath(path), entity = entity).withNs(ns)
    execHttp[Seq[DeviceId]](req)()
  }

  override def cancelUpdate(
    ns: Namespace,
    device: DeviceId): Future[Unit] = {

    val path = uri.path / "api" / "v1" / "admin" / "devices" / device.show / "queue" / "cancel"
    val req = HttpRequest(HttpMethods.PUT, uri.withPath(path)).withNs(ns)
    execHttp[Unit](req)()
  }

  override def findAffected(ns: Namespace, updateId: ExternalUpdateId, devices: Seq[DeviceId]): Future[Seq[DeviceId]] = {
    val path   = uri.path / "api" / "v1" / "admin" / "multi_target_updates" / updateId.value / "affected"
    val entity = HttpEntity(ContentTypes.`application/json`, devices.asJson.noSpaces)
    val req = HttpRequest(HttpMethods.GET, uri.withPath(path), entity = entity).withNs(ns)
    execHttp[Seq[DeviceId]](req)()
  }
}
