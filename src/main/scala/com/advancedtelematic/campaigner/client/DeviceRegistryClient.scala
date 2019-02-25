package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.stream.Materializer
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.http.HttpOps.HttpRequestOps
import com.advancedtelematic.libats.http.ServiceHttpClient
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import io.circe.Json
import io.circe.syntax._

import scala.concurrent.{ExecutionContext, Future}

trait DeviceRegistryClient {
  def devicesInGroup(namespace: Namespace,
                     groupId: GroupId,
                     offset: Long,
                     limit: Long): Future[Seq[DeviceId]]

  def createGroup(ns: Namespace, name: String): Future[GroupId]

  def addDeviceToGroup(ns: Namespace, groupId: GroupId, deviceId: DeviceId): Future[Unit]

  def countDevicesInGroups(ns: Namespace, groupIds: Set[GroupId]): Future[Map[GroupId, Int]]
}

class DeviceRegistryHttpClient(uri: Uri, httpClient: HttpRequest => Future[HttpResponse])
    (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer)
    extends ServiceHttpClient(httpClient) with DeviceRegistryClient {

  import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

  private val basePath = uri.path / "api" / "v1"

  override def devicesInGroup(ns: Namespace, groupId: GroupId, offset: Long, limit: Long): Future[Seq[DeviceId]] = {
    val path  = basePath / "device_groups" / groupId.show / "devices"
    val query = Query(Map("offset" -> offset.toString, "limit" -> limit.toString))
    val req = HttpRequest(HttpMethods.GET, uri.withPath(path).withQuery(query)).withNs(ns)
    execHttp[PaginationResult[DeviceId]](req)().map(_.values)
  }

  override def createGroup(ns: Namespace, name: String): Future[GroupId] = {
    val path = basePath / "device_groups"
    val body = Json.obj("name" -> name.asJson, "groupType" -> "static".asJson)
    val req = HttpRequest(HttpMethods.POST, uri.withPath(path)).withEntity(body.asString.get)
    execHttp[GroupId](req)()
  }

  override def addDeviceToGroup(ns: Namespace, groupId: GroupId, deviceId: DeviceId): Future[Unit] = {
    val path = basePath / "device_groups" / groupId.show / "devices" / deviceId.show
    val req = HttpRequest(HttpMethods.POST, uri.withPath(path)).withNs(ns)
    execHttp[Unit](req)()
  }

  override def countDevicesInGroups(ns: Namespace, groupIds: Set[GroupId]): Future[Map[GroupId, Int]] = {
    val path = basePath / "device_groups" / "device-count"
    val query = Query(groupIds.map("groupId" -> _.show).toMap)
    val req = HttpRequest(HttpMethods.GET, uri.withPath(path).withQuery(query))
    execHttp[Map[GroupId, Int]](req)()
  }
}
