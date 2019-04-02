package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import com.advancedtelematic.libats.http.HttpOps.HttpRequestOps
import com.advancedtelematic.libats.http.ServiceHttpClient
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import io.circe.Decoder

import scala.concurrent.{ExecutionContext, Future}

trait DeviceRegistryClient {
  def devicesInGroup(namespace: Namespace,
                     groupId: GroupId,
                     offset: Long,
                     limit: Long): Future[Seq[DeviceId]]
  def fetchOemId(ns: Namespace, deviceIds: DeviceId): Future[String]
}

class DeviceRegistryHttpClient(uri: Uri, httpClient: HttpRequest => Future[HttpResponse])
    (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer)
    extends ServiceHttpClient(httpClient) with DeviceRegistryClient {

  import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

  override def devicesInGroup(ns: Namespace, groupId: GroupId, offset: Long, limit: Long): Future[Seq[DeviceId]] = {
    val path  = uri.path / "api" / "v1" / "device_groups" / groupId.show / "devices"
    val query = Uri.Query(Map("offset" -> offset.toString, "limit" -> limit.toString))
    val req = HttpRequest(HttpMethods.GET, uri.withPath(path).withQuery(query)).withNs(ns)
    execHttp[PaginationResult[DeviceId]](req)().map(_.values)
  }

  override def fetchOemId(ns: Namespace, deviceId: DeviceId): Future[String] = {
    val path  = uri.path / "api" / "v1" / "devices" / deviceId.show
    val req = HttpRequest(HttpMethods.GET, uri.withPath(path)).withNs(ns)
    implicit val um = unmarshaller[String](Decoder.instance(_.get("deviceId")))
    execHttp[String](req)()
  }
}
