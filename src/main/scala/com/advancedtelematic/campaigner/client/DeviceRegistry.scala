package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.Namespace
import de.heikoseeberger.akkahttpcirce.CirceSupport._
import scala.concurrent.{ExecutionContext, Future}

trait DeviceRegistry {
  def getDevicesInGroup(namespace: Namespace,
                        groupId: GroupId,
                        offset: Int,
                        limit: Int): Future[Seq[DeviceId]]
}

class DeviceRegistryClient(uri: Uri)
    (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer)
    extends HttpClient("device_registry", uri) with DeviceRegistry {

  override def getDevicesInGroup(namespace: Namespace,
                                 groupId: GroupId,
                                 offset: Int,
                                 limit: Int): Future[Seq[DeviceId]] = {
    val path  = uri.path / "api" / "v1" / "device_groups" / groupId.show / "devices"
    val query = Uri.Query(Map("offset" -> offset.toString, "limit" -> limit.toString))
    val req   = HttpRequest(HttpMethods.GET, uri = uri.withPath(path).withQuery(query))
    execHttp[Seq[DeviceId]](namespace, req)
  }

}
