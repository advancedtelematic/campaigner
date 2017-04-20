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
  def getDevicesInGroup(namespace: Namespace, groupId: GroupId): Future[Seq[DeviceId]]
}

class DeviceRegistryClient(uri: Uri)
    (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer)
    extends HttpClient("device_registry", uri) with DeviceRegistry {

  override def getDevicesInGroup(namespace: Namespace, groupId: GroupId): Future[Seq[DeviceId]] = {
    val req = HttpRequest(HttpMethods.GET,
                          uri = uri.withPath(uri.path / "api" / "v1" / "device_groups" / groupId.show / "devices"))
    execHttp[Seq[DeviceId]](namespace, req)
  }

}
