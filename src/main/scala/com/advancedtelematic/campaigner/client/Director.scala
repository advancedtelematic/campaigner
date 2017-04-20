package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import cats.syntax.show._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.Namespace
import scala.concurrent.{ExecutionContext, Future}

trait Director {

  def setMultiUpdateTarget(ns: Namespace,
                           device: DeviceId,
                           update: UpdateId): Future[Unit]
}

class DirectorClient(uri: Uri)
    (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer)
    extends HttpClient("director", uri) with Director {

  import de.heikoseeberger.akkahttpcirce.CirceSupport._

  override def setMultiUpdateTarget(ns: Namespace,
                                    device: DeviceId,
                                    update: UpdateId): Future[Unit] = {
    val req = HttpRequest(
      method = HttpMethods.POST,
      uri    = uri.withPath(uri.path / "api" / "v1" / "admin" / "devices" / device.show / "multi_target_update" / update.show)
    )
    execHttp[Unit](ns, req).map(_ => ())
  }

}
