package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import com.advancedtelematic.campaigner.data.DataType.ExternalUpdateId
import com.advancedtelematic.libats.codecs.CirceCodecs._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.{ExecutionContext, Future}

trait ResolverClient {
  def availableUpdatesFor(resolverUri: Uri, ns: Namespace, devices: Seq[DeviceId]): Future[Seq[ExternalUpdateId]]
}

class ResolverHttpClient()(implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer)
  extends HttpClient("external_resolver", Uri./) with ResolverClient {

  override def availableUpdatesFor(resolverUri: Uri, ns: Namespace, devices: Seq[DeviceId]): Future[Seq[ExternalUpdateId]] = {
    val query = Uri.Query(Map("ids" -> devices.map(_.uuid).mkString(",")))
    val request = HttpRequest(HttpMethods.GET, resolverUri.withQuery(query))
    execHttp[Seq[ExternalUpdateId]](ns, request)
  }
}
