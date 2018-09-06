package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import com.advancedtelematic.campaigner.data.DataType.ExternalUpdateId
import com.advancedtelematic.libats.codecs.CirceCodecs._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.http.HttpOps.HttpRequestOps
import com.advancedtelematic.libats.http.ServiceHttpClient
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.Future

trait ResolverClient {
  def availableUpdatesFor(resolverUri: Uri, ns: Namespace, devices: Seq[DeviceId]): Future[Seq[ExternalUpdateId]]
}

class ResolverHttpClient(httpClient: HttpRequest => Future[HttpResponse])
                        (implicit system: ActorSystem, mat: Materializer)
  extends ServiceHttpClient(httpClient) with ResolverClient {

  override def availableUpdatesFor(resolverUri: Uri, ns: Namespace, devices: Seq[DeviceId]): Future[Seq[ExternalUpdateId]] = {
    val query = Uri.Query(Map("ids" -> devices.map(_.uuid).mkString(",")))
    val request = HttpRequest(HttpMethods.GET, resolverUri.withQuery(query)).withNs(ns)
    execHttp[Seq[ExternalUpdateId]](request)()
  }
}
