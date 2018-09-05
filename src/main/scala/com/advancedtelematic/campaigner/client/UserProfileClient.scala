package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.stream.Materializer
import com.advancedtelematic.campaigner.data.Codecs.uriDecoder
import com.advancedtelematic.libats.data.DataType.Namespace
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.{ExecutionContext, Future}

trait UserProfileClient {
  def externalResolverUri(ns: Namespace): Future[Uri]
}

class UserProfileHttpClient(uri: Uri)(implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer)
  extends HttpClient("user_profile", uri) with UserProfileClient {

  override def externalResolverUri(ns: Namespace): Future[Uri] = {
    val path = uri.path / "api" / "v1" / "namespace_settings" / ns.get
    val request = HttpRequest(HttpMethods.GET, uri.withPath(path))
    execHttp[Uri](ns, request)
  }

}
