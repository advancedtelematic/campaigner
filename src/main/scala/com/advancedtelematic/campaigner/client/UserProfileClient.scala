package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.Materializer
import com.advancedtelematic.campaigner.data.Codecs.uriDecoder
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.http.Errors.RemoteServiceError
import com.advancedtelematic.libats.http.ServiceHttpClient
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._

import scala.concurrent.{ExecutionContext, Future}

trait UserProfileClient {
  def externalResolverUri(ns: Namespace): Future[Option[Uri]]
}

object UserProfileClient {
  final case class NsSettings(namespace: Namespace, resolverUri: Option[Uri])

  object NsSettings {
    import com.advancedtelematic.libats.codecs.CirceCodecs.namespaceDecoder
    implicit val DecoderInstance = io.circe.generic.semiauto.deriveDecoder[NsSettings]
  }
}

class UserProfileHttpClient(uri: Uri, httpClient: HttpRequest => Future[HttpResponse])
                           (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer)
  extends ServiceHttpClient(httpClient) with UserProfileClient {

  override def externalResolverUri(ns: Namespace): Future[Option[Uri]] = {
    val path = uri.path / "api" / "v1" / "namespace_settings" / ns.get
    val request = HttpRequest(HttpMethods.GET, uri.withPath(path))

    val errorHandler: PartialFunction[Throwable, Option[Uri]] = {
      case e: RemoteServiceError if e.status == StatusCodes.NotFound => None
    }
    execHttp[UserProfileClient.NsSettings](request)().map(_.resolverUri).recover(errorHandler)
  }

}
