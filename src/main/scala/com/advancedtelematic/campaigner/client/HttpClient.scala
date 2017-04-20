package com.advancedtelematic.campaigner.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.http.scaladsl.util.FastFuture
import akka.stream.Materializer
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.http.ErrorCode
import com.advancedtelematic.libats.http.Errors.RawError
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class HttpClient(name: String, uri: Uri)
  (implicit ec: ExecutionContext, system: ActorSystem, mat: Materializer) {

  private def RemoteError(msg: String) =
    RawError(ErrorCode(s"${name}_remote_error"), StatusCodes.BadGateway, msg)

  private val _http = Http()

  protected def execHttp[T: ClassTag]
      (namespace: Namespace, request: HttpRequest)
      (implicit um: FromEntityUnmarshaller[T]): Future[T] = {
    _http.singleRequest(request.withHeaders(RawHeader("x-ats-namespace", namespace.get))).flatMap {
      case r@HttpResponse(status, _, _,_) if status.isSuccess() =>
        um(r.entity)
      case r =>
        FastFuture.failed(RemoteError(s"Unexpected response from $name: $r"))
    }
  }

}

