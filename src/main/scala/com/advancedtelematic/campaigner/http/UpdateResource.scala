package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client.{DeviceRegistryClient, Resolver}
import com.advancedtelematic.campaigner.data.AkkaSupport._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.{CreateUpdate, GroupId, Update}
import com.advancedtelematic.campaigner.db.UpdateSupport
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.data.PaginationResult
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import io.circe.{Encoder, Json}
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext}

object HypermediaResource {
  final case class Link(rel: String, uri: Uri)

  object Link {
    def self(uri: Uri): Link = Link("self", uri)
  }

  import com.advancedtelematic.libats.http.HttpCodecs.uriEncoder
  private[this] implicit val LinkEncoder = io.circe.generic.semiauto.deriveEncoder[Link]

  implicit def createEncoder[T](implicit tEnc: Encoder[T]): Encoder[HypermediaResource[T]] = {
    import io.circe.syntax._
    Encoder.instance { resource =>
      tEnc.apply(resource.value).deepMerge(Json.obj("_links" -> resource.links.asJson))
    }
  }
}

final case class HypermediaResource[T](links: Seq[HypermediaResource.Link], value: T)

class UpdateResource(extractNamespace: Directive1[Namespace], deviceRegistry: DeviceRegistryClient, resolver: Resolver)
                    (implicit db: Database, ec: ExecutionContext) extends Settings with UpdateSupport {

  private[this] val pathToUpdates = Path.Empty / "api" / "v2" / "updates"

  private[this] def linkToSelf(update: Update): HypermediaResource[Update] = {
    val links = HypermediaResource.Link.self(
      Uri.Empty.withPath(pathToUpdates / update.uuid.uuid.toString)
    ) :: Nil
    HypermediaResource(links, update)
  }

  val groupUpdateResolver = new GroupUpdateResolver(deviceRegistry, resolver)

  val route: Route =
    extractNamespace { ns =>
      pathPrefix("updates") {
        pathEnd {
          (get & parameter('groupId.as[GroupId].?) & parameters('limit.as[Long].?) & parameters('offset.as[Long].?)) { (groupId, limit, offset) =>
            groupId match {
              case Some(gid) =>
                val f = groupUpdateResolver.groupUpdates(ns, gid).map { res =>
                  PaginationResult(res.size, res.size, 0, res).map(linkToSelf)
                }
                complete(f)
              case None =>
                complete(updateRepo.all(ns, offset, limit).map(_.map(linkToSelf)))
            }
          } ~
          (post & entity(as[CreateUpdate])) { request =>
            onSuccess(updateRepo.persist(request.mkUpdate(ns))) { uuid =>
              extractRequest { req =>
                val resourceUri = req.uri.withPath(req.uri.path / uuid.uuid.toString)
                complete((StatusCodes.Created, List(Location(resourceUri)), uuid))
              }
            }
          }
        }

      }
    }
}
