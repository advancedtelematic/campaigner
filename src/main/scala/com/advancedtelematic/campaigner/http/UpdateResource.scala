package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.server.Directives._
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CreateUpdate
import com.advancedtelematic.campaigner.db.Updates
import com.advancedtelematic.libats.data.DataType.Namespace
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.ExecutionContext

class UpdateResource(extractNamespace: Directive1[Namespace])
                    (implicit db: Database, ec: ExecutionContext) extends Settings {

  val updates = Updates()

  val route: Route =
    extractNamespace { ns =>
      pathPrefix("updates") {
        pathEnd {
          (get & parameters('limit.as[Long].?) & parameters('offset.as[Long].?)) { (limit, offset) =>
            complete(updates.allUpdates(ns, offset, limit))
          } ~
          (post & entity(as[CreateUpdate])) { request =>
            onSuccess(updates.create(request.mkUpdate(ns))) { uuid =>
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
