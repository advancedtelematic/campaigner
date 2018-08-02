package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, Route}
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
              complete(updates.create(request.mkUpdate(ns)))
            }
        }

      }
    }
}
