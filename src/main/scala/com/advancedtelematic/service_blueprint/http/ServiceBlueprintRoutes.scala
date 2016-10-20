package com.advancedtelematic.service_blueprint.http

import akka.http.scaladsl.server.{Directives, _}
import akka.stream.Materializer
import com.advancedtelematic.libats.http.{ErrorHandler, HealthResource}
import com.advancedtelematic.service_blueprint.VersionInfo
import com.advancedtelematic.libats.http.DefaultRejectionHandler.rejectionHandler
import scala.concurrent.ExecutionContext
import slick.driver.MySQLDriver.api._


class ServiceBlueprintRoutes()
                   (implicit val db: Database, ec: ExecutionContext, mat: Materializer) extends VersionInfo {

  import Directives._

  val routes: Route =
    handleRejections(rejectionHandler) {
      ErrorHandler.handleErrors {
        pathPrefix("api" / "v1") {
            new BlueprintResource().route
        } ~ new HealthResource(db, versionMap).route
      }
    }
}
