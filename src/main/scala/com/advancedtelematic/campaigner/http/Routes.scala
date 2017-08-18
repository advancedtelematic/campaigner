package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.server.{Directives, Route}
import com.advancedtelematic.campaigner.VersionInfo
import com.advancedtelematic.libats.auth.NamespaceDirectives
import com.advancedtelematic.libats.http.DefaultRejectionHandler.rejectionHandler
import com.advancedtelematic.libats.http.ErrorHandler
import com.advancedtelematic.libats.slick.monitoring.DbHealthResource
import scala.concurrent.ExecutionContext
import slick.jdbc.MySQLProfile.api._

class Routes(implicit val db: Database, ec: ExecutionContext)
    extends VersionInfo {

  import Directives._

  val extractAuth = NamespaceDirectives.fromConfig()

  val routes: Route =
    handleRejections(rejectionHandler) {
      ErrorHandler.handleErrors {
        pathPrefix("api" / "v2") {
          new CampaignResource(extractAuth).route
        } ~ DbHealthResource(versionMap).route
      }
    }

}
