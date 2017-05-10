package com.advancedtelematic.campaigner.http

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.Materializer
import com.advancedtelematic.campaigner.VersionInfo
import com.advancedtelematic.libats.http.DefaultRejectionHandler.rejectionHandler
import com.advancedtelematic.libats.http.{ErrorHandler, HealthResource}
import com.advancedtelematic.libats.slick.monitoring.DbHealthResource
import scala.concurrent.ExecutionContext
import slick.driver.MySQLDriver.api._

class Routes(supervisor: ActorRef)
            (implicit val db: Database, ec: ExecutionContext, mat: Materializer, system: ActorSystem)
  extends VersionInfo {

  import Directives._

  val routes: Route =
    handleRejections(rejectionHandler) {
      ErrorHandler.handleErrors {
        pathPrefix("api" / "v1") {
            new CampaignResource(supervisor).route
        } ~ new HealthResource(Seq(DbHealthResource.HealthCheck(db)), versionMap).route
      }
    }

}
