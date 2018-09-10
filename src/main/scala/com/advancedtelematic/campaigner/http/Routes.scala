package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.server.{Directive1, Directives, Route}
import com.advancedtelematic.campaigner.VersionInfo
import com.advancedtelematic.campaigner.client.{DeviceRegistryClient, DirectorClient, Resolver}
import com.advancedtelematic.libats.auth.NamespaceDirectives
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.http.DefaultRejectionHandler.rejectionHandler
import com.advancedtelematic.libats.http.ErrorHandler
import com.advancedtelematic.libats.slick.monitoring.DbHealthResource

import scala.concurrent.ExecutionContext
import slick.jdbc.MySQLProfile.api._

class Routes(director: DirectorClient, deviceRegistry: DeviceRegistryClient, resolver: Resolver)
            (implicit val db: Database, ec: ExecutionContext)
    extends VersionInfo {

  import Directives._

  val extractAuth = NamespaceDirectives.fromConfig()

  lazy val defaultNamespaceExtractor: Directive1[Namespace] =
    NamespaceDirectives.defaultNamespaceExtractor.map(_.namespace)

  val routes: Route =
    handleRejections(rejectionHandler) {
      ErrorHandler.handleErrors {
        pathPrefix("api" / "v2") {
          new CampaignResource(extractAuth, director).route ~
          new DeviceResource(defaultNamespaceExtractor).route ~
          new UpdateResource(defaultNamespaceExtractor, deviceRegistry, resolver).route
        } ~ DbHealthResource(versionMap).route
      }
    }

}
