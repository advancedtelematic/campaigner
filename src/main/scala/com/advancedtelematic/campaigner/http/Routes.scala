package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.server.{Directive1, Directives, Route}
import com.advancedtelematic.campaigner.VersionInfo
import com.advancedtelematic.campaigner.client.{DeviceRegistryClient, DirectorClient, ResolverClient, UserProfileClient}
import com.advancedtelematic.libats.auth.NamespaceDirectives
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.http.DefaultRejectionHandler.rejectionHandler
import com.advancedtelematic.libats.http.ErrorHandler
import com.advancedtelematic.libats.messaging.MessageBusPublisher
import com.advancedtelematic.libats.slick.monitoring.DbHealthResource
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.ExecutionContext

class Routes(director: DirectorClient, deviceRegistry: DeviceRegistryClient, resolver: ResolverClient, userProfile: UserProfileClient, messageBus: MessageBusPublisher)
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
          new CampaignResource(extractAuth, director, messageBus).route ~
          new DeviceResource(userProfile, resolver, defaultNamespaceExtractor).route ~
            new UpdateResource(defaultNamespaceExtractor, deviceRegistry, resolver, userProfile).route
        } ~ DbHealthResource(versionMap).route
      }
    }

}
