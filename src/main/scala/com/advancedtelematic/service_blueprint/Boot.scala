package com.advancedtelematic.service_blueprint

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.Materializer
import com.advancedtelematic.libats.db.{BootMigrations, DatabaseConfig}
import com.advancedtelematic.libats.http.BootApp
import com.advancedtelematic.libats.monitoring.{DatabaseMetrics, MetricsSupport}
import com.advancedtelematic.service_blueprint.http.ServiceBlueprintRoutes
import com.typesafe.config.ConfigFactory
import com.advancedtelematic.libats.http.VersionDirectives._
import com.advancedtelematic.libats.http.LogDirectives._

trait Settings {
  lazy val config = ConfigFactory.load()

  val host = config.getString("server.host")
  val port = config.getInt("server.port")
}

object Boot extends BootApp
  with Directives
  with Settings
  with VersionInfo
  with DatabaseConfig
  with BootMigrations
  with MetricsSupport
  with DatabaseMetrics {

  implicit val _db = db

  log.info(s"Starting $version on http://$host:$port")

  val routes: Route =
    (versionHeaders(version) & logResponseMetrics(projectName)) {
      new ServiceBlueprintRoutes().routes
    }

  Http().bindAndHandle(routes, host, port)
}
