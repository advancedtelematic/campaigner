package com.advancedtelematic.campaigner

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.{Directives, Route}
import com.advancedtelematic.campaigner.actor._
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.http.BootApp
import com.advancedtelematic.libats.http.LogDirectives._
import com.advancedtelematic.libats.http.VersionDirectives._
import com.advancedtelematic.libats.monitoring.MetricsSupport
import com.advancedtelematic.libats.slick.db.{BootMigrations, DatabaseConfig}
import com.advancedtelematic.libats.slick.monitoring.DatabaseMetrics

trait Settings {
  import com.typesafe.config.ConfigFactory
  import java.util.concurrent.TimeUnit
  import scala.concurrent.duration._

  lazy val config = ConfigFactory.load()

  val host = config.getString("server.host")
  val port = config.getInt("server.port")

  val deviceRegistryUri = config.getString("deviceRegistry.uri")
  val directorUri = config.getString("director.uri")

  val schedulerDelay =
    FiniteDuration(config.getDuration("scheduler.delay").toNanos, TimeUnit.NANOSECONDS)
  val schedulerBatchSize =
    config.getInt("scheduler.batchSize")
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

  val deviceRegistry = new DeviceRegistryClient(deviceRegistryUri)
  val director = new DirectorClient(directorUri)
  val supervisor = system.actorOf(Supervisor.props(deviceRegistry, director))

  val routes: Route =
    (versionHeaders(version) & logResponseMetrics(projectName)) {
      new Routes(supervisor).routes
    }

  Http().bindAndHandle(routes, host, port)
}
