package com.advancedtelematic.campaigner

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.advancedtelematic.campaigner.client.DirectorHttpClient
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.http.BootApp
import com.advancedtelematic.libats.http.LogDirectives._
import com.advancedtelematic.libats.http.VersionDirectives._
import com.advancedtelematic.libats.monitoring.MetricsSupport
import com.advancedtelematic.libats.slick.db.{BootMigrations, DatabaseConfig}
import com.advancedtelematic.libats.slick.monitoring.DatabaseMetrics
import com.advancedtelematic.metrics.InfluxdbMetricsReporterSupport

trait Settings {
  import com.typesafe.config.ConfigFactory
  import java.util.concurrent.TimeUnit
  import scala.concurrent.duration._

  private lazy val _config = ConfigFactory.load()

  val host = _config.getString("server.host")
  val port = _config.getInt("server.port")

  val deviceRegistryUri = _config.getString("deviceRegistry.uri")
  val directorUri = _config.getString("director.uri")

  val schedulerPollingTimeout =
    FiniteDuration(_config.getDuration("scheduler.pollingTimeout").toNanos, TimeUnit.NANOSECONDS)
  val schedulerDelay =
    FiniteDuration(_config.getDuration("scheduler.delay").toNanos, TimeUnit.NANOSECONDS)
  val schedulerBatchSize =
    _config.getLong("scheduler.batchSize")
}

object Boot extends BootApp
  with Settings
  with VersionInfo
  with DatabaseConfig
  with BootMigrations
  with MetricsSupport
  with DatabaseMetrics
  with InfluxdbMetricsReporterSupport {

  implicit val _db = db

  log.info(s"Starting $version on http://$host:$port")

  val director = new DirectorHttpClient(directorUri)

  val routes: Route =
    (versionHeaders(version) & logResponseMetrics(projectName)) {
      new Routes(director).routes
    }

  Http().bindAndHandle(routes, host, port)
}
