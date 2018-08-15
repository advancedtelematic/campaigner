package com.advancedtelematic.campaigner

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.{Directives, Route}
import com.advancedtelematic.campaigner.client.{DeviceRegistryHttpClient, DirectorHttpClient, NoOpResolver}
import com.advancedtelematic.campaigner.http.Routes
import com.advancedtelematic.libats.http.BootApp
import com.advancedtelematic.libats.http.LogDirectives._
import com.advancedtelematic.libats.http.VersionDirectives._
import com.advancedtelematic.libats.http.monitoring.MetricsSupport
import com.advancedtelematic.libats.slick.db.DatabaseConfig
import com.advancedtelematic.libats.slick.monitoring.DatabaseMetrics
import com.advancedtelematic.metrics.prometheus.PrometheusMetricsSupport
import com.advancedtelematic.metrics.{AkkaHttpRequestMetrics, InfluxdbMetricsReporterSupport}

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
  with Directives
  with Settings
  with VersionInfo
  with DatabaseConfig
  with MetricsSupport
  with DatabaseMetrics
  with InfluxdbMetricsReporterSupport
  with AkkaHttpRequestMetrics
  with PrometheusMetricsSupport {

  implicit val _db = db

  log.info(s"Starting $version on http://$host:$port")

  val director = new DirectorHttpClient(directorUri)

  val deviceRegistry = new DeviceRegistryHttpClient(deviceRegistryUri)

  val routes: Route =
    (versionHeaders(version) & requestMetrics(metricRegistry) & logResponseMetrics(projectName)) {
      prometheusMetricsRoutes ~
      new Routes(director, deviceRegistry, new NoOpResolver).routes
    }

  Http().bindAndHandle(routes, host, port)
}
