package com.advancedtelematic.campaigner

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.advancedtelematic.campaigner.actor._
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.daemon._
import com.advancedtelematic.libats.http.monitoring.MetricsSupport
import com.advancedtelematic.libats.http.tracing.NullServerRequestTracing
import com.advancedtelematic.libats.http.{BootApp, ServiceHttpClientSupport}
import com.advancedtelematic.libats.messaging.{BusListenerMetrics, MessageListenerSupport}
import com.advancedtelematic.libats.messaging_datatype.Messages.{DeviceEventMessage, DeviceUpdateEvent}
import com.advancedtelematic.libats.slick.db.{BootMigrations, CheckMigrations, DatabaseConfig}
import com.advancedtelematic.libats.slick.monitoring.{DatabaseMetrics, DbHealthResource}
import com.advancedtelematic.metrics.prometheus.PrometheusMetricsSupport

object DaemonBoot extends BootApp
  with Settings
  with VersionInfo
  with BootMigrations
  with DatabaseConfig
  with MetricsSupport
  with DatabaseMetrics
  with CheckMigrations
  with MessageListenerSupport
  with PrometheusMetricsSupport
  with ServiceHttpClientSupport {

  import com.advancedtelematic.libats.http.LogDirectives._
  import com.advancedtelematic.libats.http.VersionDirectives._
  import akka.http.scaladsl.server.Directives._

  implicit val _db = db

  log.info("Starting campaigner daemon")

  implicit val tracing = new NullServerRequestTracing

  val deviceRegistry = new DeviceRegistryHttpClient(deviceRegistryUri, defaultHttpClient)
  val director = new DirectorHttpClient(directorUri, defaultHttpClient)
  val supervisor = system.actorOf(CampaignSupervisor.props(
    director,
    schedulerPollingTimeout,
    schedulerDelay,
    schedulerBatchSize
  ),
    "campaign-supervisor"
  )

  startListener[DeviceUpdateEvent](new DeviceUpdateEventListener())
  startListener[DeviceEventMessage](new DeviceEventListener(director))

  val routes: Route = (versionHeaders(version) & logResponseMetrics(projectName)) {
    prometheusMetricsRoutes ~
    DbHealthResource(versionMap, healthMetrics = Seq(new BusListenerMetrics(metricRegistry))).route
  }

  Http().bindAndHandle(routes, host, port)
}
