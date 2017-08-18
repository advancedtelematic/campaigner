package com.advancedtelematic.campaigner

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.advancedtelematic.campaigner.actor._
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.daemon._
import com.advancedtelematic.libats.http.BootApp
import com.advancedtelematic.libats.messaging.{BusListenerMetrics, MessageListenerSupport}
import com.advancedtelematic.libats.messaging_datatype.Messages.DeviceUpdateReport
import com.advancedtelematic.libats.monitoring.MetricsSupport
import com.advancedtelematic.libats.slick.db.{BootMigrations, DatabaseConfig}
import com.advancedtelematic.libats.slick.monitoring.{DatabaseMetrics, DbHealthResource}
import com.advancedtelematic.metrics.InfluxdbMetricsReporterSupport

object DaemonBoot extends BootApp
    with Settings
    with VersionInfo
    with BootMigrations
    with DatabaseConfig
    with MetricsSupport
    with DatabaseMetrics
    with MessageListenerSupport
    with InfluxdbMetricsReporterSupport {

  import com.advancedtelematic.libats.http.LogDirectives._
  import com.advancedtelematic.libats.http.VersionDirectives._

  implicit val _db = db

  log.info("Starting campaigner daemon")

  val deviceRegistry = new DeviceRegistryHttpClient(deviceRegistryUri)
  val director = new DirectorHttpClient(directorUri)
  val supervisor = system.actorOf(CampaignSupervisor.props(
    deviceRegistry,
    director,
    schedulerPollingTimeout,
    schedulerDelay,
    schedulerBatchSize
  ))

  startListener[DeviceUpdateReport](DeviceUpdateReportListener.apply)

  val routes: Route = (versionHeaders(version) & logResponseMetrics(projectName)) {
    DbHealthResource(versionMap, healthMetrics = Seq(new BusListenerMetrics(metricRegistry))).route
  }

  Http().bindAndHandle(routes, host, port)
}
