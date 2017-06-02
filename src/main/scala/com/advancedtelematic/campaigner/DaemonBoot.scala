package com.advancedtelematic.campaigner

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.advancedtelematic.campaigner.actor._
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.daemon._
import com.advancedtelematic.libats.http.{BootApp, HealthResource}
import com.advancedtelematic.libats.messaging.daemon.MessageBusListenerActor.Subscribe
import com.advancedtelematic.libats.monitoring.MetricsSupport
import com.advancedtelematic.libats.slick.db.{BootMigrations, DatabaseConfig}
import com.advancedtelematic.libats.slick.monitoring.{DatabaseMetrics, DbHealthResource}

object DaemonBoot extends BootApp
    with Settings
    with VersionInfo
    with BootMigrations
    with DatabaseConfig
    with MetricsSupport
    with DatabaseMetrics {

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

  system.actorOf(
    DeviceUpdateReportListener.props(config),
    "device-update-report-msg-listener"
  ) ! Subscribe

  val routes: Route = (versionHeaders(version) & logResponseMetrics(projectName)) {
    new HealthResource(Seq(DbHealthResource.HealthCheck(db)), versionMap).route
  }

  Http().bindAndHandle(routes, host, port)
}
