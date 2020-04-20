package com.advancedtelematic.campaigner

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.advancedtelematic.campaigner.actor._
import com.advancedtelematic.campaigner.client._
import com.advancedtelematic.campaigner.daemon._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.http.tracing.NullServerRequestTracing
import com.advancedtelematic.libats.http.{BootApp, ServiceHttpClientSupport}
import com.advancedtelematic.libats.messaging.MessageListenerSupport
import com.advancedtelematic.libats.messaging_datatype.Messages.{DeviceEventMessage, DeviceUpdateEvent}
import com.advancedtelematic.libats.slick.db.{BootMigrations, CheckMigrations, DatabaseConfig}
import com.advancedtelematic.libats.slick.monitoring.{DatabaseMetrics, DbHealthResource}
import com.advancedtelematic.metrics.prometheus.PrometheusMetricsSupport
import com.advancedtelematic.metrics.{MetricsSupport, MonitoredBusListenerSupport}

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object DaemonBoot extends BootApp
  with Settings
  with VersionInfo
  with BootMigrations
  with DatabaseConfig
  with MetricsSupport
  with DatabaseMetrics
  with CheckMigrations
  with MessageListenerSupport
  with MonitoredBusListenerSupport
  with PrometheusMetricsSupport
  with ServiceHttpClientSupport {

  import akka.http.scaladsl.server.Directives._
  import com.advancedtelematic.libats.http.LogDirectives._
  import com.advancedtelematic.libats.http.VersionDirectives._

  implicit val _db = db

  log.info("Starting campaigner daemon")

  implicit val tracing = new NullServerRequestTracing

  val deviceRegistry = new DeviceRegistryHttpClient(deviceRegistryUri, defaultHttpClient)

  val namespaceDirectorConfig = new NamespaceDirectorConfig(config)

  def director(ns: Namespace) = new DirectorHttpClient(namespaceDirectorConfig.getUri(ns), defaultHttpClient)

  val routes: Route = (versionHeaders(version) & logResponseMetrics(projectName)) {
    prometheusMetricsRoutes ~
      DbHealthResource(versionMap).route
  }

  Http().bindAndHandle(routes, host, port)

  // Consume all events before starting all daemons, populating the namespace config before the daemons have a chance to use the wrong director
  NamespaceDirectorChangedListener.consumeAll(config, namespaceDirectorConfig, 10.seconds).andThen {
    case Success(_) =>
      NamespaceDirectorChangedListener.start(config, namespaceDirectorConfig)

      system.actorOf(CampaignSupervisor.props(
        director,
        schedulerPollingTimeout,
        schedulerDelay,
        schedulerBatchSize
      ), "campaign-supervisor")

      startMonitoredListener[DeviceUpdateEvent](new DeviceUpdateEventListener)
      startMonitoredListener[DeviceEventMessage](new DeviceEventListener(director), skipProcessingErrors = true)

    case Failure(ex) =>
      log.warn("Error consuming namespace config", ex)
  }
}
