package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.server.Directive1
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import com.advancedtelematic.libats.http.UUIDKeyPath._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.ExecutionContext
import akka.http.scaladsl.server.Directives._
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import com.advancedtelematic.campaigner.data.Codecs._

class DeviceResource(extractNamespace: Directive1[Namespace])
                    (implicit val db: Database, val ec: ExecutionContext) {

  val deviceCampaigns = new DeviceCampaigns()

  val route =
    extractNamespace { _ =>
      pathPrefix("device" / DeviceId.Path) { deviceId =>
        path("campaigns") {
          get {
            complete(deviceCampaigns.findScheduledCampaigns(deviceId))
          }
        }
      }
    }
}
