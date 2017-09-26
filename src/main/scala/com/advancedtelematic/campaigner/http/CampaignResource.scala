package com.advancedtelematic.campaigner.http

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directive1
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.libats.auth.AuthedNamespaceScope
import com.advancedtelematic.libats.data.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import concurrent.{ExecutionContext, Future}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import slick.jdbc.MySQLProfile.api._

class CampaignResource(extractAuth: Directive1[AuthedNamespaceScope],
                       director: DirectorClient)
                      (implicit db: Database, ec: ExecutionContext)
  extends Settings {

  val campaigns = Campaigns()

  def createCampaign(ns: Namespace, request: CreateCampaign): Future[CampaignId] = {
    val campaign = request.mkCampaign(ns)
    campaigns.create(campaign, request.groups)
  }

  def cancelCampaign(ns: Namespace, id: CampaignId): Future[Unit] = for {
    devs     <- campaigns.cancelCampaign(ns, id)
    affected <- director.cancelUpdate(ns, devs.toSeq)
    _        <- campaigns.finishDevices(id, affected, DeviceStatus.cancelled)
  } yield ()

  def cancelDeviceUpdate(
    ns: Namespace,
    update: UpdateId,
    device: DeviceId)
    (implicit log: LoggingAdapter): Future[Unit] =
    director.cancelUpdate(ns, device).flatMap { _ =>
      campaigns.findCampaignsByUpdate(ns, update).flatMap {
        case cs if cs.isEmpty =>
          log.info(s"No campaign exists for $device.");
          FastFuture.successful(())
        case _ =>
          campaigns.finishDevice(update, device, DeviceStatus.cancelled)
      }
    }

  val route =
    extractAuth { auth =>
      val ns = auth.namespace
      pathPrefix("campaigns") {
        pathEnd {
          (get & parameters('limit.as[Long].?) & parameters('offset.as[Long].?)) { (mLimit, mOffset) =>
            val offset = mOffset.getOrElse(0L)
            val limit  = mLimit.getOrElse(50L)
            complete(campaigns.allCampaigns(ns, offset, limit))
          } ~
          (post & entity(as[CreateCampaign])) { request =>
            complete(StatusCodes.Created -> createCampaign(ns, request))
          }
        } ~
        pathPrefix(CampaignId.Path) { id =>
          pathEnd {
            get {
              complete(campaigns.findCampaign(ns, id))
            } ~
            (put & entity(as[UpdateCampaign])) { updated =>
              complete(campaigns.update(ns, id, updated.name))
            }
          } ~
          (post & path("launch")) {
            complete(campaigns.launch(ns, id))
          } ~
          (get & path("stats")) {
            complete(campaigns.campaignStats(ns, id))
          } ~
          (post & path("cancel")) {
            complete(cancelCampaign(ns, id))
          }
        }
      } ~
      extractLog { implicit log =>
        (post & path("cancel_device_update_campaign") & entity(as[CancelDeviceUpdateCampaign])) { cancelDevice =>
              complete(cancelDeviceUpdate(ns, cancelDevice.update, cancelDevice.device))
        }
      }
    }
}

final case class CancelDeviceUpdateCampaign(update: UpdateId, device: DeviceId)

object CancelDeviceUpdateCampaign {
  import io.circe.{Decoder, Encoder}
  implicit val encoder: Encoder[CancelDeviceUpdateCampaign] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[CancelDeviceUpdateCampaign] = io.circe.generic.semiauto.deriveDecoder
}
