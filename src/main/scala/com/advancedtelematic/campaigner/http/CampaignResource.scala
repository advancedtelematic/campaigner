package com.advancedtelematic.campaigner.http

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.util.FastFuture
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.data.AkkaSupport._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.libats.auth.AuthedNamespaceScope
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.http.UUIDKeyAkka._
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class CampaignResource(extractAuth: Directive1[AuthedNamespaceScope], director: DirectorClient)
                      (implicit db: Database, ec: ExecutionContext) extends Settings {

  val campaigns = Campaigns()

  def createCampaign(ns: Namespace, request: CreateCampaign): Future[CampaignId] = {
    val campaign = request.mkCampaign(ns)
    val metadata = request.mkCampaignMetadata(campaign.id)
    campaigns.create(campaign, request.groups, metadata)
  }

  def cancelDeviceUpdate(ns: Namespace, update: UpdateId, device: DeviceId)
                        (implicit log: LoggingAdapter): Future[Unit] =
    director.cancelUpdate(ns, device).flatMap { _ =>
      campaigns.findCampaignsByUpdate(update).flatMap {
        case cs if cs.isEmpty =>
          log.info(s"No campaign exists for $device.")
          FastFuture.successful(())
        case _ =>
          campaigns.finishDevice(update, device, DeviceStatus.cancelled)
      }
    }

  private def UserCampaignPathPrefix(namespace: Namespace): Directive1[Campaign] =
    pathPrefix(CampaignId.Path).flatMap { campaign =>
      onSuccess(campaigns.findNamespaceCampaign(namespace, campaign)).flatMap(provide)
    }

  val route: Route =
    extractAuth { auth =>
      val ns = auth.namespace
      pathPrefix("campaigns") {
        pathEnd {
          (get & parameters(('status.as[CampaignStatus].?, 'nameContains.as[String].?, 'sortBy.as[SortBy].?, 'offset.as[Long] ? 0L, 'limit.as[Long] ? 50L))) {
            (status, nameContains, sortBy, offset, limit) => complete(campaigns.allCampaigns(ns, sortBy.getOrElse(SortBy.Name), offset, limit, status, nameContains))
          } ~
          (post & entity(as[CreateCampaign])) { request =>
            complete(StatusCodes.Created -> createCampaign(ns, request))
          }
        } ~
        UserCampaignPathPrefix(ns) { campaign =>
          pathEnd {
            get {
              complete(campaigns.findClientCampaign(campaign.id))
            } ~
            (put & entity(as[UpdateCampaign])) { updated =>
              complete(campaigns.update(campaign.id, updated.name, updated.metadata.toList.flatten.map(_.toCampaignMetadata(campaign.id))))
            }
          } ~
          (post & path("launch")) {
            complete(campaigns.launch(campaign.id))
          } ~
          (get & path("stats")) {
            complete(campaigns.campaignStats(campaign.id))
          } ~
          (post & path("cancel")) {
            complete(campaigns.cancel(campaign.id))
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
