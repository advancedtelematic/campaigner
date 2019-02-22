package com.advancedtelematic.campaigner.http

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.util.FastFuture
import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client.DirectorClient
import com.advancedtelematic.campaigner.data.AkkaSupport._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.libats.auth.AuthedNamespaceScope
import com.advancedtelematic.libats.data.DataType.{CorrelationId, MultiTargetUpdateId, Namespace, CampaignId => CampaignCorrelationId}
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

  def retryFailedDevices(ns: Namespace, mainCampaign: Campaign, request: RetryFailedDevices): Future[CampaignId] = {
    campaigns.fetchFailedGroupId(mainCampaign.id, request.failureCode).flatMap {
      case None =>
        Future.failed(Errors.MissingFailedGroup)
      case Some(gid) =>
        val retryGroup = NonEmptyList.one(gid)
        val retryCampaign = CreateCampaign(
          s"retryCampaignWith-mainCampaign-${mainCampaign.id.uuid}-failureCode-${request.failureCode}",
          mainCampaign.updateId,
          retryGroup,
          Some(mainCampaign.id)
        ).mkCampaign(ns)
        campaigns.create(retryCampaign, retryGroup, Nil)
    }
  }

  def cancelDeviceUpdate(ns: Namespace, correlationId: CorrelationId, device: DeviceId)
                        (implicit log: LoggingAdapter): Future[Unit] =
    director.cancelUpdate(ns, device).flatMap { _ =>
      correlationId match {
        case CampaignCorrelationId(uuid) =>
          campaigns.finishDevices(CampaignId(uuid), Seq(device), DeviceStatus.cancelled)
        case MultiTargetUpdateId(uuid) =>
          campaigns.findCampaignsByUpdate(UpdateId(uuid)).flatMap {
            case cs if cs.isEmpty =>
              log.info(s"No campaign exists for $device.")
              FastFuture.successful(())
            case _ =>
              campaigns.finishDevice(UpdateId(uuid), device, DeviceStatus.cancelled)
          }
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
        path("count") {
          complete(campaigns.countByStatus)
        } ~
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
          post {
            path("launch") {
              complete(campaigns.launch(campaign.id))
            } ~
            path("cancel") {
              complete(campaigns.cancel(campaign.id))
            } ~
            (path("retry-failed") & entity(as[RetryFailedDevices])) { request =>
              complete(StatusCodes.Created -> retryFailedDevices(ns, campaign, request))
            }
          } ~
          (get & path("stats")) {
            complete(campaigns.campaignStats(campaign.id))
          }
        }
      } ~
      extractLog { implicit log =>
        (post & path("cancel_device_update_campaign") & entity(as[CancelDeviceUpdateCampaign])) { cancelDevice =>
              complete(cancelDeviceUpdate(ns, cancelDevice.correlationId, cancelDevice.device))
        }
      }
    }
}

final case class CancelDeviceUpdateCampaign(correlationId: CorrelationId, device: DeviceId)

object CancelDeviceUpdateCampaign {
  import io.circe.{Decoder, Encoder}
  implicit val encoder: Encoder[CancelDeviceUpdateCampaign] = io.circe.generic.semiauto.deriveEncoder
  implicit val decoder: Decoder[CancelDeviceUpdateCampaign] = io.circe.generic.semiauto.deriveDecoder
}
