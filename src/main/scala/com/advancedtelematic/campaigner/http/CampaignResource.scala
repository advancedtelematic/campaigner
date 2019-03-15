package com.advancedtelematic.campaigner.http

import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.server.Directives._
import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client.{DeviceRegistryClient, DirectorClient}
import com.advancedtelematic.campaigner.data.AkkaSupport._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.campaigner.http.Errors.MissingFailedDevices
import com.advancedtelematic.libats.auth.AuthedNamespaceScope
import com.advancedtelematic.libats.data.DataType.{CorrelationId, Namespace}
import com.advancedtelematic.libats.http.UUIDKeyAkka._
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.{ExecutionContext, Future}

class CampaignResource(extractAuth: Directive1[AuthedNamespaceScope],
                       director: DirectorClient,
                       deviceRegistry: DeviceRegistryClient)
                      (implicit db: Database, ec: ExecutionContext) extends Settings {

  val campaigns = Campaigns()

  def createCampaign(ns: Namespace, request: CreateCampaign): Future[CampaignId] = {
    val campaign = request.mkCampaign(ns)
    val metadata = request.mkCampaignMetadata(campaign.id)
    for {
      devices <- fetchDevicesInGroups(ns, request.groups)
      campaignId <- campaigns.create(campaign, request.groups.toList.toSet, devices, metadata)
    } yield campaignId
  }

  /**
    * Create and immediately launch a retry-campaign for all the devices that were processed by `mainCampaign` and
    * failed with a code `failureCode`. We assume there is at least one failed device. Hence, if there were not any
    * such devices in `mainCampaign`, throw an error.
    */
  def retryFailedDevices(ns: Namespace, mainCampaign: Campaign, request: RetryFailedDevices): Future[Unit] =
    campaigns.fetchFailedDevices(mainCampaign.id, request.failureCode).map(_.toSeq).flatMap {
      case Nil =>
        Future.failed(MissingFailedDevices(request.failureCode))

      case deviceIds =>
        // FIXME we just need to create this dummy group until we get totally rid of the groups.
        val retryGroup = GroupId(UUID.fromString("0000-00-00-00-000000"))
        val retryCampaign = CreateRetryCampaign(
          s"retryCampaignWith-mainCampaign-${mainCampaign.id.uuid}-failureCode-${request.failureCode}",
          mainCampaign.updateId,
          retryGroup,
          mainCampaign.id,
          request.failureCode
        ).mkCampaign(ns)

        campaigns
          .create(retryCampaign, Set(retryGroup), deviceIds.toSet, Nil)
          .flatMap(campaigns.launch)
    }

  private def UserCampaignPathPrefix(namespace: Namespace): Directive1[Campaign] =
    pathPrefix(CampaignId.Path).flatMap { campaign =>
      onSuccess(campaigns.findNamespaceCampaign(namespace, campaign)).flatMap(provide)
    }

  private def fetchDevicesInGroups(ns: Namespace, groups: NonEmptyList[GroupId]): Future[Set[DeviceId]] = {
    val groupSet = groups.toList.toSet
    // TODO (OTA-2385) review the retrieval logic and 'limit' parameter
    Future.traverse(groupSet)(gid => deviceRegistry.devicesInGroup(ns, gid, 0, 50)).map(_.flatten)
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
          // Deprecated
          complete(director.cancelUpdate(ns, cancelDevice.device))
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
