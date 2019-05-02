package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.marshalling.{Marshaller, ToResponseMarshaller}
import akka.http.scaladsl.model.headers.{ContentDispositionTypes, `Content-Disposition`}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, Route}
import akka.http.scaladsl.unmarshalling.{FromStringUnmarshaller, Unmarshaller}
import cats.data.NonEmptyList
import com.advancedtelematic.campaigner.Settings
import com.advancedtelematic.campaigner.client.{DeviceRegistryClient, DirectorClient}
import com.advancedtelematic.campaigner.data.AkkaSupport._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.CsvSerializer
import com.advancedtelematic.campaigner.data.DataType.CampaignStatus.CampaignStatus
import com.advancedtelematic.campaigner.data.DataType.SortBy.SortBy
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.db.Campaigns
import com.advancedtelematic.libats.auth.AuthedNamespaceScope
import com.advancedtelematic.libats.data.DataType.{CorrelationId, Namespace, ResultCode, ResultDescription}
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

  implicit val resultCodeUnmarshaller: FromStringUnmarshaller[ResultCode] = Unmarshaller.strict(ResultCode)

  def createCampaign(ns: Namespace, request: CreateCampaign): Future[CampaignId] = {
    val campaign = request.mkCampaign(ns)
    val metadata = request.mkCampaignMetadata(campaign.id)
    for {
      devices <- fetchDevicesInGroups(ns, request.groups)
      campaignId <- campaigns.create(campaign, request.groups.toList.toSet, devices, metadata)
    } yield campaignId
  }

  def searchCampaigns(ns: Namespace): Route =
    parameters((
      'status.as[CampaignStatus].?,
      'nameContains.as[String].?,
      'sortBy.as[SortBy].?,
      'offset.as[Long] ? 0L,
      'limit.as[Long] ? 50L)) {
      (status, nameContains, sortBy, offset, limit) =>
        complete(campaigns.allCampaigns(ns, sortBy.getOrElse(SortBy.CreatedAt), offset, limit, status, nameContains))
    }

  /**
    * Create and immediately launch a retry-campaign for all the devices that were processed by `mainCampaign` and
    * failed with the code given in `request`.
    */
  def retryFailedDevices(ns: Namespace, mainCampaign: Campaign, request: RetryFailedDevices): Future[CampaignId] =
    campaigns.retryCampaign(ns, mainCampaign, request.failureCode)


  def installationFailureCsvMarshaller(campaignId: CampaignId): ToResponseMarshaller[Seq[(String, ResultCode, ResultDescription)]] =
    Marshaller.withFixedContentType(ContentTypes.`text/csv(UTF-8)`) { t =>
      val csv = CsvSerializer.asCsv(Seq("Device ID", "Failure Code", "Failure Description"), t)
      val e = HttpEntity(ContentTypes.`text/csv(UTF-8)`, csv)
      val h = `Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> s"campaign-${campaignId.uuid.toString}-device-failures.csv"))
      HttpResponse(headers = h :: Nil, entity = e)
    }

  /**
    * For the devices that are in failed status `failureCode` after executing the campaign with ID `campaignId`
    * or any of its retry-campaigns, calculate the triplets (DeviceOemId, ResultCode, ResultDescription) and
    * return them as a CSV file.
    */
  def fetchFailureCodes(ns: Namespace, campaignId: CampaignId, failureCode: ResultCode): Route = {
    val f = campaigns
      .findLatestFailedUpdates(campaignId, failureCode)
      .map(_.map(du => (du.device, du.resultCode.getOrElse(ResultCode("")), du.resultDescription.getOrElse(ResultDescription("")))))
      .flatMap {
        Future.traverse(_) { case (did, fc, fd) =>
          deviceRegistry.fetchOemId(ns, did).map((_, fc, fd))
        }.map(_.toSeq)
    }
    implicit val marshaller = installationFailureCsvMarshaller(campaignId)
    complete(f)
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
          get {
            searchCampaigns(ns)
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
          get {
            path("stats") {
              complete(campaigns.campaignStats(campaign.id))
            } ~
            (path("failed-installations.csv") & parameter('failureCode.as[ResultCode])) {
              failureCode => fetchFailureCodes(ns, campaign.id, failureCode)
            }
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
