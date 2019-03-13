package com.advancedtelematic.campaigner.util

import akka.http.scaladsl.model.StatusCodes._
import com.advancedtelematic.campaigner.data.Codecs._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, UpdateSupport}
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId
import com.advancedtelematic.libats.test.DatabaseSpec
import de.heikoseeberger.akkahttpcirce.FailFastCirceSupport._
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalatest.Matchers
import org.scalatest.concurrent.ScalaFutures
import CampaignerSpecUtil._
import cats.data.NonEmptyList
import com.advancedtelematic.libats.data.DataType.Namespace

import scala.concurrent.Future

trait UpdateResourceSpecUtil {
  self: ResourceSpec with Matchers =>

  def createUpdateOk(request: CreateUpdate): UpdateId = {
    Post(apiUri("updates"), request).withHeaders(header) ~> routes ~> check {
      status shouldBe Created
      responseAs[UpdateId]
    }
  }

  def createCampaignWithUpdateOk(gen: Gen[CreateCampaign] = genCreateCampaign()) = {
    val createUpdate = genCreateUpdate().map(cu => cu.copy(updateSource = UpdateSource(cu.updateSource.id, UpdateType.multi_target))).generate
    val updateId = createUpdateOk(createUpdate)
    val createCampaign = gen.map(_.copy(update = updateId)).generate
    createCampaignOk(createCampaign) -> createCampaign
  }
}

trait DatabaseUpdateSpecUtil {
  self: DatabaseSpec with ScalaFutures with UpdateSupport =>

  import scala.concurrent.ExecutionContext.Implicits.global

  private val campaigns = Campaigns()

  def createDbUpdate(updateId: UpdateId): Future[UpdateId] = {
    val update = genMultiTargetUpdate.generate.copy(uuid = updateId)
    updateRepo.persist(update)
  }

  def createDbCampaign(namespace: Namespace, updateId: UpdateId, groups: NonEmptyList[GroupId]): Future[Campaign] = {
    val campaign = arbitrary[Campaign].generate.copy(updateId = updateId, namespace = namespace)
    campaigns.create(campaign, groups, Set.empty, Seq.empty).map(_ => campaign)
  }

  def createDbCampaignWithUpdate(maybeCampaign: Option[Campaign] = None, maybeGroups: Option[NonEmptyList[GroupId]] = None): Future[Campaign] = {
    val campaign = maybeCampaign.getOrElse(arbitrary[Campaign].generate)
    val groups = maybeGroups.getOrElse(NonEmptyList.one(GroupId.generate()))
    for {
      _ <- createDbUpdate(campaign.updateId)
      _ <- campaigns.create(campaign, groups, Set.empty, Seq.empty)
    }  yield campaign
  }
}
