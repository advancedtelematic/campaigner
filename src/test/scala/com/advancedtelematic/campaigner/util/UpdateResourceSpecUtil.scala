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
    val createUpdate = genCreateUpdate().map(cu => cu.copy(updateSource = UpdateSource(cu.updateSource.id, UpdateType.multi_target))).sample.get
    val updateId = createUpdateOk(createUpdate)
    val createCampaign = gen.map(_.copy(update = updateId)).gen
    createCampaignOk(createCampaign) -> createCampaign
  }
}

trait DatabaseUpdateSpecUtil {
  self: DatabaseSpec with ScalaFutures with UpdateSupport =>

  import scala.concurrent.ExecutionContext.Implicits.global

  private val campaigns = Campaigns()

  def createDbUpdate(updateId: UpdateId): Future[UpdateId] = {
    val update = genMultiTargetUpdate.sample.get.copy(uuid = updateId)
    updateRepo.persist(update)
  }

  def createDbCampaign(namespace: Namespace, updateId: UpdateId, groups: Set[GroupId] = Set.empty): Future[Campaign] = {
    val campaign = arbitrary[Campaign].gen.copy(updateId = updateId, namespace = namespace)
    campaigns.create(campaign, groups, Seq.empty).map(_ => campaign)
  }

  def createDbCampaignWithUpdate(maybeCampaign: Option[Campaign] = None, groups: Set[GroupId] = Set.empty): Future[Campaign] = {
    val campaign = maybeCampaign.getOrElse(arbitrary[Campaign].gen)

    for {
      _ <- createDbUpdate(campaign.updateId)
      _ <- campaigns.create(campaign, groups, Seq.empty)
    }  yield campaign
  }
}