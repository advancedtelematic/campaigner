package com.advancedtelematic.campaigner.util

import akka.actor.Scheduler
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.data.DataType.Campaign
import com.advancedtelematic.campaigner.db.UpdateRepository
import com.advancedtelematic.libats.test.LongTest
import slick.jdbc.MySQLProfile.api._
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{FlatSpecLike, Matchers}

import scala.concurrent.ExecutionContext

trait CampaignerSpecUtil {
  implicit class GenerateOps[T](value: Gen[T]) {
    def generate: T = value.pureApply(Gen.Parameters.default, Seed.random(), retries = 100)
  }
}

object CampaignerSpecUtil extends CampaignerSpecUtil

trait CampaignerSpec extends FlatSpecLike
  with Matchers
  with ScalaFutures
  with LongTest
  with CampaignerSpecUtil {

  def buildCampaignWithUpdate(implicit db: Database, ec: ExecutionContext, scheduler: Scheduler): Campaign = {
    val updateRepo = new UpdateRepository()
    val update = genMultiTargetUpdate.generate
    val updateId = updateRepo.persist(update)
    updateId.map(uid => genCampaign.generate.copy(updateId = uid)).futureValue
  }
}
