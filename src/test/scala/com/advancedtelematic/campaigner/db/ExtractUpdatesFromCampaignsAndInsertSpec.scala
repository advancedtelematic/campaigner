package com.advancedtelematic.campaigner.db

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.advancedtelematic.campaigner.data.DataType.{CampaignMetadata, GroupId}
import com.advancedtelematic.campaigner.data.Generators.genCampaign
import com.advancedtelematic.libats.slick.db.SlickUUIDKey._
import com.advancedtelematic.libats.test.DatabaseSpec
import org.scalacheck.Gen
import org.scalatest.Inspectors.forAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest._
import slick.jdbc.MySQLProfile.api._


class ExtractUpdatesFromCampaignsAndInsertSpec extends FlatSpec
  with BeforeAndAfterEach
  with OptionValues
  with DatabaseSpec
  with Matchers
  with ScalaFutures {

  private val TEST_CAMPAIGNS = 6
  private implicit lazy val system: ActorSystem = ActorSystem(this.getClass.getSimpleName)
  private implicit val materializer: ActorMaterializer = ActorMaterializer()

  import system.dispatcher

  private val cs = Campaigns()
  private val extractor = new ExtractUpdatesFromCampaignsAndInsert()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    db.run { sqlu"""DELETE FROM campaigns;"""}.futureValue
    db.run { sqlu"""DELETE FROM updates;"""}.futureValue
  }

  "Running the migration" should "create one update record in the DB for each campaign." in {
    val campaigns = Gen.listOfN(TEST_CAMPAIGNS, genCampaign).sample.get
    forAll(campaigns) { campaign =>
      cs.create(campaign, Set.empty[GroupId], Seq.empty[CampaignMetadata]).futureValue
    }

    val updatedCount = extractor.createOneUpdateRecordForEachCampaign().futureValue
    updatedCount.value shouldBe TEST_CAMPAIGNS
  }

  "Migrating duplicated UpdateSource's" should "create only one update record in the DB." in {
    val campaign1 = genCampaign.sample.get
    val campaign2 = genCampaign.sample.get.copy(updateId = campaign1.updateId)
    cs.create(campaign1, Set.empty[GroupId], Seq.empty[CampaignMetadata]).futureValue
    cs.create(campaign2, Set.empty[GroupId], Seq.empty[CampaignMetadata]).futureValue

    val updatedCount = extractor.createOneUpdateRecordForEachCampaign().futureValue
    updatedCount.value shouldBe 1
  }

  "Migrating duplicated (namespace, updateId)" should "create only one update record in the DB." in {
    val campaign1 = genCampaign.sample.get
    val campaign2 = genCampaign.sample.get.copy(namespace = campaign1.namespace, updateId = campaign1.updateId)
    cs.create(campaign1, Set.empty[GroupId], Seq.empty[CampaignMetadata]).futureValue
    cs.create(campaign2, Set.empty[GroupId], Seq.empty[CampaignMetadata]).futureValue

    val updatedCount = extractor.createOneUpdateRecordForEachCampaign().futureValue
    updatedCount.value shouldBe 1
  }

  "UpdateId of campaign" should "be the updateId of the newly created update." in {
    val campaign = genCampaign.sample.get
    val oldUpdateId = campaign.updateId
    cs.create(campaign, Set.empty[GroupId], Seq.empty[CampaignMetadata]).futureValue

    new ExtractUpdatesFromCampaignsAndInsert().run().futureValue

    val campaignFromDB = db.run(Schema.campaigns.filter(_.id === campaign.id).result).futureValue.head
    val updateFromDB = db.run(Schema.updates.take(1).result).futureValue.head
    oldUpdateId.uuid.toString shouldBe updateFromDB.source.id
    campaignFromDB.updateId shouldBe updateFromDB.uuid
  }

}
