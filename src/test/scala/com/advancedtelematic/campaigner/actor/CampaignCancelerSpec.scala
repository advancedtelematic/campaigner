package com.advancedtelematic.campaigner.actor

import akka.actor.Terminated
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import com.advancedtelematic.campaigner.data.DataType.{Campaign, DeviceStatus}
import com.advancedtelematic.campaigner.data.Generators._
import com.advancedtelematic.campaigner.db.{Campaigns, DeviceUpdateSupport, UpdateSupport}
import com.advancedtelematic.campaigner.util.{ActorSpec, CampaignerSpec, DatabaseUpdateSpecUtil}
import com.advancedtelematic.libats.messaging_datatype.DataType.DeviceId
import org.scalacheck.Gen
import org.scalatest.Inspectors

import scala.concurrent.duration._

class CampaignCancelerSpec extends ActorSpec[CampaignCancelerSpec] with CampaignerSpec with UpdateSupport
    with DeviceUpdateSupport
    with DatabaseUpdateSpecUtil with Inspectors {

  val campaigns = Campaigns()

  "campaign canceler" should "cancel devices which were not scheduled yet" in {
    val campaign = buildCampaignWithUpdate
    val parent   = TestProbe()
    val devices = Gen.listOfN(1, genDeviceId).generate.toSet

    campaigns.create(campaign, Set.empty, devices, Seq.empty).futureValue

    campaigns.cancel(campaign.id).futureValue

    val canceler = parent.childActorOf(CampaignCanceler.props(director, campaign.id, campaign.namespace, 10))

    parent.watch(canceler)

    parent.expectMsgPF(5.seconds) ({
      case Terminated(_) => true
    })

    val processed = deviceUpdateRepo.findByCampaignStream(campaign.id, DeviceStatus.cancelled)
      .map(d => List(d._1)).runWith(Sink.fold(List.empty[DeviceId])(_ ++ _)).futureValue

    processed should contain allElementsOf(devices)
  }
}
