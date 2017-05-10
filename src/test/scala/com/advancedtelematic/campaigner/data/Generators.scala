package com.advancedtelematic.campaigner.data


import com.advancedtelematic.campaigner.actor.StatsCollector._
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.Namespace
import java.time.Instant
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}

object Generators {

  val genCampaignId: Gen[CampaignId] = Gen.uuid.map(CampaignId(_))
  val genDeviceId: Gen[DeviceId] = Gen.uuid.map(DeviceId(_))
  val genGroupId: Gen[GroupId] = Gen.uuid.map(GroupId(_))
  val genUpdateId: Gen[UpdateId] = Gen.uuid.map(UpdateId(_))
  val genNamespace: Gen[Namespace] = arbitrary[String].map(Namespace(_))

  val genCampaign: Gen[Campaign] = for {
    id     <- arbitrary[CampaignId]
    ns     <- arbitrary[Namespace]
    n      <- arbitrary[String]
    update <- arbitrary[UpdateId]
    t1      = Gen.choose(0,  Instant.MAX.getEpochSecond)
    t2      = t1.flatMap(Gen.choose(_, Instant.MAX.getEpochSecond))
    ca     <- t1.map(Instant.ofEpochSecond(_))
    ua     <- t2.map(Instant.ofEpochSecond(_))
  } yield Campaign(id, ns, n, update, ca, ua)

  val genCreateCampaign: Gen[CreateCampaign] = for {
    ns  <- arbitrary[Namespace]
    n   <- arbitrary[String].suchThat(!_.isEmpty)
    update <- arbitrary[UpdateId]
    gs  <- arbitrary[Set[GroupId]]
  } yield CreateCampaign(ns, n, update, gs)

  val genUpdateCampaign: Gen[UpdateCampaign] = for {
    n   <- arbitrary[String]
  } yield UpdateCampaign(n)

  val genStats: Gen[Stats] = for {
    p <- Gen.posNum[Long]
    a <- Gen.posNum[Long]
  } yield Stats(p, a)

  implicit lazy val arbCampaignId: Arbitrary[CampaignId] = Arbitrary(genCampaignId)
  implicit lazy val arbGroupId: Arbitrary[GroupId] = Arbitrary(genGroupId)
  implicit lazy val arbDeviceId: Arbitrary[DeviceId] = Arbitrary(genDeviceId)
  implicit lazy val arbUpdateId: Arbitrary[UpdateId] = Arbitrary(genUpdateId)
  implicit lazy val arbNamespace: Arbitrary[Namespace] = Arbitrary(genNamespace)
  implicit lazy val arbCampaign: Arbitrary[Campaign] = Arbitrary(genCampaign)
  implicit lazy val arbCreateCampaign: Arbitrary[CreateCampaign] = Arbitrary(genCreateCampaign)
  implicit lazy val arbUpdateCampaign: Arbitrary[UpdateCampaign] = Arbitrary(genUpdateCampaign)
  implicit lazy val arbStats: Arbitrary[Stats] = Arbitrary(genStats)

}
