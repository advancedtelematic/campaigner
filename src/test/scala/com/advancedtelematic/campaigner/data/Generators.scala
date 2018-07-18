package com.advancedtelematic.campaigner.data

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import java.time.Instant

import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}

object Generators {

  val genCampaignId: Gen[CampaignId] = Gen.uuid.map(CampaignId(_))
  val genDeviceId: Gen[DeviceId] = Gen.uuid.map(DeviceId(_))
  val genGroupId: Gen[GroupId] = Gen.uuid.map(GroupId(_))
  val genUpdateId: Gen[UpdateId] = Gen.uuid.map(UpdateId(_))
  val genNamespace: Gen[Namespace] = arbitrary[String].map(Namespace(_))

  val genCampaignMetadata: Gen[CreateCampaignMetadata] = for {
    t <- Gen.const(MetadataType.install)
    v <- Gen.alphaStr
  } yield CreateCampaignMetadata(t, v)

  val genCampaign: Gen[Campaign] = for {
    ns     <- arbitrary[Namespace]
    id     <- arbitrary[CampaignId]
    n       = id.uuid.toString.take(5)
    update <- arbitrary[UpdateId]
    ca      = Instant.now()
    ua      = Instant.now()
  } yield Campaign(ns, id, n, update, ca, ua)

  val genCreateCampaign: Gen[CreateCampaign] = for {
    n   <- arbitrary[String]
    update <- arbitrary[UpdateId]
    gs  <- arbitrary[Set[GroupId]]
    meta   <- Gen.oneOf(genCampaignMetadata.map(List(_)), Gen.const(Seq.empty))
  } yield CreateCampaign(n, update, gs, meta)

  val genUpdateCampaign: Gen[UpdateCampaign] = for {
    n   <- arbitrary[String].suchThat(!_.isEmpty)
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
