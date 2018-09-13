package com.advancedtelematic.campaigner.data

import java.time.Instant

import com.advancedtelematic.campaigner.data.DataType.MetadataType.MetadataType
import com.advancedtelematic.campaigner.data.DataType.UpdateType.UpdateType
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.DataType.Namespace
import com.advancedtelematic.libats.messaging_datatype.DataType.{DeviceId, UpdateId}
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}

object Generators {

  val genCampaignId: Gen[CampaignId] = Gen.uuid.map(CampaignId(_))
  val genDeviceId: Gen[DeviceId] = Gen.uuid.map(DeviceId(_))
  val genGroupId: Gen[GroupId] = Gen.uuid.map(GroupId(_))
  val genUpdateId: Gen[UpdateId] = Gen.uuid.map(UpdateId(_))
  val genNamespace: Gen[Namespace] = arbitrary[String].map(Namespace)
  val genUpdateType: Gen[UpdateType] = Gen.oneOf(UpdateType.values.toSeq)
  val genMetadataType: Gen[MetadataType] = Gen.oneOf(MetadataType.values.toSeq)


  val genCampaignMetadata: Gen[CreateCampaignMetadata] = for {
    t <- arbitrary[MetadataType]
    v <- Gen.alphaStr
  } yield CreateCampaignMetadata(t, v)

  val genCampaign: Gen[Campaign] = for {
    ns     <- arbitrary[Namespace]
    id     <- arbitrary[CampaignId]
    n       = id.uuid.toString.take(5)
    update <- arbitrary[UpdateId]
    ca      = Instant.now()
    ua      = Instant.now()
  } yield Campaign(ns, id, n, update, CampaignStatus.prepared, ca, ua)

  val genCreateCampaign: Gen[CreateCampaign] = for {
    n   <- arbitrary[String]
    update <- arbitrary[UpdateId]
    gs  <- arbitrary[Set[GroupId]]
    meta   <- Gen.option(genCampaignMetadata.map(List(_)))
  } yield CreateCampaign(n, update, gs, meta)

  val genCreateCampaignWithAlphanumericName: Gen[CreateCampaign] = for {
    n   <- Gen.alphaNumStr.retryUntil(_.nonEmpty)
    update <- arbitrary[UpdateId]
    gs  <- arbitrary[Set[GroupId]]
    meta   <- Gen.option(genCampaignMetadata.map(List(_)))
  } yield CreateCampaign(n, update, gs, meta)

  val genUpdateCampaign: Gen[UpdateCampaign] = for {
    n   <- arbitrary[String].suchThat(!_.isEmpty)
    meta   <- genCampaignMetadata
  } yield UpdateCampaign(n, Option(List(meta)))

  val genUpdateSource: Gen[UpdateSource] = for {
    id <- arbitrary[String].suchThat(_.length < 200)
    t <- arbitrary[UpdateType]
  } yield UpdateSource(ExternalUpdateId(id), t)

  val genUpdate: Gen[Update] = for {
    id <- genUpdateId
    src <- genUpdateSource
    ns <- genNamespace
    nm <- Gen.alphaNumStr
    des <- Gen.option(Gen.alphaStr)
  } yield Update(id, src, ns, nm, des, Instant.now, Instant.now)

  val genCreateUpdate: Gen[CreateUpdate] = for {
    us <- genUpdateSource
    n <- arbitrary[String]
    d <- Gen.option(arbitrary[String])
  } yield CreateUpdate(us, n, d)

  val genCreateUpdateWithAlphanumericName: Gen[CreateUpdate] = for {
    us <- genUpdateSource
    n <- Gen.alphaNumStr.retryUntil(_.nonEmpty)
    d <- Gen.option(arbitrary[String])
  } yield CreateUpdate(us, n, d)

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
  implicit lazy val arbUpdateType: Arbitrary[UpdateType] = Arbitrary(genUpdateType)
  implicit lazy val arbUpdateSource: Arbitrary[UpdateSource] = Arbitrary(genUpdateSource)
  implicit lazy val arbUpdate: Arbitrary[Update] = Arbitrary(genUpdate)
  implicit lazy val arbMetadataType: Arbitrary[MetadataType] = Arbitrary(genMetadataType)
  implicit lazy val arbCreateUpdate: Arbitrary[CreateUpdate] = Arbitrary(genCreateUpdate)
  implicit lazy val arbStats: Arbitrary[Stats] = Arbitrary(genStats)

}
