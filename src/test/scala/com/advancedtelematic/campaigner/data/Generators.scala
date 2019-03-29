package com.advancedtelematic.campaigner.data

import java.time.Instant

import cats.data.NonEmptyList
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
  val genNonEmptyGroupIdList: Gen[NonEmptyList[GroupId]] = Gen.nonEmptyListOf(genGroupId).map(NonEmptyList.fromListUnsafe)

  val genCampaignMetadata: Gen[CreateCampaignMetadata] = for {
    t <- arbitrary[MetadataType]
    v <- Gen.alphaStr
  } yield CreateCampaignMetadata(t, v)

  val genCampaign: Gen[Campaign] = for {
    ns     <- arbitrary[Namespace]
    id     <- arbitrary[CampaignId]
    n       = id.uuid.toString.take(5)
    update <- arbitrary[UpdateId]
    now      = Instant.now()
  } yield Campaign(ns, id, n, update, CampaignStatus.prepared, now, now, None, None)

  def genCreateCampaign(genName: Gen[String] = arbitrary[String]): Gen[CreateCampaign] = for {
    n   <- genName
    update <- arbitrary[UpdateId]
    gs <- arbitrary[NonEmptyList[GroupId]]
    meta   <- Gen.option(genCampaignMetadata.map(List(_)))
  } yield CreateCampaign(n, update, gs, meta)

  val genUpdateCampaign: Gen[UpdateCampaign] = for {
    n   <- arbitrary[String].suchThat(!_.isEmpty)
    meta   <- genCampaignMetadata
  } yield UpdateCampaign(n, Option(List(meta)))

  def genUpdateSource(genType: => Gen[UpdateType]): Gen[UpdateSource] = for {
    id <- arbitrary[String].retryUntil(_.length > 0)
    t <- genType
  } yield UpdateSource(ExternalUpdateId(id), t)

  def genUpdate(genType: Gen[UpdateType] = arbitrary[UpdateType]): Gen[Update] = for {
    id <- genUpdateId
    src <- genUpdateSource(genType)
    ns <- genNamespace
    nm <- Gen.alphaNumStr
    des <- Gen.option(Gen.alphaStr)
  } yield Update(id, src, ns, nm, des, Instant.now, Instant.now)

  val genMultiTargetUpdate: Gen[Update] = genUpdate(Gen.const(UpdateType.multi_target))

  def genCreateUpdate(genName: Gen[String] = arbitrary[String], genType: Gen[UpdateType] = arbitrary[UpdateType]): Gen[CreateUpdate] = for {
    us <- genUpdateSource(genType)
    n <- genName
    d <- Gen.option(arbitrary[String])
  } yield CreateUpdate(us, n, d)

  def genDeviceUpdate(
      genCampaignId: Gen[CampaignId] = arbitrary[CampaignId],
      genResultCode: Gen[String] = Gen.alphaNumStr,
      genResultDescription: Gen[String] = Gen.alphaNumStr): Gen[DeviceUpdate] =
    for {
      cid <- genCampaignId
      uid <- genUpdateId
      did <- genDeviceId
      st <- Gen.oneOf(DeviceStatus.values.toSeq)
      rc <- Gen.option(genResultCode)
      rd <- Gen.option(genResultDescription)
      upAt <- Gen.posNum[Long].map(Instant.ofEpochSecond)
    } yield DeviceUpdate(cid, uid, did, st, rc, rd, upAt)

  val genExportCase: Gen[(DeviceId, String, String, String)] = for {
    deviceId <- genDeviceId
    deviceOemId <- Gen.alphaNumStr.map("OEM-ID-" + _)
    failureCode <- Gen.alphaStr.map("FAILURE-CODE-" + _)
    failureDescription <- Gen.alphaStr.map("FAILURE-DESCRIPTION-" + _)
  } yield (deviceId, deviceOemId, failureCode, failureDescription)

  implicit lazy val arbCampaignId: Arbitrary[CampaignId] = Arbitrary(genCampaignId)
  implicit lazy val arbGroupId: Arbitrary[GroupId] = Arbitrary(genGroupId)
  implicit lazy val arbDeviceId: Arbitrary[DeviceId] = Arbitrary(genDeviceId)
  implicit lazy val arbUpdateId: Arbitrary[UpdateId] = Arbitrary(genUpdateId)
  implicit lazy val arbNamespace: Arbitrary[Namespace] = Arbitrary(genNamespace)
  implicit lazy val arbCampaign: Arbitrary[Campaign] = Arbitrary(genCampaign)
  implicit lazy val arbCreateCampaign: Arbitrary[CreateCampaign] = Arbitrary(genCreateCampaign())
  implicit lazy val arbUpdateCampaign: Arbitrary[UpdateCampaign] = Arbitrary(genUpdateCampaign)
  implicit lazy val arbUpdateType: Arbitrary[UpdateType] = Arbitrary(genUpdateType)
  implicit lazy val arbUpdate: Arbitrary[Update] = Arbitrary(genUpdate())
  implicit lazy val arbMetadataType: Arbitrary[MetadataType] = Arbitrary(genMetadataType)
  implicit lazy val arbCreateUpdate: Arbitrary[CreateUpdate] = Arbitrary(genCreateUpdate())
  implicit lazy val arbNonEmptyGroupIdList: Arbitrary[NonEmptyList[GroupId]] = Arbitrary(genNonEmptyGroupIdList)

}
