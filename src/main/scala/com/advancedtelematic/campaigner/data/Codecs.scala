package com.advancedtelematic.campaigner.data

import com.advancedtelematic.campaigner.actor.StatsCollector._
import com.advancedtelematic.libats.codecs.AkkaCirce._
import com.advancedtelematic.libats.codecs.Codecs._
import com.advancedtelematic.libats.data.UUIDKey.UUIDKey
import io.circe.{Decoder, Encoder, KeyDecoder, KeyEncoder}
import java.util.UUID

object Codecs {

  import DataType._
  import io.circe.generic.semiauto._
  import shapeless._

  case class Foo(bar: Map[String, String])

  implicit val decoderCampaign: Decoder[Campaign] = deriveDecoder
  implicit val encoderCampaign: Encoder[Campaign] = deriveEncoder

  implicit val decoderCreateCampaign: Decoder[CreateCampaign] = deriveDecoder
  implicit val encoderCreateCampaign: Encoder[CreateCampaign] = deriveEncoder

  implicit val decoderGetCampaign: Decoder[GetCampaign] = deriveDecoder
  implicit val encoderGetCampaign: Encoder[GetCampaign] = deriveEncoder

  implicit val decoderUpdateCampaign: Decoder[UpdateCampaign] = deriveDecoder
  implicit val encoderUpdateCampaign: Encoder[UpdateCampaign] = deriveEncoder

  implicit val decoderStats: Decoder[Stats] = deriveDecoder
  implicit val encoderStats: Encoder[Stats] = deriveEncoder

  implicit def uuidKeyDecoder[T](implicit gen: Generic.Aux[T, UUID :: HNil]): KeyDecoder[T] =
    KeyDecoder[String].map(s => gen.from(UUID.fromString(s) :: HNil))

  implicit def uuidKeyEncoder[T <: UUIDKey]: KeyEncoder[T] =
    KeyEncoder[String].contramap(_.uuid.toString)

  implicit val decoderCampaignStatsResult: Decoder[CampaignStatsResult] = deriveDecoder
  implicit val encoderCampaignStatsResult: Encoder[CampaignStatsResult] = deriveEncoder

}
