package com.advancedtelematic.campaigner.data

import com.advancedtelematic.libats.codecs.AkkaCirce._
import com.advancedtelematic.libats.codecs.Codecs._
import io.circe.{Decoder, Encoder}

object Codecs {

  import DataType._
  import io.circe.generic.semiauto._

  implicit val decoderCampaign: Decoder[Campaign] = deriveDecoder
  implicit val encoderCampaign: Encoder[Campaign] = deriveEncoder

  implicit val decoderCreateCampaign: Decoder[CreateCampaign] = deriveDecoder
  implicit val encoderCreateCampaign: Encoder[CreateCampaign] = deriveEncoder

  implicit val decoderGetCampaign: Decoder[GetCampaign] = deriveDecoder
  implicit val encoderGetCampaign: Encoder[GetCampaign] = deriveEncoder

  implicit val decoderUpdateCampaign: Decoder[UpdateCampaign] = deriveDecoder
  implicit val encoderUpdateCampaign: Encoder[UpdateCampaign] = deriveEncoder

}
