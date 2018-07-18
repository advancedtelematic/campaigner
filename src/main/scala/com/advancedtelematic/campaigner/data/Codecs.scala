package com.advancedtelematic.campaigner.data

import com.advancedtelematic.campaigner.data.DataType.MetadataType.MetadataType
import com.advancedtelematic.libats.codecs.CirceCodecs._
import io.circe.{Decoder, Encoder}

object Codecs {
  import DataType._
  import DataType.CampaignStatus.CampaignStatus
  import io.circe.generic.semiauto._

  implicit val metadataTypeEncoder: Encoder[MetadataType] = Encoder.enumEncoder(MetadataType)
  implicit val metadataTypeDecoder: Decoder[MetadataType] = Decoder.enumDecoder(MetadataType)

  implicit val userCampaignMetadataEncoder: Encoder[UserCampaignMetadata] = deriveEncoder
  implicit val userCampaignMetadataDecoder: Decoder[UserCampaignMetadata] = deriveDecoder

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

  implicit val decoderCampaignStatus: Decoder[CampaignStatus] = Decoder.enumDecoder(CampaignStatus)
  implicit val encoderCampaignStatus: Encoder[CampaignStatus] = Encoder.enumEncoder(CampaignStatus)

  implicit val decoderCampaignStats: Decoder[CampaignStats] = deriveDecoder
  implicit val encoderCampaignStats: Encoder[CampaignStats] = deriveEncoder
}
