package com.advancedtelematic.campaigner.data

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.DataType.MetadataType.MetadataType
import com.advancedtelematic.campaigner.data.DataType.UpdateType.UpdateType
import com.advancedtelematic.libats.codecs.CirceCodecs._
import io.circe.{Decoder, Encoder}

object Codecs {
  import DataType.CampaignStatus.CampaignStatus
  import io.circe.generic.semiauto._

  implicit val deviceCampaignsEncoder: Encoder[GetDeviceCampaigns] = deriveEncoder
  implicit val deviceCampaignsDecoder: Decoder[GetDeviceCampaigns] = deriveDecoder

  implicit val clientCampaignEncoder: Encoder[DeviceCampaign] = deriveEncoder
  implicit val clientCampaignDecoder: Decoder[DeviceCampaign] = deriveDecoder

  implicit val createCampaignMetadataEncoder: Encoder[CreateCampaignMetadata] = deriveEncoder
  implicit val createCampaignMetadataDecoder: Decoder[CreateCampaignMetadata] = deriveDecoder

  implicit val campaignMetadataEncoder: Encoder[CampaignMetadata] = deriveEncoder
  implicit val campaignMetadataDecoder: Decoder[CampaignMetadata] = deriveDecoder

  implicit val metadataTypeEncoder: Encoder[MetadataType] = Encoder.enumEncoder(MetadataType)
  implicit val metadataTypeDecoder: Decoder[MetadataType] = Decoder.enumDecoder(MetadataType)

  implicit val decoderCampaign: Decoder[Campaign] = deriveDecoder
  implicit val encoderCampaign: Encoder[Campaign] = deriveEncoder

  implicit val decoderUpdateSource: Decoder[UpdateSource] = deriveDecoder
  implicit val encoderUpdateSource: Encoder[UpdateSource] = deriveEncoder

  implicit val decoderUpdate: Decoder[Update] = deriveDecoder
  implicit val encoderUpdate: Encoder[Update] = deriveEncoder

  implicit val decoderCreateCampaign: Decoder[CreateCampaign] = deriveDecoder
  implicit val encoderCreateCampaign: Encoder[CreateCampaign] = deriveEncoder

  implicit val decoderCreateUpdate: Decoder[CreateUpdate] = deriveDecoder
  implicit val encoderCreateUpdate: Encoder[CreateUpdate] = deriveEncoder

  implicit val decoderGetCampaign: Decoder[GetCampaign] = deriveDecoder
  implicit val encoderGetCampaign: Encoder[GetCampaign] = deriveEncoder

  implicit val updateTypeEncoder : Encoder[UpdateType] = Encoder.enumEncoder(UpdateType)
  implicit val updateTypeDecoder : Decoder[UpdateType] = Decoder.enumDecoder(UpdateType)

  implicit val decoderUpdateCampaign: Decoder[UpdateCampaign] = deriveDecoder
  implicit val encoderUpdateCampaign: Encoder[UpdateCampaign] = deriveEncoder

  implicit val decoderStats: Decoder[Stats] = deriveDecoder
  implicit val encoderStats: Encoder[Stats] = deriveEncoder

  implicit val decoderCampaignStatus: Decoder[CampaignStatus] = Decoder.enumDecoder(CampaignStatus)
  implicit val encoderCampaignStatus: Encoder[CampaignStatus] = Encoder.enumEncoder(CampaignStatus)

  implicit val decoderCampaignStats: Decoder[CampaignStats] = deriveDecoder
  implicit val encoderCampaignStats: Encoder[CampaignStats] = deriveEncoder
}
