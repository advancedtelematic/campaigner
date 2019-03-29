package com.advancedtelematic.campaigner.data

import akka.http.scaladsl.model.Uri
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.campaigner.data.DataType.MetadataType.MetadataType
import com.advancedtelematic.campaigner.data.DataType.UpdateType.UpdateType
import com.advancedtelematic.libats.codecs.CirceCodecs._
import io.circe.{Decoder, Encoder, KeyEncoder}

object Codecs {
  import DataType.CampaignStatus.CampaignStatus
  import DataType.RetryStatus.RetryStatus
  import io.circe.generic.semiauto._


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

  implicit val decoderRetryFailedDevices: Decoder[RetryFailedDevices] = deriveDecoder
  implicit val encoderRetryFailedDevices: Encoder[RetryFailedDevices] = deriveEncoder

  implicit val decoderCreateUpdate: Decoder[CreateUpdate] = deriveDecoder
  implicit val encoderCreateUpdate: Encoder[CreateUpdate] = deriveEncoder

  implicit val decoderGetCampaign: Decoder[GetCampaign] = deriveDecoder
  implicit val encoderGetCampaign: Encoder[GetCampaign] = deriveEncoder

  implicit val updateTypeEncoder : Encoder[UpdateType] = Encoder.enumEncoder(UpdateType)
  implicit val updateTypeDecoder : Decoder[UpdateType] = Decoder.enumDecoder(UpdateType)

  implicit val decoderUpdateCampaign: Decoder[UpdateCampaign] = deriveDecoder
  implicit val encoderUpdateCampaign: Encoder[UpdateCampaign] = deriveEncoder

  implicit val decoderCampaignStatus: Decoder[CampaignStatus] = Decoder.enumDecoder(CampaignStatus)
  implicit val encoderCampaignStatus: Encoder[CampaignStatus] = Encoder.enumEncoder(CampaignStatus)

  implicit val keyEncoderCampaignStatus: KeyEncoder[CampaignStatus] = KeyEncoder[String].contramap(_.toString)

  implicit val decoderRetryStatus: Decoder[RetryStatus] = Decoder.enumDecoder(RetryStatus)
  implicit val encoderRetryStatus: Encoder[RetryStatus] = Encoder.enumEncoder(RetryStatus)

  implicit val decoderCampaignFailureStats: Decoder[CampaignFailureStats] = deriveDecoder
  implicit val encoderCampaignFailureStats: Encoder[CampaignFailureStats] = deriveEncoder

  implicit val decoderCampaignStats: Decoder[CampaignStats] = deriveDecoder
  implicit val encoderCampaignStats: Encoder[CampaignStats] = deriveEncoder

  implicit val uriEncoder: Encoder[Uri] = Encoder.encodeString.contramap(_.toString)
  implicit val uriDecoder: Decoder[Uri] = Decoder.decodeString.map(Uri.apply)
}
