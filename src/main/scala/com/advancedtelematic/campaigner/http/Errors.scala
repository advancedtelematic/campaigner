package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.ErrorCode
import com.advancedtelematic.libats.http.Errors.{MissingEntity, RawError}

object ErrorCodes {
  val ConflictingCampaign = ErrorCode("campaign_already_exists")
  val MissingUpdateSource = ErrorCode("missing_update_source")
  val ConflictingMetadata = ErrorCode("campaign_metadata_already_exists")
  val CampaignAlreadyLaunched = ErrorCode("campaign_already_launched")
  val InvalidCounts = ErrorCode("invalid_stats_count")
  val DeviceNotScheduled = ErrorCode("device_not_scheduled")
  val ConflictingUpdate = ErrorCode("update_already_exists")
  val TooManyRequestsToRemote = ErrorCode("too_many_requests_to_remote")
}

object Errors {

  val CampaignMissing = MissingEntity[Campaign]

  val MissingUpdateSource = RawError(
    ErrorCodes.MissingUpdateSource,
    StatusCodes.PreconditionFailed,
    "The update associated with the given campaign does not exist."
  )

  val ConflictingCampaign = RawError(ErrorCodes.ConflictingCampaign, StatusCodes.Conflict, "campaign already exists")

  val ConflictingMetadata = RawError(
    ErrorCodes.ConflictingMetadata,
    StatusCodes.Conflict,
    "Metadata for campaign with given type already exists"
  )

  val CampaignAlreadyLaunched = RawError(
    ErrorCodes.CampaignAlreadyLaunched, StatusCodes.Conflict, "This campaign has already been launched."
  )
  val InvalidCounts = RawError(
    ErrorCodes.InvalidCounts,
    StatusCodes.InternalServerError,
    "The numbers of processed, affected, and/or finished devices do not match up."
  )
  val DeviceNotScheduled = RawError(
    ErrorCodes.DeviceNotScheduled,
    StatusCodes.PreconditionFailed,
    "The device has not been scheduled."
  )
  val ConflictingUpdate = RawError(
    ErrorCodes.ConflictingUpdate,
    StatusCodes.Conflict,
    "An update with that externalId already exists."
  )
  val TooManyRequestsToRemote = RawError(ErrorCodes.TooManyRequestsToRemote, StatusCodes.InternalServerError,
    "Too many requests to remote service")
}
