package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.data.ErrorCode
import com.advancedtelematic.libats.http.Errors.{Error, MissingEntity, RawError}
import com.advancedtelematic.libats.messaging_datatype.DataType.UpdateId

object ErrorCodes {
  val ConflictingCampaign = ErrorCode("campaign_already_exists")
  val MissingUpdateSource = ErrorCode("missing_update_source")
  val MissingParentCampaign = ErrorCode("missing_parent_campaign")
  val MissingUpdate = ErrorCode("missing_update")
  val MissingFailedGroup = ErrorCode("missing_failed_group")
  val ConflictingMetadata = ErrorCode("campaign_metadata_already_exists")
  val CampaignAlreadyLaunched = ErrorCode("campaign_already_launched")
  val InvalidCounts = ErrorCode("invalid_stats_count")
  val DeviceNotScheduled = ErrorCode("device_not_scheduled")
  val ConflictingUpdate = ErrorCode("update_already_exists")
  val TimeoutFetchingUpdates = ErrorCode("timeout_fetching_updates")
}

object Errors {

  val CampaignMissing = MissingEntity[Campaign]

  case class MissingUpdate(id: UpdateId) extends Error(ErrorCodes.MissingUpdate, StatusCodes.NotFound, s"Missing $id")

  case class MissingExternalUpdate(externalUpdateId: ExternalUpdateId) extends Error(ErrorCodes.MissingUpdate, StatusCodes.NotFound, s"Missing $externalUpdateId")

  val MissingUpdateSource = RawError(
    ErrorCodes.MissingUpdateSource,
    StatusCodes.PreconditionFailed,
    "The update associated with the given campaign does not exist."
  )

  val MissingParentCampaign = RawError(
    ErrorCodes.MissingParentCampaign,
    StatusCodes.PreconditionFailed,
    "The parent campaign for this retry campaign is invalid or does not exist."
  )

  val MissingFailedGroup = RawError(
    ErrorCodes.MissingFailedGroup,
    StatusCodes.PreconditionFailed,
    "There is no failed device group for the given campaign and failure code."
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
  val TimeoutFetchingUpdates = RawError(
    ErrorCodes.TimeoutFetchingUpdates,
    StatusCodes.GatewayTimeout,
    "It took too long to retrieve all the devices in the given groups.")
}
