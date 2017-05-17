package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.http.ErrorCode
import com.advancedtelematic.libats.http.Errors.RawError
import com.advancedtelematic.libats.http.Errors._

object ErrorCodes {
  val ConflictingCampaign = ErrorCode("campaign_already_exists")
  val CampaignAlreadyLaunched = ErrorCode("campaign_already_launched")
}

object Errors {
  val CampaignMissing = MissingEntity[Campaign]
  val ConflictingCampaign = RawError(
    ErrorCodes.ConflictingCampaign, StatusCodes.Conflict, "A campaign with that name already exists."
  )
  val CampaignAlreadyLaunched = RawError(
    ErrorCodes.CampaignAlreadyLaunched, StatusCodes.Conflict, "This campaign has already been launched."
  )
}
