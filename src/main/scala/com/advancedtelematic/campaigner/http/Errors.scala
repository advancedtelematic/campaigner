package com.advancedtelematic.campaigner.http

import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.libats.http.ErrorCode
import com.advancedtelematic.libats.http.Errors.RawError

object ErrorCodes {
  val CampaignMissing = ErrorCode("Campaign_missing")
}

object Errors {
  val CampaignMissing = RawError(ErrorCodes.CampaignMissing, StatusCodes.PreconditionFailed, "Campaign reference does not exist")
}
