package com.advancedtelematic.service_blueprint.http

import akka.http.scaladsl.model.StatusCodes
import com.advancedtelematic.libats.http.ErrorCode
import com.advancedtelematic.libats.http.Errors.RawError

object ErrorCodes {
  val BlueprintMissing = ErrorCode("blueprint_missing")
}

object Errors {
  val BlueprintMissing = RawError(ErrorCodes.BlueprintMissing, StatusCodes.PreconditionFailed, "blueprint reference does not exist")
}
