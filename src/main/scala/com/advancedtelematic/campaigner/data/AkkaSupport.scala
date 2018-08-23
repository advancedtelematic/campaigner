package com.advancedtelematic.campaigner.data

import com.advancedtelematic.campaigner.data.DataType.GroupId

import com.advancedtelematic.libats.http.UUIDKeyAkka._

object AkkaSupport {
  implicit val groupIdUnmarshaller = GroupId.unmarshaller
}
