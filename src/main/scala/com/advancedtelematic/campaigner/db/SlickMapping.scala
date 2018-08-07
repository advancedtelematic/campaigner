package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType._
import com.advancedtelematic.libats.slick.codecs.SlickEnumMapper

object SlickMapping {
  implicit val metadataTypeMapper = SlickEnumMapper.enumMapper(MetadataType)
  implicit val deviceStatusMapper = SlickEnumMapper.enumMapper(DeviceStatus)
  implicit val groupStatusMapper = SlickEnumMapper.enumMapper(GroupStatus)
  implicit val cancelTaskStatusMapper = SlickEnumMapper.enumMapper(CancelTaskStatus)
  implicit val updateKindMapper = SlickEnumMapper.enumMapper(UpdateKind)
}
