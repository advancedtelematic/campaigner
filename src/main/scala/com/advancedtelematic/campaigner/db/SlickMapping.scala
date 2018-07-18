package com.advancedtelematic.campaigner.db

import com.advancedtelematic.campaigner.data.DataType.{CancelTaskStatus, DeviceStatus, GroupStatus, MetadataType}
import com.advancedtelematic.libats.slick.codecs.SlickEnumMapper

object SlickMapping {
  implicit val metadataTypeMapper = SlickEnumMapper.enumMapper(MetadataType)
  implicit val deviceStatusMapper = SlickEnumMapper.enumMapper(DeviceStatus)
  implicit val groupStatusMapper = SlickEnumMapper.enumMapper(GroupStatus)
  implicit val cancelTaskStatusMapper = SlickEnumMapper.enumMapper(CancelTaskStatus)
}
