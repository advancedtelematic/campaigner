package com.advancedtelematic.campaigner.db

import java.sql.SQLIntegrityConstraintViolationException

object DBErrors {

  object CampaignUpdateFKViolation {
    def unapply(e: SQLIntegrityConstraintViolationException): Boolean =
      e.getErrorCode == 1452 && e.getMessage.contains("update_fk")
  }


  object CampaignCampaignFKViolation {
    def unapply(e: SQLIntegrityConstraintViolationException): Boolean =
      e.getErrorCode == 1452 && e.getMessage.contains("fk_parent_campaign_uuid")
  }

}
