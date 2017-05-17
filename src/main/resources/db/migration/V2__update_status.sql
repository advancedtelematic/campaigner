ALTER TABLE `campaign_stats`
  RENAME TO `group_stats`,
  ADD COLUMN `status` ENUM('scheduled', 'launched', 'cancelled') NOT NULL DEFAULT 'scheduled',
  DROP COLUMN `completed`,
  DROP FOREIGN KEY `campaign_stats`,
  ADD CONSTRAINT `group_stats_campaign_id_fk` FOREIGN KEY (campaign_id) REFERENCES `campaigns` (uuid);

