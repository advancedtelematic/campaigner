CREATE TABLE `campaign_stats` (
  `campaign_id` CHAR(36) NOT NULL,
  `group_id` CHAR(36) NOT NULL,
  `completed` BOOLEAN NOT NULL,
  `processed` BIGINT UNSIGNED NOT NULL,
  `affected` BIGINT UNSIGNED NOT NULL,

  PRIMARY KEY (campaign_id, group_id),
  CONSTRAINT `campaign_stats` FOREIGN KEY (campaign_id) REFERENCES `campaigns` (uuid)
);

