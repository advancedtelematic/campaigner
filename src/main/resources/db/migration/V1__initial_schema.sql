ALTER DATABASE CHARACTER SET utf8 COLLATE utf8_unicode_ci;

CREATE TABLE `campaigns` (
  `uuid` CHAR(36) NOT NULL,
  `namespace` CHAR(200) NOT NULL,
  `name` VARCHAR(200) NOT NULL,
  `update_id` CHAR(36) NOT NULL,
  `created_at` DATETIME(3) NOT NULL DEFAULT NOW(),
  `updated_at` DATETIME(3) NOT NULL DEFAULT NOW(),

   PRIMARY KEY (uuid)
);

CREATE UNIQUE INDEX `campaigns_unique_name` ON
  `campaigns` (`namespace`, `name`)
;

CREATE TABLE `campaign_groups` (
  `campaign_id` CHAR(36) NOT NULL,
  `group_id` CHAR(36) NOT NULL,

  PRIMARY KEY (campaign_id, group_id),
  CONSTRAINT `campaign_groups_campaign_id_fk` FOREIGN KEY (campaign_id) REFERENCES `campaigns` (uuid)
);
