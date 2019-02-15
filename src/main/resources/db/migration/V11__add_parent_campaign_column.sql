ALTER TABLE `campaigns` ADD COLUMN `parent_campaign_uuid` CHAR(36) NULL DEFAULT NULL;

ALTER TABLE `campaigns` ADD CONSTRAINT fk_parent_campaign_uuid FOREIGN KEY (parent_campaign_uuid) REFERENCES campaigns(uuid);
