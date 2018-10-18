CREATE TABLE `campaign_errors` (
  `campaign_id` CHAR(36),
  `error_count` INT NOT NULL,
  `last_error` VARCHAR(1024) NOT NULL,
  `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

   PRIMARY KEY (`campaign_id`)
);
