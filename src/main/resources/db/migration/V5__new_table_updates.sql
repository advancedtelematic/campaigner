CREATE TABLE `updates` (
  `uuid` CHAR(36) NOT NULL,
  `external_id` VARCHAR(200),
  `namespace` CHAR(200) NOT NULL,
  `name` VARCHAR(200) NOT NULL,
  `description` VARCHAR(600),
  `created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

   PRIMARY KEY (`uuid`),
   UNIQUE KEY `unique_external_id` (`namespace`, `external_id`)
);