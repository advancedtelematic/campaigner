CREATE TABLE failed_groups (
  campaign_id  CHAR(36) NOT NULL,
  group_id     CHAR(36) NOT NULL,
  failure_code VARCHAR(200) NOT NULL,

  CONSTRAINT pk_failed_groups PRIMARY KEY (campaign_id, failure_code),
  CONSTRAINT fk_failed_groups_campaign_id FOREIGN KEY (campaign_id) REFERENCES campaigns (uuid)
);