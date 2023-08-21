create index `namespace_parent_campaign_created_at_idx`
    on campaigns (namespace, parent_campaign_uuid, created_at desc);
