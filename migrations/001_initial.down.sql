-- Destructive: explicit operator rollback only; back up retained data first.
DROP TABLE IF EXISTS price_availability_observation;
DROP TABLE IF EXISTS offer;
DROP TABLE IF EXISTS crawl_request;
DROP TABLE IF EXISTS source_domain_config;
DROP TABLE IF EXISTS message_outbox;
