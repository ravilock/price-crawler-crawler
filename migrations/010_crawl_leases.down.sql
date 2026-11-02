DROP INDEX crawl_request_work;
ALTER TABLE crawl_request DROP COLUMN lease_token, DROP COLUMN lease_until;
