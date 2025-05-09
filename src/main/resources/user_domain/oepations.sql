-- Profile Service
UPDATE user_domain
SET profile = '{"name": "Alice", "email": "alice@example.com", "created_at": "2025-05-08T15:00:00Z"}'
WHERE user_id = uuid();

-- Consent Service
UPDATE user_domain
SET consent = '{"gdpr": true, "marketing_opt_in": false}'
WHERE user_id = uuid();

-- Address Service
UPDATE user_domain
SET addresses = '[{"number": "1739", "street": "Shasta Ave", "city": "San Jose", "region": "CA", "postcode": ""}]'
WHERE user_id = uuid();
