CREATE KEYSPACE demo WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE demo.user_domain (
     user_id BIGINT PRIMARY KEY,

     profile TEXT,              -- Profile service
     preferences TEXT,          -- Preferences service
     auth TEXT,                 -- Auth service

     privacy TEXT,              -- Consent tracking service (GDPR, marketing)
     compliance TEXT,           -- Compliance service (KYC, AML, etc.)
     internal_metadata TEXT     -- Internal system-level fields (not exposed externally)
) WITH cdc = {'enabled': true};


select * from demo.user_domain;


UPDATE demo.user_domain
SET profile = '{
  "full_name": "Alice Johnson",
  "email": "alice.j@example.com",
  "dob": "1990-04-23",
  "phone": "+15555550123"
}'
WHERE user_id = 1001;

UPDATE demo.user_domain
SET preferences = '{
  "language": "en",
  "timezone": "America/Los_Angeles",
  "notification_channels": ["email", "push"]
}'
WHERE user_id = 1001;

UPDATE demo.user_domain
SET privacy = '{
  "gdpr_consent": true,
  "marketing_consent": false,
  "consent_timestamp": "2025-05-12T16:02:00Z"
}'
WHERE user_id = 1001;

