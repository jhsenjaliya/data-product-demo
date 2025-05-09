CREATE KEYSPACE demo WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 0};

CREATE TABLE demo.user_domain (
     user_id BIGINT PRIMARY KEY,

     profile TEXT,              -- Profile service
     preferences TEXT,          -- Preferences service
     auth TEXT,                 -- Auth service

     addresses TEXT,            -- Address service (array of objects)
     status TEXT,               -- Status service (e.g., ACTIVE, BLOCKED)
     consent TEXT,              -- Consent tracking service (GDPR, marketing)
     compliance TEXT,           -- Compliance service (KYC, AML, etc.)
     internal_metadata TEXT     -- Internal system-level fields (not exposed externally)
) WITH cdc = {'enabled': true};


