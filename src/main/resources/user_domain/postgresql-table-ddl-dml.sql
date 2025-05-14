SHOW wal_level;

SELECT *
FROM pg_publication
WHERE pubname = 'user_domain_pub';

SELECT pg_drop_replication_slot('user_slot');

SELECT *
FROM pg_replication_slots;

SELECT *
FROM pg_replication_slots
WHERE slot_name = 'user_slot';

SELECT *
FROM pg_publication_tables
WHERE pubname = 'user_domain_pub';


CREATE TABLE IF NOT EXISTS user_profile
(
    user_id    BIGINT PRIMARY KEY,
    profile    JSONB NOT NULL,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_preferences
(
    user_id     BIGINT PRIMARY KEY,
    preferences JSONB NOT NULL,
    updated_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_privacy
(
    user_id    BIGINT PRIMARY KEY,
    privacy    JSONB NOT NULL,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_compliance
(
    user_id    BIGINT PRIMARY KEY,
    compliance JSONB NOT NULL,
    updated_at TIMESTAMP DEFAULT now()
);


CREATE OR REPLACE VIEW full_user_view AS
SELECT p.user_id,
       p.profile,
       pr.preferences,
       pv.privacy,
       c.compliance
FROM user_profile p
         LEFT JOIN user_preferences pr ON p.user_id = pr.user_id
         LEFT JOIN user_privacy pv ON p.user_id = pv.user_id
         LEFT JOIN user_compliance c ON p.user_id = c.user_id;



INSERT INTO user_profile (id, profile)
VALUES (1001, '{
  "name": "Alice Johnson",
  "email": "alice.j@example.com",
  "dob": "1990-04-23",
  "phone": "+14085550123",
  "address": "123 Market Street, San Jose, CA"
}'),

       (1002, '{
         "name": "Bob Smith",
         "email": "bob.smith@example.com",
         "dob": "1985-06-15",
         "phone": "+14085550124",
         "address": "456 Santa Clara St, San Jose, CA"
       }'),

       (1003, '{
         "name": "Carol Lee",
         "email": "carol.lee@example.com",
         "dob": "1992-11-30",
         "phone": "+14085550125",
         "address": "789 Almaden Blvd, San Jose, CA"
       }'),

       (1004, '{
         "name": "David Park",
         "email": "david.park@example.com",
         "dob": "1988-02-19",
         "phone": "+14085550126",
         "address": "321 San Carlos St, San Jose, CA"
       }'),

       (1005, '{
         "name": "Eva Chen",
         "email": "eva.chen@example.com",
         "dob": "1995-08-01",
         "phone": "+14085550127",
         "address": "654 Willow St, San Jose, CA 95125"
       }');



INSERT INTO user_privacy (user_id, privacy)
VALUES (1001, '{
  "gdpr_consent": true,
  "marketing_consent": true,
  "consent_timestamp": "2025-05-12T16:00:00Z"
}'),

       (1002, '{
         "gdpr_consent": true,
         "marketing_consent": false,
         "consent_timestamp": "2025-05-11T10:30:00Z"
       }'),

       (1003, '{
         "gdpr_consent": false,
         "marketing_consent": false,
         "consent_timestamp": "2025-05-10T08:15:00Z"
       }'),

       (1004, '{
         "gdpr_consent": true,
         "marketing_consent": true,
         "consent_timestamp": "2025-05-09T14:45:00Z"
       }'),

       (1005, '{
         "gdpr_consent": true,
         "marketing_consent": true,
         "consent_timestamp": "2025-05-12T18:00:00Z"
       }');

INSERT INTO user_preferences (user_id, preferences)
VALUES (1001, '{
  "language": "en",
  "timezone": "America/Los_Angeles",
  "notification_channels": [
    "email",
    "push"
  ],
  "notification_time": [
    "evening"
  ]
}'),

       (1002, '{
         "language": "fr",
         "timezone": "Europe/Paris",
         "notification_channels": [
           "sms"
         ],
         "notification_time": [
           "morning",
           "daytime"
         ]
       }'),

       (1003, '{
         "language": "de",
         "timezone": "Europe/Berlin",
         "notification_channels": [
           "email"
         ],
         "notification_time": [
           "night"
         ]
       }'),

       (1004, '{
         "language": "es",
         "timezone": "America/Mexico_City",
         "notification_channels": [
           "push",
           "sms"
         ],
         "notification_time": [
           "daytime"
         ]
       }'),

       (1005, '{
         "language": "en",
         "timezone": "America/Los_Angeles",
         "notification_channels": [
           "email",
           "sms"
         ],
         "notification_time": [
           "evening"
         ]
       }');

INSERT INTO user_compliance (user_id, compliance)
VALUES (1001, '{
  "kyc_status": "verified",
  "aml_score": 12.5,
  "last_checked": "2025-05-12T14:20:00Z"
}'),

       (1002, '{
         "kyc_status": "pending",
         "aml_score": 35.0,
         "last_checked": "2025-05-10T09:45:00Z"
       }'),

       (1003, '{
         "kyc_status": "failed",
         "aml_score": 82.3,
         "last_checked": "2025-05-08T12:30:00Z"
       }'),

       (1004, '{
         "kyc_status": "verified",
         "aml_score": 5.4,
         "last_checked": "2025-05-11T16:10:00Z"
       }'),

       (1005, '{
         "kyc_status": "verified",
         "aml_score": 9.8,
         "last_checked": "2025-05-12T18:05:00Z"
       }');
