CREATE TABLE IF NOT EXISTS user_profile
(
    user_id    BIGINT PRIMARY KEY,
    profile JSONB NOT NULL,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_preferences
(
    user_id    BIGINT PRIMARY KEY,
    preferences JSONB NOT NULL,
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_privacy
(
    user_id    BIGINT PRIMARY KEY,
    privacy     JSONB NOT NULL,
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


-- Profile data
INSERT INTO user_profile (user_id, profile)
VALUES (1001,
        '{
          "full_name": "Alice Johnson",
          "email": "alice.j@example.com",
          "dob": "1990-04-23",
          "phone": "+15555550123"
        }');

-- Preferences data
INSERT INTO user_preferences (user_id, preferences)
VALUES (1001,
        '{
          "language": "en",
          "timezone": "America/Los_Angeles",
          "notification_channels": ["email", "push"]
        }');

-- Privacy data
INSERT INTO user_privacy (user_id, privacy)
VALUES (1001,
        '{
          "gdpr_consent": true,
          "marketing_consent": false,
          "consent_timestamp": "2025-05-12T16:02:00Z"
        }');

-- Compliance data
INSERT INTO user_compliance (user_id, compliance)
VALUES (1001,
        '{
          "kyc_status": "verified",
          "aml_score": 3.2,
          "last_checked": "2025-05-11T18:30:00Z"
        }');












INSERT INTO user_profile (user_id, profile)
VALUES (
           1002,
           '{
             "full_name": "Bob Smith",
             "email": "bob@example.com",
             "dob": "1985-07-12",
             "phone": "+15551234567"
           }'
       );