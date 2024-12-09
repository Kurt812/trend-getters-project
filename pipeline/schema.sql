CREATE SCHEMA IF NOT EXISTS :schema_name;
SET search_path TO :schema_name;

DROP TABLE IF EXISTS subscription;
DROP TABLE IF EXISTS related_term_assignment;
DROP TABLE IF EXISTS related_terms;
DROP TABLE IF EXISTS keyword_recordings;
DROP TABLE IF EXISTS keywords;
DROP TABLE IF EXISTS "user";


SET DateStyle TO 'European'; 
CREATE TABLE IF NOT EXISTS "user" (
    user_id INT GENERATED ALWAYS AS IDENTITY,
    first_name VARCHAR(30) NOT NULL,
    last_name VARCHAR(30) NOT NULL,
    email VARCHAR(255) UNIQUE,
    PRIMARY KEY (user_id)
);


CREATE TABLE IF NOT EXISTS keywords (
    keywords_id BIGINT GENERATED ALWAYS AS IDENTITY,
    keyword VARCHAR(50) NOT NULL,
    PRIMARY KEY (keywords_id)
); 


CREATE TABLE IF NOT EXISTS keyword_recordings (
    keyword_recordings_id BIGINT GENERATED ALWAYS AS IDENTITY,
    keywords_id BIGINT,
    total_mentions SMALLINT,
    avg_sentiment FLOAT,
    date_and_hour TIMESTAMP,
    PRIMARY KEY (keyword_recordings_id),
    FOREIGN KEY (keywords_id) REFERENCES keywords(keywords_id)
);


CREATE TABLE IF NOT EXISTS related_terms (
    related_term_id BIGINT GENERATED ALWAYS AS IDENTITY,
    related_term VARCHAR(255) NOT NULL,
    PRIMARY KEY (related_term_id),
    FOREIGN KEY (related_term_id) REFERENCES related_terms(related_term_id)
);


CREATE TABLE IF NOT EXISTS related_term_assignment (
    related_term_assignment BIGINT GENERATED ALWAYS AS IDENTITY,
    keywords_id BIGINT,
    related_term_id BIGINT,
    PRIMARY KEY (related_term_assignment),
    FOREIGN KEY (keywords_id) REFERENCES keywords(keywords_id),
    FOREIGN KEY (related_term_id) REFERENCES related_terms(related_term_id)
);


CREATE TABLE IF NOT EXISTS subscription (
    subscription_id INT GENERATED ALWAYS AS IDENTITY,
    user_id INT NOT NULL,
    keywords_id INT NOT NULL,
    subscription_status BOOLEAN NOT NULL,
    notification_threshold SMALLINT,
    PRIMARY KEY (subscription_id),
    FOREIGN KEY (user_id) REFERENCES "user"(user_id),
    FOREIGN KEY (keywords_id) REFERENCES keywords(keywords_id)
);








