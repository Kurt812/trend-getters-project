CREATE SCHEMA IF NOT EXISTS :schema_name;
SET search_path TO :schema_name;

DROP TABLE IF EXISTS trend_data;
DROP TABLE IF EXISTS keyword_assignment;
DROP TABLE IF EXISTS keywords;
DROP TABLE IF EXISTS subscription;
DROP TABLE IF EXISTS topic;
DROP TABLE IF EXISTS "user";
DROP TABLE IF EXISTS data_source;


SET DateStyle TO 'European'; 
CREATE TABLE IF NOT EXISTS "user" (
    user_id INT GENERATED ALWAYS AS IDENTITY,
    first_name VARCHAR(30) NOT NULL,
    last_name VARCHAR(30) NOT NULL,
    phone_number VARCHAR(20) UNIQUE,
    PRIMARY KEY (user_id)
);

CREATE TABLE IF NOT EXISTS topic (
    topic_id INT GENERATED ALWAYS AS IDENTITY,
    topic_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (topic_id)
);

CREATE TABLE IF NOT EXISTS data_source (
    data_source_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(255) NOT NULL,
    website_url VARCHAR(255) NOT NULL,
    PRIMARY KEY (data_source_id)
);

CREATE TABLE IF NOT EXISTS subscription (
    subscription_id INT GENERATED ALWAYS AS IDENTITY,
    user_id INT NOT NULL,
    topic_id INT NOT NULL,
    subscription_status BOOLEAN NOT NULL,
    notification_threshold SMALLINT,
    PRIMARY KEY (subscription_id),
    FOREIGN KEY (user_id) REFERENCES "user"(user_id),
    FOREIGN KEY (topic_id) REFERENCES topic(topic_id)
);


CREATE TABLE IF NOT EXISTS keywords (
    keywords_id BIGINT GENERATED ALWAYS AS IDENTITY,
    keyword VARCHAR(50) NOT NULL,
    PRIMARY KEY (keywords_id)
); 

CREATE TABLE IF NOT EXISTS keyword_assignment (
    keyword_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    topic_id BIGINT,
    keywords_id BIGINT,
    PRIMARY KEY (keyword_assignment_id),
    FOREIGN KEY (topic_id) REFERENCES topic(topic_id),
    FOREIGN KEY (keywords_id) REFERENCES keywords(keywords_id)
);


CREATE TABLE IF NOT EXISTS trend_data (
    trend_data_id BIGINT GENERATED ALWAYS AS IDENTITY,
    topic_id INT,
    data_source_id SMALLINT,
    time_recorded TIMESTAMPTZ NOT NULL,
    sentiment_score FLOAT NOT NULL,
    mentions_count SMALLINT NOT NULL,
    related_terms VARCHAR(255) NOT NULL,
    PRIMARY KEY (trend_data_id),
    FOREIGN KEY (topic_id) REFERENCES topic(topic_id),
    FOREIGN KEY (data_source_id) REFERENCES data_source(data_source_id)
);

-- Seed data source Data
INSERT INTO data_source (name, website_url) VALUES 
('BlueSky Firehose', 'https://bsky.social/'),
('Google Trends', 'https://trends.google.com/trends/')
