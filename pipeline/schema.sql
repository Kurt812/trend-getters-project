CREATE SCHEMA if NOT EXISTS 


SET DateStyle TO 'European'; 
CREATE TABLE user (
    user_id INT GENERATED ALWAYS AS IDENTITY,
    first_name VARCHAR(30) NOT NULL,
    last_name VARCHAR(30) NOT NULL,
    phone_number VARCHAR(20) UNIQUE,
    PRIMARY KEY (user_id)
);

CREATE TABLE subscription (
    subscription_id INT GENERATED ALWAYS AS IDENTITY,
    user_id INT NOT NULL,
    topic_id INT NOT NULL,
    subscription_status BOOLEAN NOT NULL,
    notification_threshold SMALLINT,
    PRIMARY KEY (subscription_id),
    FOREIGN KEY (user_id) REFERENCES user(user_id) ON DELETE CASCADE,
    FOREIGN KEY (topic_id) REFERENCES topic(topic_id) ON DELETE CASCADE
);

CREATE TABLE topic (
    topic_id INT GENERATED ALWAYS AS IDENTITY,
    topic_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (topic_id)
);

CREATE TABLE keyword_assignment (
    keyword_assignment_id BIGINT GENERATED ALWAYS AS IDENTITY,
    topic_id BIGINT,
    keywords_id BIGINT,
    PRIMARY KEY (keyword_assignment_id),
    FOREIGN KEY (topic_id) REFERENCES topic(topic_id) ON DELETE CASCADE,
    FOREIGN KEY (keywords_id) REFERENCES keywords(keywords_id) ON DELETE CASCADE
);

CREATE TABLE keywords (
    keywords_id BIGINT GENERATED ALWAYS AS IDENTITY,
    keyword VARCHAR(50) NOT NULL,
    PRIMARY KEY (keywords_id)
); 

CREATE TABLE trend_data (
    trend_data_id BIGINT GENERATED ALWAYS AS IDENTITY,
    topic_id INT,
    data_source_id SMALLINT,
    time_recorded TIMESTAMPTZ NOT NULL,
    sentiment_score FLOAT NOT NULL,
    mentions_count SMALLINT NOT NULL,
    related_terms VARCHAR(255) NOT NULL,
    PRIMARY KEY (trend_data_id),
    FOREIGN KEY (topic_id) REFERENCES topic(topic_id) ON DELETE CASCADE,
    FOREIGN KEY (data_source_id) REFERENCES data_source(data_source_id) ON DELETE CASCADE
);

CREATE TABLE data_source (
    data_source_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(255) NOT NULL,
    api_endpoint VARCHAR(255) NOT NULL,
    PRIMARY KEY (data_source_id)
);