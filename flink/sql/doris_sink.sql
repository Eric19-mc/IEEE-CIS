-- =========================================================
-- FinGuard
-- Kafka risk_output -> Flink -> Doris
-- =========================================================

-- 1. Kafka Source
CREATE TABLE risk_output_source (
                                    TransactionID BIGINT,
                                    TransactionAmt DECIMAL(18,3),
                                    ProductCD STRING,
                                    card4 STRING,
                                    card6 STRING,
                                    TransactionDT BIGINT,
                                    subject_key STRING,
                                    fraud_probability DOUBLE,
                                    behavior_score DOUBLE,
                                    behavior_level STRING,
                                    transaction_count INT,
                                    total_amount DECIMAL(18,3),
                                    max_amount DECIMAL(18,3),
                                    avg_amount DECIMAL(18,3),
                                    risk_score INT,
                                    risk_level STRING,
                                    risk_reason STRING,
                                    process_time STRING
)
    WITH (
        'connector' = 'kafka',
        'topic' = 'risk_output',
        'properties.bootstrap.servers' = 'master:9092,worker01:9092,worker02:9092',
        'properties.group.id' = 'finguard_doris_sink',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
        );


-- 2. Doris Sink
CREATE TABLE doris_risk_sink (
                                 TransactionID BIGINT,
                                 TransactionAmt DECIMAL(18,3),
                                 ProductCD STRING,
                                 card4 STRING,
                                 card6 STRING,
                                 TransactionDT BIGINT,
                                 subject_key STRING,
                                 fraud_probability DOUBLE,
                                 behavior_score DOUBLE,
                                 behavior_level STRING,
                                 transaction_count INT,
                                 total_amount DECIMAL(18,3),
                                 max_amount DECIMAL(18,3),
                                 avg_amount DECIMAL(18,3),
                                 rule_score INT,
                                 risk_score INT,
                                 risk_level STRING,
                                 risk_reason STRING,
                                 process_time TIMESTAMP(0),
                                 PRIMARY KEY (TransactionID) NOT ENFORCED
)
    WITH (
        'connector' = 'doris',
        'fenodes' = '192.168.56.101:8050',
        'table.identifier' = 'finguard.ads_realtime_risk',
        'username' = 'root',
        'password' = '',
        'sink.label-prefix' = 'finguard_risk_sink',
        'sink.enable-2pc' = 'false'
        );


-- 3. Kafka -> Doris
INSERT INTO doris_risk_sink
SELECT
    TransactionID,
    TransactionAmt,
    ProductCD,
    card4,
    card6,
    TransactionDT,
    subject_key,
    fraud_probability,
    behavior_score,
    behavior_level,
    transaction_count,
    total_amount,
    max_amount,
    avg_amount,
    0 AS rule_score,
    risk_score,
    risk_level,
    risk_reason,
    CAST(process_time AS TIMESTAMP(0))
FROM risk_output_source;