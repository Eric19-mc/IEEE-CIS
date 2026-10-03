-- =========================================================
-- FinGuard
-- Kafka risk_output -> Flink Source
-- 用于检查风险结果字段是否正确解析
-- =========================================================

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
        'properties.group.id' = 'finguard_risk_check',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'json'
        );

SELECT
    TransactionID,
    behavior_score,
    behavior_level,
    transaction_count,
    total_amount,
    max_amount,
    avg_amount
FROM risk_output_source
         LIMIT 5;