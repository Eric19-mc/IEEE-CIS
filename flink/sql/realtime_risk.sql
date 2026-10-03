-- ============================================================
-- FinGuard 实时风控 Flink SQL
-- Kafka → Flink → Doris
--
-- 数据来源：
--   Kafka topic: transaction_topic
--
-- 数据目标：
--   Doris: finguard.ads_realtime_risk
--
-- 风险判断：
--   基于交易金额、卡类型、交易时间进行规则评分
--
-- 注意：
--   fraud_probability 为规则计算得到的风险概率估计，
--   不是机器学习模型预测结果。
-- ============================================================


-- ============================================================
-- 1. Checkpoint
-- ============================================================

SET 'execution.checkpointing.interval' = '10 s';


-- ============================================================
-- 2. Kafka Source
-- ============================================================

CREATE TABLE transaction_source (
    TransactionID BIGINT,
TransactionAmt DOUBLE,
ProductCD STRING,
card4 STRING,
card6 STRING,
TransactionDT BIGINT
) WITH (
    'connector' = 'kafka',
'topic' = 'transaction_topic',
'properties.bootstrap.servers' = 'master:9092,worker01:9092,worker02:9092',
'properties.group.id' = 'finguard_risk_group',
'scan.startup.mode' = 'earliest-offset',
'format' = 'json'
);


-- ============================================================
-- 3. Doris Sink
-- ============================================================

CREATE TABLE doris_risk_sink (
    TransactionID BIGINT,
TransactionAmt DECIMAL(18,3),
ProductCD STRING,
card4 STRING,
card6 STRING,
TransactionDT BIGINT,

fraud_probability DOUBLE,
rule_score INT,
risk_score INT,
risk_level STRING,
risk_reason STRING,

process_time TIMESTAMP(3)
) WITH (
    'connector' = 'doris',
'fenodes' = '192.168.56.101:8050',
'table.identifier' = 'finguard.ads_realtime_risk',
'username' = 'root',
'password' = '',
'sink.properties.format' = 'json',
'sink.properties.strip_outer_array' = 'true'
);


-- ============================================================
-- 4. 实时风险计算
-- ============================================================

INSERT INTO doris_risk_sink

SELECT
TransactionID,
CAST(TransactionAmt AS DECIMAL(18,3)),
ProductCD,
card4,
card6,
TransactionDT,

-- --------------------------------------------------------
-- fraud_probability
--
-- 基础概率：
--   >= 500  → 0.10
--   >= 200  → 0.06
--   >= 100  → 0.03
--   其他    → 0.01
--
-- 信用卡 +0.005
-- 深夜交易 +0.02
-- --------------------------------------------------------

CAST(
    (
        CASE
        WHEN TransactionAmt >= 500 THEN 0.10
        WHEN TransactionAmt >= 200 THEN 0.06
        WHEN TransactionAmt >= 100 THEN 0.03
        ELSE 0.01
        END

        +

        CASE
        WHEN card6 = 'credit' THEN 0.005
        ELSE 0
        END

        +

        CASE
        WHEN MOD(TransactionDT, 86400) < 21600 THEN 0.02
ELSE 0
END
)
AS DOUBLE
) AS fraud_probability,


-- --------------------------------------------------------
-- rule_score
--
-- 金额：
--   >= 500  → +20
--   >= 200  → +10
--   >= 100  → +5
--
-- 信用卡：
--   +5
--
-- 深夜：
--   +10
-- --------------------------------------------------------

(
    CASE
    WHEN TransactionAmt >= 500 THEN 20
    WHEN TransactionAmt >= 200 THEN 10
    WHEN TransactionAmt >= 100 THEN 5
    ELSE 0
    END

    +

    CASE
    WHEN card6 = 'credit' THEN 5
    ELSE 0
    END

    +

    CASE
    WHEN MOD(TransactionDT, 86400) < 21600 THEN 10
ELSE 0
END
) AS rule_score,


-- --------------------------------------------------------
-- risk_score
--
-- risk_score = rule_score + fraud_probability * 100
-- --------------------------------------------------------

CAST(
    (
        CASE
        WHEN TransactionAmt >= 500 THEN 20
        WHEN TransactionAmt >= 200 THEN 10
        WHEN TransactionAmt >= 100 THEN 5
        ELSE 0
        END

        +

        CASE
        WHEN card6 = 'credit' THEN 5
        ELSE 0
        END

        +

        CASE
        WHEN MOD(TransactionDT, 86400) < 21600 THEN 10
ELSE 0
END

+

(
        (
            CASE
            WHEN TransactionAmt >= 500 THEN 0.10
            WHEN TransactionAmt >= 200 THEN 0.06
            WHEN TransactionAmt >= 100 THEN 0.03
            ELSE 0.01
            END

            +

            CASE
            WHEN card6 = 'credit' THEN 0.005
            ELSE 0
            END

            +

            CASE
            WHEN MOD(TransactionDT, 86400) < 21600 THEN 0.02
ELSE 0
END
) * 100
)
)
AS INT
) AS risk_score,


-- --------------------------------------------------------
-- risk_level
--
-- >= 50 → HIGH
-- >= 30 → MEDIUM
-- 其他   → LOW
-- --------------------------------------------------------

CASE
WHEN
(
    CASE
    WHEN TransactionAmt >= 500 THEN 20
    WHEN TransactionAmt >= 200 THEN 10
    WHEN TransactionAmt >= 100 THEN 5
    ELSE 0
    END

    +

    CASE
    WHEN card6 = 'credit' THEN 5
    ELSE 0
    END

    +

    CASE
    WHEN MOD(TransactionDT, 86400) < 21600 THEN 10
ELSE 0
END

+

(
        (
            CASE
            WHEN TransactionAmt >= 500 THEN 0.10
            WHEN TransactionAmt >= 200 THEN 0.06
            WHEN TransactionAmt >= 100 THEN 0.03
            ELSE 0.01
            END

            +

            CASE
            WHEN card6 = 'credit' THEN 0.005
            ELSE 0
            END

            +

            CASE
            WHEN MOD(TransactionDT, 86400) < 21600 THEN 0.02
ELSE 0
END
) * 100
)
) >= 50
THEN 'HIGH'

WHEN
(
    CASE
    WHEN TransactionAmt >= 500 THEN 20
    WHEN TransactionAmt >= 200 THEN 10
    WHEN TransactionAmt >= 100 THEN 5
    ELSE 0
    END

    +

    CASE
    WHEN card6 = 'credit' THEN 5
    ELSE 0
    END

    +

    CASE
    WHEN MOD(TransactionDT, 86400) < 21600 THEN 10
ELSE 0
END

+

(
        (
            CASE
            WHEN TransactionAmt >= 500 THEN 0.10
            WHEN TransactionAmt >= 200 THEN 0.06
            WHEN TransactionAmt >= 100 THEN 0.03
            ELSE 0.01
            END

            +

            CASE
            WHEN card6 = 'credit' THEN 0.005
            ELSE 0
            END

            +

            CASE
            WHEN MOD(TransactionDT, 86400) < 21600 THEN 0.02
ELSE 0
END
) * 100
)
) >= 30
THEN 'MEDIUM'

ELSE 'LOW'
END AS risk_level,


-- --------------------------------------------------------
-- risk_reason
-- --------------------------------------------------------

CASE
WHEN TransactionAmt >= 500
AND card6 = 'credit'
AND MOD(TransactionDT, 86400) < 21600
THEN '大额交易;信用卡;深夜交易'

WHEN TransactionAmt >= 500
THEN '大额交易'

WHEN card6 = 'credit'
AND MOD(TransactionDT, 86400) < 21600
THEN '信用卡;深夜交易'

WHEN card6 = 'credit'
THEN '信用卡'

WHEN MOD(TransactionDT, 86400) < 21600
THEN '深夜交易'

ELSE '普通交易'
END AS risk_reason,


-- --------------------------------------------------------
-- 实时处理时间
-- --------------------------------------------------------

CURRENT_TIMESTAMP AS process_time

FROM transaction_source;