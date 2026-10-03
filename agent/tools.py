import os
import pymysql


# =========================================================
# Doris 配置
# =========================================================

DORIS_HOST = os.getenv("DORIS_HOST", "192.168.56.101")
DORIS_PORT = int(os.getenv("DORIS_PORT", "9030"))
DORIS_USER = os.getenv("DORIS_USER", "root")
DORIS_PASSWORD = os.getenv("DORIS_PASSWORD", "")
DORIS_DATABASE = os.getenv("DORIS_DATABASE", "finguard")


def get_connection():
    return pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        database=DORIS_DATABASE,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
        read_timeout=10,
        write_timeout=10,
    )


# =========================================================
# 1. 获取风险概览
# =========================================================

def get_risk_summary():
    sql = """
          SELECT
              total_transactions,
              high_risk_count,
              medium_risk_count,
              low_risk_count,
              avg_fraud_probability,
              avg_risk_score,
              latest_process_time
          FROM v_risk_summary \
          """

    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()

    finally:
        conn.close()


# =========================================================
# 2. 获取最近高风险交易
# =========================================================

def get_recent_high_risk(limit=10):
    sql = f"""
    SELECT
        TransactionID,
        TransactionAmt,
        fraud_probability,
        rule_score,
        risk_score,
        risk_level,
        risk_reason,
        process_time
    FROM v_recent_high_risk
    LIMIT {int(limit)}
    """

    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    finally:
        conn.close()


# =========================================================
# 3. 查询指定交易
# =========================================================

def get_transaction(transaction_id):
    sql = """
          SELECT
              TransactionID,
              TransactionAmt,
              ProductCD,
              card4,
              card6,
              fraud_probability,
              rule_score,
              risk_score,
              risk_level,
              risk_reason,
              process_time
          FROM ads_realtime_risk
          WHERE TransactionID = %s
          ORDER BY process_time DESC
              LIMIT 1 \
          """

    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (transaction_id,))
            return cursor.fetchone()

    finally:
        conn.close()