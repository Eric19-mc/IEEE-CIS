from pydantic import BaseModel
from fastapi import FastAPI, Query
import pymysql
import os


app = FastAPI(
    title="FinGuard Risk API",
    description="IEEE-CIS 实时风控数据接口",
    version="1.0.0",
)


# =========================================================
# Doris 配置
# =========================================================

DORIS_HOST = os.getenv(
    "DORIS_HOST",
    "192.168.56.101",
)

DORIS_PORT = int(
    os.getenv(
        "DORIS_PORT",
        "9030",
    )
)

DORIS_USER = os.getenv(
    "DORIS_USER",
    "root",
)

DORIS_PASSWORD = os.getenv(
    "DORIS_PASSWORD",
    "",
)

DORIS_DATABASE = os.getenv(
    "DORIS_DATABASE",
    "finguard",
)


def get_connection():

    return pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        database=DORIS_DATABASE,
        cursorclass=pymysql.cursors.DictCursor,
    )


# =========================================================
# 基础测试
# =========================================================

@app.get("/")
def root():

    return {
        "service": "FinGuard Risk API",
        "status": "running",
    }


# =========================================================
# 风险概览
# =========================================================

@app.get("/api/risk/summary")
def risk_summary():

    conn = get_connection()

    try:

        with conn.cursor() as cursor:

            sql = """
                  SELECT
                      COUNT(*) AS total_transactions,

                      SUM(
                              CASE
                                  WHEN risk_level = 'HIGH'
                                      THEN 1
                                  ELSE 0
                                  END
                      ) AS high_risk,

                      SUM(
                              CASE
                                  WHEN risk_level = 'MEDIUM'
                                      THEN 1
                                  ELSE 0
                                  END
                      ) AS medium_risk,

                      SUM(
                              CASE
                                  WHEN risk_level = 'LOW'
                                      THEN 1
                                  ELSE 0
                                  END
                      ) AS low_risk,

                      ROUND(
                              AVG(fraud_probability),
                              4
                      ) AS avg_fraud_probability

                  FROM ads_realtime_risk \
                  """

            cursor.execute(sql)

            result = cursor.fetchone()

            total = (
                    result["total_transactions"]
                    or 0
            )

            high = (
                    result["high_risk"]
                    or 0
            )

            high_rate = (
                round(
                    high / total * 100,
                    2,
                    )
                if total > 0
                else 0
            )

            return {
                "total_transactions": total,
                "high_risk": high,
                "medium_risk": (
                        result["medium_risk"]
                        or 0
                ),
                "low_risk": (
                        result["low_risk"]
                        or 0
                ),
                "high_risk_rate": high_rate,
                "avg_fraud_probability": (
                        result["avg_fraud_probability"]
                        or 0
                ),
            }

    finally:

        conn.close()


# =========================================================
# 最近风险交易
# =========================================================

@app.get("/api/risk/recent")
def recent_risk(
        limit: int = Query(
            default=20,
            ge=1,
            le=100,
        )
):

    conn = get_connection()

    try:

        with conn.cursor() as cursor:

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

                  ORDER BY process_time DESC

                      LIMIT %s \
                  """

            cursor.execute(
                sql,
                (limit,),
            )

            rows = cursor.fetchall()

            return {
                "count": len(rows),
                "data": rows,
            }

    finally:

        conn.close()


# =========================================================
# 风险趋势
# =========================================================

@app.get("/api/risk/trend")
def risk_trend(
        hours: int = Query(
            default=24,
            ge=1,
            le=168,
        )
):

    conn = get_connection()

    try:

        with conn.cursor() as cursor:

            sql = """
                  SELECT
                      DATE_FORMAT(
                              process_time,
                              '%%Y-%%m-%%d %%H:00:00'
                      ) AS time_point,

                      COUNT(*) AS total,

                      SUM(
                              CASE
                                  WHEN risk_level = 'HIGH'
                                      THEN 1
                                  ELSE 0
                                  END
                      ) AS high_risk,

                      SUM(
                              CASE
                                  WHEN risk_level = 'MEDIUM'
                                      THEN 1
                                  ELSE 0
                                  END
                      ) AS medium_risk,

                      SUM(
                              CASE
                                  WHEN risk_level = 'LOW'
                                      THEN 1
                                  ELSE 0
                                  END
                      ) AS low_risk

                  FROM ads_realtime_risk

                  WHERE process_time >=
                        DATE_SUB(
                                NOW(),
                                INTERVAL %s HOUR
                        )

                  GROUP BY
                      DATE_FORMAT(
                              process_time,
                              '%%Y-%%m-%%d %%H:00:00'
                      )

                  ORDER BY time_point \
                  """

            cursor.execute(
                sql,
                (hours,),
            )

            rows = cursor.fetchall()

            return {
                "hours": hours,
                "data": rows,
            }

    finally:

        conn.close()


# =========================================================
# AI Agent
# =========================================================

class AgentRequest(BaseModel):

    message: str


@app.post("/api/agent/chat")
def agent_chat(
        request: AgentRequest,
):

    from agent.agent import ask_agent

    result = ask_agent(
        request.message
    )

    return {
        "message": request.message,
        "answer": result,
    }