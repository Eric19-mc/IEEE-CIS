import os

import pandas as pd
import pymysql
import requests
import streamlit as st


# =========================================================
# 页面配置
# =========================================================

st.set_page_config(
    page_title="FinGuard 实时风控监控平台",
    page_icon="🛡️",
    layout="wide",
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


# =========================================================
# FastAPI / AI Agent 配置
# =========================================================

AI_API_URL = os.getenv(
    "AI_API_URL",
    "http://127.0.0.1:8000/api/agent/chat",
)


# =========================================================
# 页面标题
# =========================================================

st.title(
    "🛡️ FinGuard 银行实时风控监控平台"
)

st.caption(
    "实时交易监控 · 欺诈概率预测 · 行为风险分析 · "
    "风险融合 · Doris 数据展示 · AI 风控分析"
)


# =========================================================
# Doris 连接
# =========================================================

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
# Doris 查询
# =========================================================

def query_doris(sql: str) -> pd.DataFrame:

    conn = None

    try:

        conn = get_connection()

        with conn.cursor() as cursor:

            cursor.execute(sql)

            rows = cursor.fetchall()

        return pd.DataFrame(rows)

    except Exception as e:

        st.error(
            f"数据库查询失败：{e}"
        )

        return pd.DataFrame()

    finally:

        if conn:

            conn.close()


# =========================================================
# AI Agent
# =========================================================

def ask_ai(question: str):

    try:

        response = requests.post(
            AI_API_URL,
            json={
                "message": question,
            },
            timeout=60,
        )

        response.raise_for_status()

        data = response.json()

        return data.get(
            "answer",
            "AI 未返回有效结果。",
        )

    except requests.exceptions.ConnectionError:

        return (
            "无法连接到 AI 服务。\n\n"
            "请确认 FastAPI 已启动：\n"
            "`python -m uvicorn api.main:app "
            "--host 0.0.0.0 --port 8000`"
        )

    except requests.exceptions.Timeout:

        return (
            "AI 服务响应超时，请稍后重试。"
        )

    except requests.exceptions.HTTPError as e:

        try:

            detail = response.json().get(
                "detail",
                str(e),
            )

        except Exception:

            detail = str(e)

        return (
            f"AI 服务调用失败：{detail}"
        )

    except Exception as e:

        return (
            f"AI 分析失败：{e}"
        )


# =========================================================
# 查询函数
# =========================================================

def load_summary():

    return query_doris(
        """
        SELECT *
        FROM v_risk_summary
        """
    )


def load_distribution():

    return query_doris(
        """
        SELECT *
        FROM v_risk_distribution
        ORDER BY
            CASE risk_level
                WHEN 'HIGH' THEN 1
                WHEN 'MEDIUM' THEN 2
                WHEN 'LOW' THEN 3
                ELSE 4
                END
        """
    )


def load_hourly():

    return query_doris(
        """
        SELECT *
        FROM v_risk_hourly
        ORDER BY stat_hour
        """
    )


def load_high_risk():

    return query_doris(
        """
        SELECT *
        FROM v_recent_high_risk
        ORDER BY
            process_time DESC,
            TransactionID DESC
            LIMIT 20
        """
    )


def load_recent_transactions():

    return query_doris(
        """
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
        WHERE fraud_probability IS NOT NULL
        ORDER BY
            process_time DESC,
            TransactionID DESC
            LIMIT 20
        """
    )


# =========================================================
# 手动刷新
# =========================================================

if st.button(
        "🔄 刷新数据",
        width="stretch",
):

    st.rerun()


# =========================================================
# Dashboard
# 每 10 秒自动刷新
# =========================================================

@st.fragment(run_every="10s")
def dashboard():

    # =====================================================
    # 查询数据
    # =====================================================

    summary = load_summary()
    distribution = load_distribution()
    hourly = load_hourly()
    high_risk = load_high_risk()
    recent = load_recent_transactions()

    # =====================================================
    # 无数据
    # =====================================================

    if summary.empty:

        st.warning(
            "暂时没有实时风控数据。"
        )

        return

    row = summary.iloc[0]

    # =====================================================
    # KPI
    # =====================================================

    total_transactions = int(
        row.get(
            "total_transactions",
            0,
        )
        or 0
    )

    high_risk_count = int(
        row.get(
            "high_risk_count",
            0,
        )
        or 0
    )

    medium_risk_count = int(
        row.get(
            "medium_risk_count",
            0,
        )
        or 0
    )

    low_risk_count = int(
        row.get(
            "low_risk_count",
            0,
        )
        or 0
    )

    avg_fraud_probability = float(
        row.get(
            "avg_fraud_probability",
            0,
        )
        or 0
    )

    avg_risk_score = float(
        row.get(
            "avg_risk_score",
            0,
        )
        or 0
    )

    latest_process_time = row.get(
        "latest_process_time"
    )

    # =====================================================
    # 实时概览
    # =====================================================

    st.subheader(
        "📊 实时风控概览"
    )

    col1, col2, col3, col4, col5 = st.columns(
        5
    )

    with col1:

        st.metric(
            "总交易数",
            f"{total_transactions:,}",
        )

    with col2:

        st.metric(
            "高风险交易",
            f"{high_risk_count:,}",
        )

    with col3:

        st.metric(
            "中风险交易",
            f"{medium_risk_count:,}",
        )

    with col4:

        st.metric(
            "低风险交易",
            f"{low_risk_count:,}",
        )

    with col5:

        st.metric(
            "平均风险评分",
            f"{avg_risk_score:.2f}",
        )

    # =====================================================
    # 平均欺诈概率 + 更新时间
    # =====================================================

    st.caption(
        f"平均欺诈概率："
        f"{avg_fraud_probability:.2%}"
    )

    if latest_process_time is not None:

        st.caption(
            f"最近处理时间："
            f"{latest_process_time}"
        )

    # =====================================================
    # 风险分布
    # =====================================================

    st.divider()

    st.subheader(
        "🎯 风险等级分布"
    )

    col1, col2 = st.columns(
        [1.2, 1]
    )

    with col1:

        if not distribution.empty:

            chart_data = distribution.copy()

            chart_data["风险等级"] = (
                chart_data[
                    "risk_level"
                ].map(
                    {
                        "HIGH": "高风险",
                        "MEDIUM": "中风险",
                        "LOW": "低风险",
                    }
                )
            )

            chart_data = chart_data[
                [
                    "风险等级",
                    "transaction_count",
                ]
            ]

            chart_data = chart_data.rename(
                columns={
                    "transaction_count":
                        "交易数量"
                }
            )

            chart_data = chart_data.set_index(
                "风险等级"
            )

            st.bar_chart(
                chart_data,
                width="stretch",
            )

    with col2:

        if not distribution.empty:

            distribution_display = (
                distribution.copy()
            )

            distribution_display[
                "risk_level"
            ] = distribution_display[
                "risk_level"
            ].map(
                {
                    "HIGH": "高风险",
                    "MEDIUM": "中风险",
                    "LOW": "低风险",
                }
            )

            distribution_display = (
                distribution_display.rename(
                    columns={
                        "risk_level":
                            "风险等级",

                        "transaction_count":
                            "交易数量",

                        "avg_fraud_probability":
                            "平均欺诈概率",

                        "avg_risk_score":
                            "平均风险评分",
                    }
                )
            )

            if "平均欺诈概率" in (
                    distribution_display.columns
            ):

                distribution_display[
                    "平均欺诈概率"
                ] = pd.to_numeric(
                    distribution_display[
                        "平均欺诈概率"
                    ],
                    errors="coerce",
                ).map(
                    lambda x:
                    f"{x:.2%}"
                    if pd.notna(x)
                    else "-"
                )

            st.dataframe(
                distribution_display,
                hide_index=True,
                width="stretch",
            )

    # =====================================================
    # 每小时风险趋势
    # =====================================================

    st.divider()

    st.subheader(
        "⏱️ 每小时风险趋势"
    )

    if not hourly.empty:

        hourly_chart = hourly.copy()

        hourly_chart[
            "stat_hour"
        ] = pd.to_datetime(
            hourly_chart[
                "stat_hour"
            ]
        )

        hourly_chart = (
            hourly_chart.set_index(
                "stat_hour"
            )
        )

        hourly_chart = hourly_chart[
            [
                "high_risk_count",
                "medium_risk_count",
                "low_risk_count",
            ]
        ]

        hourly_chart = (
            hourly_chart.rename(
                columns={
                    "high_risk_count":
                        "高风险",

                    "medium_risk_count":
                        "中风险",

                    "low_risk_count":
                        "低风险",
                }
            )
        )

        st.line_chart(
            hourly_chart,
            width="stretch",
        )

    else:

        st.info(
            "暂无小时级趋势数据。"
        )

    # =====================================================
    # 最近高风险交易
    # =====================================================

    st.divider()

    st.subheader(
        "🚨 最近高风险交易"
    )

    if high_risk.empty:

        st.success(
            "当前暂无高风险交易。"
        )

    else:

        high_risk_display = (
            high_risk.copy()
        )

        high_risk_display = (
            high_risk_display.rename(
                columns={
                    "TransactionID":
                        "交易编号",

                    "TransactionAmt":
                        "交易金额",

                    "fraud_probability":
                        "欺诈概率",

                    "rule_score":
                        "行为评分",

                    "risk_score":
                        "综合风险评分",

                    "risk_level":
                        "风险等级",

                    "risk_reason":
                        "风险原因",

                    "process_time":
                        "处理时间",
                }
            )
        )

        if "欺诈概率" in (
                high_risk_display.columns
        ):

            high_risk_display[
                "欺诈概率"
            ] = pd.to_numeric(
                high_risk_display[
                    "欺诈概率"
                ],
                errors="coerce",
            ).map(
                lambda x:
                f"{x:.2%}"
                if pd.notna(x)
                else "-"
            )

        if "风险等级" in (
                high_risk_display.columns
        ):

            high_risk_display[
                "风险等级"
            ] = (
                high_risk_display[
                    "风险等级"
                ].map(
                    {
                        "HIGH": "高风险",
                        "MEDIUM": "中风险",
                        "LOW": "低风险",
                    }
                ).fillna(
                    high_risk_display[
                        "风险等级"
                    ]
                )
            )

        st.dataframe(
            high_risk_display,
            hide_index=True,
            width="stretch",
        )

    # =====================================================
    # 最近实时交易
    # =====================================================

    st.divider()

    st.subheader(
        "💳 最近实时交易"
    )

    if recent.empty:

        st.info(
            "暂无实时交易数据。"
        )

    else:

        recent_display = (
            recent.copy()
        )

        recent_display = (
            recent_display.rename(
                columns={
                    "TransactionID":
                        "交易编号",

                    "TransactionAmt":
                        "交易金额",

                    "ProductCD":
                        "产品类型",

                    "card4":
                        "卡组织",

                    "card6":
                        "卡类型",

                    "fraud_probability":
                        "欺诈概率",

                    "rule_score":
                        "行为评分",

                    "risk_score":
                        "综合风险评分",

                    "risk_level":
                        "风险等级",

                    "risk_reason":
                        "风险原因",

                    "process_time":
                        "处理时间",
                }
            )
        )

        if "欺诈概率" in (
                recent_display.columns
        ):

            recent_display[
                "欺诈概率"
            ] = pd.to_numeric(
                recent_display[
                    "欺诈概率"
                ],
                errors="coerce",
            ).map(
                lambda x:
                f"{x:.2%}"
                if pd.notna(x)
                else "-"
            )

        if "风险等级" in (
                recent_display.columns
        ):

            recent_display[
                "风险等级"
            ] = (
                recent_display[
                    "风险等级"
                ].map(
                    {
                        "HIGH": "高风险",
                        "MEDIUM": "中风险",
                        "LOW": "低风险",
                    }
                ).fillna(
                    recent_display[
                        "风险等级"
                    ]
                )
            )

        st.dataframe(
            recent_display,
            hide_index=True,
            width="stretch",
        )


# =========================================================
# 启动 Dashboard
# =========================================================

dashboard()


# =========================================================
# AI 风控分析
# =========================================================

st.divider()

st.subheader(
    "🤖 AI 风控分析"
)

st.caption(
    "通过 FastAPI 调用 FinGuard AI Agent，"
    "Agent 查询 Doris 风控数据后由 ARK 生成分析结果。"
)

question = st.text_input(
    "请输入风控问题",
    placeholder="例如：最近有哪些高风险交易？",
    key="ai_question",
)

if st.button(
        "🔍 开始分析",
        key="ai_analyze",
        width="stretch",
):

    if not question.strip():

        st.warning(
            "请输入问题。"
        )

    else:

        with st.spinner(
                "AI 正在分析风控数据，请稍候..."
        ):

            answer = ask_ai(
                question.strip()
            )

        st.markdown(
            "**🤖 AI 分析结果**"
        )

        st.info(answer)