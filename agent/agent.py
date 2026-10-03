import json
import os
import re

from dotenv import load_dotenv
from openai import OpenAI

from agent.tools import (
    get_risk_summary,
    get_recent_high_risk,
    get_transaction,
)


# =========================================================
# 加载项目根目录 .env
# =========================================================

load_dotenv()


# =========================================================
# ARK 配置
# =========================================================

MODEL_API_KEY = os.getenv("ARK_API_KEY")

BASE_URL = os.getenv(
    "ARK_BASE_URL",
    "https://ark.cn-beijing.volces.com/api/v3",
)

MODEL_NAME = "doubao-seed-2-0-lite-260428"


if not MODEL_API_KEY:
    raise RuntimeError(
        "未检测到 ARK_API_KEY，请检查项目根目录 .env。"
    )


client = OpenAI(
    api_key=MODEL_API_KEY,
    base_url=BASE_URL,
)


# =========================================================
# 工具执行
# =========================================================

def execute_tool(question: str):
    """
    根据用户问题选择对应的 Doris 查询工具。

    当前支持：
    1. 指定交易查询
    2. 最近高风险交易查询
    3. 整体风险概览
    """

    question = question.strip()

    if not question:
        return None

    # =====================================================
    # 1. 指定交易查询
    # =====================================================
    # 支持：
    #
    # 交易 3663556
    # 交易ID 3663556
    # 交易编号：3663556
    # TransactionID: 3663556
    # TransactionID 3663556
    # 3663556 这笔交易怎么样
    #
    # 注意：
    # 交易 ID 优先级最高。
    # =====================================================

    transaction_patterns = [
        r"(?:TransactionID|transaction_id|交易编号|交易ID|交易)\s*[:：]?\s*(\d+)",
        r"\b(\d{5,})\b\s*(?:这笔|该笔)?\s*交易",
    ]

    transaction_id = None

    for pattern in transaction_patterns:

        match = re.search(
            pattern,
            question,
            re.IGNORECASE,
        )

        if match:
            transaction_id = int(match.group(1))
            break

    if transaction_id is not None:

        result = get_transaction(
            transaction_id
        )

        return {
            "tool": "get_transaction",
            "data": result,
        }


    # =====================================================
    # 2. 高风险交易查询
    # =====================================================
    #
    # 明确询问高风险、危险交易时，
    # 只调用高风险工具。
    #
    # 不把“风险情况”这种普通概览问题误判成高风险。
    # =====================================================

    high_risk_keywords = [
        "高风险",
        "高危",
        "高风险交易",
        "高危交易",
        "危险交易",
        "风险最高",
        "风险最大的交易",
        "最危险的交易",
    ]

    if any(
            keyword in question
            for keyword in high_risk_keywords
    ):

        result = get_recent_high_risk(10)

        return {
            "tool": "get_recent_high_risk",
            "data": result,
        }


    # =====================================================
    # 3. 风险概览
    # =====================================================

    summary_keywords = [
        "风险概览",
        "风险情况",
        "风险统计",
        "整体风险",
        "整体情况",
        "总体风险",
        "总体情况",
        "交易情况",
        "最近情况",
        "风险怎么样",
        "风险如何",
        "整体怎么样",
        "总体怎么样",
        "最近风险",
        "当前风险",
        "当前情况",
        "现在风险",
        "现在情况",
    ]

    if any(
            keyword in question
            for keyword in summary_keywords
    ):

        result = get_risk_summary()

        return {
            "tool": "get_risk_summary",
            "data": result,
        }


    # =====================================================
    # 4. 无匹配
    # =====================================================

    return None


# =========================================================
# 工具结果格式化
# =========================================================

def format_context(data) -> str:
    """
    将 Doris 查询结果转换成稳定的 JSON 上下文。
    """

    return json.dumps(
        data,
        ensure_ascii=False,
        default=str,
        indent=2,
    )


# =========================================================
# AI 回答
# =========================================================

def ask_agent(question: str):
    """
    FinGuard AI 风控分析入口。

    流程：

    用户问题
        ↓
    execute_tool()
        ↓
    Doris 查询
        ↓
    数据约束 Prompt
        ↓
    ARK
        ↓
    中文风控分析
    """

    question = question.strip()

    if not question:
        return "请输入需要分析的风控问题。"


    # =====================================================
    # 执行工具
    # =====================================================

    tool_result = execute_tool(question)


    # =====================================================
    # 不支持的问题
    # =====================================================

    if tool_result is None:

        return (
            "目前支持以下风控查询：\n\n"
            "1. 最近整体风险情况\n"
            "2. 最近有哪些高风险交易\n"
            "3. 查询指定交易的风险情况\n\n"
            "例如：\n"
            "• 最近整体风险怎么样？\n"
            "• 最近有没有高风险交易？\n"
            "• 交易 3663556 怎么样？"
        )


    # =====================================================
    # Doris 数据
    # =====================================================

    context = format_context(
        tool_result["data"]
    )


    # =====================================================
    # 根据工具类型设置回答约束
    # =====================================================

    if tool_result["tool"] == "get_recent_high_risk":

        tool_instruction = """
当前查询类型：最近高风险交易查询。

回答要求：

1. 只根据查询结果判断 HIGH 高风险交易。
2. 如果查询结果为空，明确回答：
   “当前暂无高风险交易。”
3. 如果存在数据，只说明查询结果中实际存在的 HIGH 数据。
4. 严禁把 MEDIUM 中风险交易说成 HIGH 高风险交易。
5. 严禁因为存在 MEDIUM 数据就声称存在 HIGH 数据。
6. 可以列出部分高风险交易的交易编号、金额、
   欺诈概率、风险评分和风险原因。
7. 如果数据中没有某个字段，不要自行补充。
8. 不要自行判断“为什么风险高”，除非 risk_reason
   字段中已经提供了对应原因。
"""


    elif tool_result["tool"] == "get_transaction":

        tool_instruction = """
当前查询类型：指定交易查询。

回答时重点说明：

- TransactionID
- TransactionAmt
- fraud_probability
- rule_score
- risk_score
- risk_level
- risk_reason

回答要求：

1. 明确指出查询的是哪一笔交易。
2. 严格按照查询结果中的 risk_level 判断风险等级。
3. HIGH = 高风险。
4. MEDIUM = 中风险。
5. LOW = 低风险。
6. 不要自行修改风险等级。
7. 如果查询不到该交易，明确说明：
   “未查询到该交易。”
8. 如果某个字段为 NULL，不要自行猜测。
9. 不要凭交易金额、概率或评分自行创造数据库中不存在的风险原因。
"""


    elif tool_result["tool"] == "get_risk_summary":

        tool_instruction = """
当前查询类型：整体风险概览。

请根据查询结果说明：

- 总交易量
- 高风险交易数量
- 中风险交易数量
- 低风险交易数量
- 平均欺诈概率
- 平均风险评分

回答要求：

1. 只陈述查询结果中的统计数据。
2. 不要自行推测风险趋势。
3. 不要自行推测风险产生原因。
4. 不要根据个人判断添加“风险严重”“风险较高”
   等数据库中不存在的结论。
5. 如果某个统计字段不存在或为 NULL，
   明确说明该数据不可用。
"""


    else:

        tool_instruction = ""


    # =====================================================
    # AI Prompt
    # =====================================================

    prompt = f"""
你是 FinGuard 银行实时风控监控平台的 AI 风控分析助手。

你的任务是：

根据 Doris 查询结果，对用户提出的风控问题进行简洁、
准确、可追溯的数据解释。

=========================================================
用户问题
=========================================================

{question}


=========================================================
当前调用的工具
=========================================================

{tool_result["tool"]}


=========================================================
Doris 查询结果
=========================================================

{context}


=========================================================
当前查询类型的特殊要求
=========================================================

{tool_instruction}


=========================================================
通用回答规则
=========================================================

1. 只能使用 Doris 查询结果中的数据回答。

2. 严禁编造：
   - 交易编号
   - 交易金额
   - 欺诈概率
   - 行为评分
   - 风险评分
   - 风险等级
   - 风险原因
   - 统计数据

3. 如果查询结果为空：
   必须明确告诉用户当前没有对应数据。

4. 如果某个字段为 NULL：
   不允许自行推测。

5. 风险等级必须严格按照数据库结果：
   HIGH = 高风险
   MEDIUM = 中风险
   LOW = 低风险

6. 绝对不能：
   - 把 MEDIUM 说成 HIGH
   - 把 LOW 说成 MEDIUM
   - 根据概率自行改变 risk_level
   - 根据金额自行改变 risk_level

7. risk_reason 只有在数据库结果中存在时才能使用。
   不要自行创造风险原因。

8. 不要预测未来风险。

9. 不要在没有数据依据的情况下给出投资、
   金融或其他决策建议。

10. 回答应该简洁，适合直接显示在 FinGuard
    Dashboard 中。

11. 使用中文。

12. 如果数据不足以回答问题：
    直接说明“当前查询数据不足以回答该问题”。

13. 如果用户的问题超出了当前查询结果能够支持的范围：
    不要自行扩展推理。

14. 优先使用具体数据回答，而不是泛泛而谈。


=========================================================
回答格式
=========================================================

尽量按照以下结构回答：

查询结果：
- 关键数据 1
- 关键数据 2
- 关键数据 3

结论：
根据查询结果给出简洁的数据结论。

如果查询结果本身不适合使用该格式，
可以采用自然语言回答。
"""


    # =====================================================
    # 调用 ARK
    # =====================================================

    try:

        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "你是 FinGuard 银行实时风控数据分析助手。"
                        "你只能依据提供的 Doris 查询结果回答问题。"
                        "严禁编造数据库中不存在的信息。"
                    ),
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            temperature=0.1,
        )

        answer = response.choices[0].message.content

        if not answer:

            return "AI 未返回有效分析结果。"

        return answer.strip()


    except Exception as e:

        return (
            f"AI 服务调用失败：{e}"
        )


# =========================================================
# 命令行入口
# =========================================================

if __name__ == "__main__":

    print("=" * 60)
    print("FinGuard AI 风控分析助手")
    print("输入 exit 退出")
    print("=" * 60)

    while True:

        question = input(
            "\n请输入问题："
        ).strip()

        if question.lower() == "exit":
            break

        if not question:
            continue

        try:

            answer = ask_agent(
                question
            )

            print(
                "\n========== AI 分析 =========="
            )

            print(answer)

        except Exception as e:

            print(
                f"\nAgent 执行失败：{e}"
            )