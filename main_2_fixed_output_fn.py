import os
import json
from datetime import datetime

from dotenv import load_dotenv
from typing import Optional, List

from langchain_core.prompts import PromptTemplate
from pydantic import BaseModel, Field, field_validator

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferWindowMemory
from langchain.agents import AgentExecutor, create_react_agent
from langchain import hub
from langchain.tools import BaseTool
from langchain.agents.openai_functions_agent.base import create_openai_functions_agent
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder

from main_pipeline import UnifiedDataCollector



class FinancialDataParams(BaseModel):
    """
    用于从用户的自然语言请求中，严格提取出收集财报数据所需的参数。
    """
    stock_code: str = Field(description="股票代码，例如 '00700', '600519' 等。")
    start_date: int = Field(description="需要获取的财报起始年份，例如 2023。")
    end_date: int = Field(description="需要获取的财报结束年份，例如2025")

    @field_validator("stock_code")
    def validate_stock_code(cls, value):
        if len(value) != 6 or not value.isdigit():
            raise ValueError(f"股票代码 '{value}' 格式不正确，需要是6位数字")
        return value

    @field_validator("start_date", "end_date")
    def validate_year(cls, value):
        current_year = datetime.now().year
        # 如果用户输入年份大于当前年份或小于1990年，则报错
        if value > current_year:
            raise ValueError(f"年份 {value} 超出有效范围")
        if value < 1990:
            raise ValueError(f"年份 {value} 过早")
        return value

# 定义 Tool (工具)
# 我们并不需要真正执行爬取，只需定义这个“动作”让 LLM 知道它存在
class CollectFinancialDataTool(BaseTool):
    """用于严格提取和确认用户请求中的公司股票代码和财报年份的工具。"""

    name: str = "collect_financial_data_pipeline"
    description: str = (
        "当用户明确请求获取某公司（提供股票代码）的特定年份（例如 2023 年）的财报数据时，"
        "必须调用此工具，并严格填充 stock_code 和 start_date 和 end_date 字段。"
        "如果用户只给出一个年份，请将 start_date 和 end_date 的值设为相同"
        ""
    )
    args_schema: type[BaseModel] = FinancialDataParams # Tool 的输入 Schema 即 Pydantic 模型

    def _run(self, stock_code: str, start_date: int, end_date: int):
        """Tool 的实际执行逻辑，Agent 决定调用它时会运行这里。"""
        # 在这里我们不执行爬取，而是返回一个 JSON 格式的确认信息
        return f"已成功提取参数并确认：股票代码='{stock_code}', 起始年份='{start_date}', 结束年份='{end_date}'。准备执行数据收集..."

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")


class ExecuteFinancialDataTool(BaseTool):
    """
    当用户明确**确认**了股票代码和年份信息后，用于执行实际数据收集流程的工具。
    Agent 必须将 CollectFinancialDataTool 返回的参数传递给此工具。
    """

    name: str = "execute_financial_data_collection"
    description: str = (
        "只有当用户明确回复 '确认', '是的', '继续' 等表示同意的词语后，"
        "且 Agent 已经从对话历史中获得了 'stock_code', 'start_date', 'end_date' 三个参数时，"
        "必须调用此工具来执行数据收集的最终操作。"
    )
    args_schema: type[BaseModel] = FinancialDataParams

    def _run(self, stock_code: str, start_date: int, end_date: int):
        """Tool 的实际执行逻辑，即您之前放在循环中的 `execute_data_collection` 函数。"""
        # 🌟 实际执行逻辑在这里！

        output = {
            "stock_code": stock_code,
            "start_date": start_date,
            "end_date": end_date,
        }
        return output


    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")



# --- 3. Agent 核心组件定义 (Final Agent Fix) ---

# 1. 定义 Agent 的 Prompt (使用 ChatPromptTemplate, 更适合 ChatModel)
system_prompt = (
    "你是一位资深金融研究员，专门负责财报数据收集。"
    "你的任务是接收用户的请求，并进行以下判断："
    "1. **如果**用户的请求是闲聊或不涉及数据收集，请以自然语言回复。"
    "2. **如果**你需要调用工具，你**必须**使用 collect_financial_data_pipeline 工具，"
    "   并且**严格使用**以下 JSON 键名来填充参数：'stock_code', 'start_date', 'end_date'，并放入parameters参数中"
    "   并等待用户回复 '确认' 或 '否认'。"
    "3. **执行阶段**："
    "   - **如果用户回复 '确认' 或同意的词语**，你必须立即使用对话历史中已有的参数，调用 `execute_financial_data_collection` 工具来执行最终任务。"
    "   - **绝对禁止在没有调用 `execute_financial_data_collection` 工具并获得结果之前，臆造或生成任何形式的『执行报告』或『数据抓取已启动』的自然语言回复。你必须通过工具调用来完成这一步骤。**"
    "   - **如果用户回复 '否认' 或拒绝的词语**，你必须回复自然语言，要求用户重新输入完整准确的信息。"
    "4. **在调用工具之前，请勿以自然语言形式回复收集财报数据相关的问题。**"
    "\n\n请严格遵循工具调用格式，确保JSON键名和工具名称的准确性。"
)

prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    MessagesPlaceholder(variable_name="chat_history"),
    ("human", "{input}"),
    # 🌟 关键：这个占位符在 functions agent 中用于传递历史 Function Call 消息
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

# --- 2. 配置初始化 ---

load_dotenv()
if not os.getenv("OPENAI_API_KEY"):
    raise ValueError("环境变量 OPENAI_API_KEY 未设置或加载失败。请检查 .env 文件。")

# 1. 初始化 LLM
# 为了更好地支持 Function Calling，将 temperature 设低一些
llm = ChatOpenAI(
    model='deepseek-r1-250528',
    base_url='https://ark.cn-beijing.volces.com/api/v3',
    temperature=0.1,
)

# 2. 初始化内存
memory = ConversationBufferWindowMemory(
    memory_key="chat_history",
    k=5, # 扩大窗口以更好地维持 Agent 流程
    return_messages=True
)


# 3. 定义工具列表
tools: List[BaseTool] = [
    CollectFinancialDataTool(),
    ExecuteFinancialDataTool() # 🌟 新增的执行工具
]

# 2. 创建 Agent
# 🌟 关键修正：重新切换到 create_openai_functions_agent
# 它是为 Chat 模型设计的，提供了最高的稳定性
agent = create_openai_functions_agent(
    llm=llm,
    tools=tools,
    prompt=prompt,
)

# 3. 创建 Agent Executor（执行器）保持不变
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True,
    memory=memory,
    handle_parsing_errors=True
)


# --- 4. 运行和测试函数 ---

def run_chat_agent():
    """模拟多轮对话的 Agent 函数。"""
    print("--- 🔬 Agent 多轮对话/Function Calling 模拟启动 ---")
    print("输入 '退出' 或 'exit' 结束对话。")
    print("-" * 60)

    while True:
        user_input = input("你: ")
        if user_input.lower() in ["退出", "exit"]:
            print("对话结束。")
            break

        pending_confirmation_data: Optional[dict] = None

        try:
            # 调用 Agent Executor
            result = agent_executor.invoke({"input": user_input})
            agent_output = result["output"]

            # --- 步骤 1 检查: 是否是 Tool 1 返回的参数 JSON? ---
            # 检查 Agent 的输出是否是 Tool Call 返回的 JSON 字符串 (通常 Agent 会返回 Tool 的结果)
            if agent_output.strip().startswith('{') and any(
                    key in agent_output for key in ["stock_code", "start_date"]):
                try:
                    data = json.loads(agent_output)

                    if data['tool'] == "collect_financial_data_pipeline":
                        # 捕获待确认数据
                        pending_confirmation_data = data

                        # 构造固定格式的回复
                        formatted_json = json.dumps(data, indent=2, ensure_ascii=False)
                        ai_response = (
                            "我已成功提取您请求的参数，请确认：\n"
                            f"{formatted_json}\n"
                            "请回复 **'确认'** 或 **'否认'**。"
                        )

                    elif data['tool'] == "execute_financial_data_collection":
                        pending_confirmation_data = data
                        data = data["parameters"]
                        data["stock_code"] = data["stock_code"].split(".")[0]
                        data["exchange_type"] = None
                        data["company_name"] = data["stock_code"]
                        print(data)
                        collector = UnifiedDataCollector(
                            company_name=data["stock_code"],
                            stock_code=data["stock_code"],
                            start_date=data["start_date"],
                            end_date=data["end_date"],
                            exchange_type=data["exchange_type"],
                        )
                        collector.run_all()

                        ai_response = (
                            "已按以下信息爬取财报数据：\n"
                            f"{data}\n"
                            "现在请询问任何关于此公司的信息"
                        )

                    else:
                        ai_response = agent_output
                except json.JSONDecodeError:
                    # 不是 JSON，按 Agent 的普通回复处理
                    ai_response = agent_output

            # --- 步骤 2 检查: Agent 内部自己处理了确认/执行或闲聊 ---
            else:
                # 可能是自然语言回复（闲聊、否认后的重输要求、或 Tool 2 成功执行后的返回结果）
                ai_response = agent_output
                # 如果 Agent 成功执行了 Tool 2，或者用户回复了否认，我们可以清空状态
                if pending_confirmation_data and (user_input.lower() in ["确认", "否认", "确定", "不要"]):
                    pending_confirmation_data = None


        except Exception as e:
            ai_response = f"抱歉，处理您的请求时出现错误: {e}"
            print(f"Agent 错误: {e}")

        print(f"\nAI: {ai_response}")
        print("-" * 60)


if __name__ == "__main__":
    run_chat_agent()
