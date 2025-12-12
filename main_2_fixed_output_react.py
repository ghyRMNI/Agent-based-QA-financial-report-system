import os
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


class FinancialDataParams(BaseModel):
    """
    用于从用户的自然语言请求中，严格提取出收集财报数据所需的参数。
    """
    stock_code: str = Field(description="股票代码，例如 '00700', '600519' 等。")
    start_year: int = Field(description="需要获取的财报起始年份，例如 2023。")
    end_year: int = Field(description="需要获取的财报结束年份，例如2025")

    @field_validator("stock_code")
    def validate_stock_code(cls, value):
        if len(value) != 6 or not value.isdigit():
            raise ValueError(f"股票代码 '{value}' 格式不正确，需要是6位数字")
        return value

    @field_validator("start_year", "end_year")
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
        "必须调用此工具，并严格填充 stock_code 和 start_year 和 end_year 字段。"
        "如果用户只给出一个年份，请将 start_year 和 end_year 的值设为相同"
        ""
    )
    args_schema: type[BaseModel] = FinancialDataParams # Tool 的输入 Schema 即 Pydantic 模型

    def _run(self, stock_code: str, start_year: int, end_year: int):
        """Tool 的实际执行逻辑，Agent 决定调用它时会运行这里。"""
        # 在这里我们不执行爬取，而是返回一个 JSON 格式的确认信息
        return f"已成功提取参数并确认：股票代码='{stock_code}', 起始年份='{start_year}', 结束年份='{end_year}'。准备执行数据收集..."

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")


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
tools: List[BaseTool] = [CollectFinancialDataTool()]


# 3. 定义工具列表
tools: List[BaseTool] = [CollectFinancialDataTool()]


# --- 3. Agent 核心组件定义 ---

# 1. 定义 Agent 的 Prompt
template = (
    "你是一位资深金融研究员，也是一位友善的聊天助手。"
    "你的任务是接收用户的请求，并进行以下判断："
    "1. 如果请求是闲聊，或者你已经得出了最终结论，请使用 Action: Final Answer 格式停止。"
    "2. 如果你需要调用工具，Action 必须是 collect_financial_data_pipeline。"
    "\n\n***重要提示：在 Action Input 中输出的 JSON 字符串，请不要使用 ```json 或 ``` 标签包裹。请直接输出纯净的 JSON 对象。***"

    # 修复点 A：添加工具名称和工具描述
    "\n\n你拥有的工具及其用途描述如下:\n{tools}"
    "\n\n你只能使用的工具名称是: {tool_names}"

    # 明确指示 ReAct 行为
    "\n\n请严格遵循以下思考-行动-观察的格式进行决策："
    "\nThought: [你的思考过程]"
    "\nAction: [调用的工具名称，例如 collect_financial_data_pipeline 或 Final Answer]"
    "\nAction Input: [工具所需的JSON参数 或 最终答案文本]"

    # 内存和思考历史占位符 (保持原样，让 AgentExecutor 自动注入)
    "\n\n--- 对话历史 ---\n{chat_history}"
    "\n\n--- 历史思考过程 ---\n{agent_scratchpad}"

    "\n\n--- 最新用户输入 ---"
    "\nHuman: {input}"
)

# 2. 构造 PromptTemplate
# 这是 ReAct Agent 期望的 PromptTemplate 类型
prompt = PromptTemplate.from_template(template)
# base_prompt = hub.pull("hwchase17/react-chat")
# prompt = ChatPromptTemplate.from_messages([
#     # 1. 系统提示 (包含工具描述 {tools} 和 {tool_names})
#     ("system", system_prompt),
#     # 2. 内存占位符 (多轮对话的关键)
#     MessagesPlaceholder(variable_name="chat_history"),
#     # 3. 用户当前输入
#     ("human", "{input}"),
#     # 4. Agent 历史思考过程 (修复 TypeError 的关键)
#     MessagesPlaceholder(variable_name="agent_scratchpad"),
# ])

# 2. 创建 Agent
agent = create_react_agent(
    llm=llm,
    tools=tools,
    prompt=prompt,
)

# 3. 创建 Agent Executor（执行器）
agent_executor = AgentExecutor(
    agent=agent,
    tools=tools,
    verbose=True, # 开启 verbose 可以看到 Agent 的思考过程
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

        # 调用 Agent Executor
        try:
            # Agent Executor 已经内置了内存处理
            result = agent_executor.invoke({"input": user_input})
            ai_response = result["output"]

        except Exception as e:
            ai_response = f"抱歉，处理您的请求时出现错误: {e}"
            print(f"Agent 错误: {e}")

        print(f"\nAI: {ai_response}")
        print("-" * 60)


if __name__ == "__main__":
    run_chat_agent()
