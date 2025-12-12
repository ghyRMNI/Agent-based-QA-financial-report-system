import os
from dotenv import load_dotenv
from typing import Optional
from langchain_core.tools import tool # 定义工具
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.agents import AgentExecutor, create_react_agent # 核心：ReAct Agent
from langchain.memory import ConversationBufferWindowMemory
from langchain import hub # ReAct Agent 默认需要一个 Prompt

# --- 0. 环境变量配置 (确保您已设置 ARK_API_KEY) ---
load_dotenv()
# --- 1. 定义两个工具 ---

@tool
def extract_and_confirm_params(company_name: str, start_year: int) -> str:
    """
    负责从用户的请求中提取出公司名称和开始年份，并请求用户确认。
    输入参数：
      - company_name: 必须提供的公司名称或代码。
      - start_year: 必须提供的财报收集开始年份（YYYY）。

    返回：返回提取的参数并请求用户回复'确认'。
    """
    # 这个工具不执行任何昂贵的操作，只是将参数回显给用户
    return (
        f"提取参数成功：公司={company_name}, 年份={start_year}。\n"
        f"请回复 '确认' 以启动执行函数，或修正参数。"
    )


@tool
def execute_pipeline(company_name: str, year: int) -> str:
    """
    在参数被确认后，执行数据收集管道的实际操作。
    输入参数必须是上一步确认的 company_name 和 year。
    """
    # 模拟您的昂贵操作
    print("=" * 50)
    print(f"✅ 执行成功！已确认参数：公司={company_name}, 年份={year}")
    print("=" * 50)
    return f"执行成功！数据收集已启动，目标：{company_name}，年份：{year}。"


financial_tools = [extract_and_confirm_params, execute_pipeline]

# --- 3. 初始化 LLM 和记忆 ---
llm = ChatOpenAI(
    model='deepseek-r1-250528',
    base_url='https://ark.cn-beijing.volces.com/api/v3',
    temperature=0.2,
)

memory = ConversationBufferWindowMemory(
    memory_key="chat_history",
    k=5,
    return_messages=True
)

# --- 4. 定义 Prompt (关键：指导 Agent 行为) ---
system_prompt = (
    "你是一位金融数据专家。你的任务是确保执行参数的准确性。"
    "1. **初始请求：** 如果用户请求收集数据，你必须先调用 `extract_and_confirm_params` 工具来提取参数，并等待用户确认。"
    "2. **用户确认：** 如果用户回复了 '确认'，并且你已经在历史记录中看到了上一步提取的准确参数，你必须调用 `execute_pipeline` 工具来执行操作。"
    "3. **其他情况：** 如果参数提取失败或用户输入不明确，请要求用户修正。"
)

prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    MessagesPlaceholder(variable_name="chat_history"),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

# --- 5. 创建 AgentExecutor ---
agent = create_openai_fn_agent(llm=llm, tools=financial_tools, prompt=prompt)

agent_executor = AgentExecutor(
    agent=agent,
    tools=financial_tools,
    memory=memory,
    verbose=True, # 打印思考过程
    handle_parsing_errors=True
)


# --- 6. 运行演示 ---

def chat_with_agent(user_input):
    print(f"\n==========================================")
    print(f"User Input: {user_input}")
    print(f"==========================================")

    # 运行 Agent，它会根据 memory 和 tools 自主决策
    result = agent_executor.invoke({"input": user_input})

    print(f"\nAgent Final Response: {result['output']}")
    return result


# --- 轮次 1: 提取参数（LLM 调用 Tool 1）---
print("--- 轮次 1: 提取参数并请求确认 ---")
chat_with_agent("请帮我收集阿里巴巴2023年的财报数据。")

# --- 轮次 2: 用户确认（LLM 根据记忆调用 Tool 2）---
print("\n--- 轮次 2: 用户确认，启动执行 ---")
chat_with_agent("确认")

# --- 轮次 3: 新的请求（LLM 会重新开始流程）---
print("\n--- 轮次 3: 尝试新的请求 ---")
chat_with_agent("现在帮我收集腾讯控股2024年的数据")