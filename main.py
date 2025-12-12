import os
from dotenv import load_dotenv
from fastapi import FastAPI
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferWindowMemory
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from pydantic_schema import FinancialDataParams


load_dotenv()

if not os.getenv("OPENAI_API_KEY"):
    raise ValueError("环境变量 OPENAI_API_KEY 未设置或加载失败。请检查 .env 文件。")


app = FastAPI(
    title="NLP course project",
    description="Financial report acquisition and analysis",
    version="1.0",
)

llm = ChatOpenAI(
    model='deepseek-r1-250528',
    base_url='https://ark.cn-beijing.volces.com/api/v3',
    temperature=0.2,
)
structured_llm = llm.with_structured_output(FinancialDataParams)

memory = ConversationBufferWindowMemory(
    memory_key="chat_history",
    k=3,
    return_messages=True
)

# system_prompt = (
#     "你是一位资深金融研究员，专门负责财报数据收集。"
#     "你的任务是接收用户的请求，并进行以下操作："
#     "1. 如果请求是第一次出现，请先以自然语言回复，确认公司股票代码和需要获取的财报年份信息，并等待用户回复 '确认'。"
#     "2. 如果用户回复了 '确认'，或者请求非常明确，则调用 collect_financial_data_pipeline 工具来执行数据收集。"
#     "3. 如果用户回复了其他内容，则继续对话，直到获得明确的指令或确认。"
# )
#
# # 1. 定义 Prompt 模板（包含 SystemMessage 和 HumanMessage）
# prompt = ChatPromptTemplate.from_messages([
#     ("system", system_prompt),
#     # 加入历史消息占位符
#     MessagesPlaceholder(variable_name="chat_history"),
#     ("human", "{question}"),
#     ("placeholder", "agent_scratchpad")  # ?
# ])

confirmation_prompt = ChatPromptTemplate.from_messages([
    ("system", (
        "你是人类的聊天好朋友，简短的回复用户消息"
    )),
    MessagesPlaceholder(variable_name="chat_history"), # 确保能看到历史消息
    ("human", "{input}"),
])

chain = confirmation_prompt | llm | StrOutputParser()

def run_chat():
    """
    模拟多轮对话的函数。
    """
    print("--- AI 多轮对话模拟启动 ---")
    print("输入 '退出' 或 'exit' 结束对话。")
    print("-" * 30)

    while True:
        # 获取用户输入
        user_input = input("你: ")
        if user_input.lower() in ["退出", "exit"]:
            print("对话结束。")
            break

        # 1. 从内存中获取历史消息
        history = memory.load_memory_variables({})["chat_history"]

        # 2. 准备 Chain 的输入
        chain_input = {
            "input": user_input,
            "chat_history": history
        }

        # 3. 调用 Chain
        try:
            ai_response = chain.invoke(chain_input)
        except Exception as e:
            ai_response = f"抱歉，调用 LLM 时发生错误: {e}"
            print(f"AI: {ai_response}")
            continue

        print(f"AI: {ai_response}")

        # 4. 将新的对话回合保存到内存中
        memory.save_context(
            {"input": user_input},
            {"output": ai_response}
        )

        # (可选) 打印当前内存中的消息，方便调试
        # print("\n[当前内存状态]")
        # print(memory.load_memory_variables({}))
        # print("[/当前内存状态]\n")


# 运行聊天模拟
if __name__ == "__main__":
    run_chat()


