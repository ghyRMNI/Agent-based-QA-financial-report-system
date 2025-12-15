from typing import Optional, List
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator, PrivateAttr

from langchain.tools import BaseTool
from langchain.memory import ConversationBufferWindowMemory
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.prompts import PromptTemplate
from langchain.agents import AgentExecutor, create_react_agent
from langchain.agents.openai_functions_agent.base import create_openai_functions_agent
from langchain import hub


GLOBAL_LLM = None


# --- 1. Agent 2 相关的 Pydantic 模型 ---

class FileAnalysisParams(BaseModel):
    """用于从用户请求中，严格提取出分析文件所需的具体问题。"""
    user_query: str = Field(description="用户希望对固定报告提出的具体问题，例如 '2023年营收同比增速是多少?'。")

    @field_validator("user_query")
    def validate_user_query(cls, value):
        if not value:
            raise ValueError("分析问题不能为空")
        return value


# --- 2. Agent 2 核心 Tool 定义 ---
class FinancialReportAnalysisTool(BaseTool):
    """
    用于分析一个固定路径下的报告的工具。Agent 只需要提取用户的具体问题。
    """

    name: str = "report_analysis_tool"
    description: str = (
        "当用户要求分析报告（例如，询问关于公司财务数据的问题）时，"
        "必须调用此工具，并严格填充 user_query 字段。"
        "此工具会自动从一个**固定、预设的路径**读取报告内容，并根据用户问题进行分析。"
    )
    args_schema: type[BaseModel] = FileAnalysisParams

    FIXED_REPORT_PATH: str = "./unified_outputs/002216/financial_statements.csv"


    def _read_file_content(self) -> str:
        """从固定的、确定的路径读取文件内容。"""
        actual_path = self.FIXED_REPORT_PATH
        try:
            with open(actual_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            # 如果是固定的文件，如果找不到，说明是系统配置问题，返回错误
            return f"系统配置错误：找不到固定报告文件: {actual_path}"
        except Exception as e:
            return f"读取固定报告文件时出错: {e}"

    def _run(self, user_query: str):
        """Tool 的实际执行逻辑：读取固定文件，然后送给 LLM 分析。"""

        llm = globals().get("GLOBAL_LLM")
        # 1. Tool 内部读取固定文件内容
        report_content = self._read_file_content()

        if report_content.startswith("系统配置错误"):
            return report_content

        # 2. 构建最终分析 Prompt
        # 🌟 假设 llm 实例在 Tool 外部已初始化并传入（或者像原代码一样全局可用）
        # 这里需要确保 llm 实例是可用的
        # 为了简洁，我们假设llm是可用的

        analysis_prompt = (
            f"你是一位专业的财务分析师。请根据以下固定报告内容，"
            f"**简洁、准确地**回答用户提出的问题。\n\n"
            f"--- 报告内容 ---\n"
            f"{report_content}\n"
            f"-----------------\n\n"
            f"用户问题: {user_query}"
        )

        # 3. 调用 LLM 进行分析
        try:
            # 假设 llm 实例已初始化并可用
            analysis_result = llm.invoke(analysis_prompt).content
            return f"对固定报告的分析结果：\n{analysis_result}"
        except Exception as e:
            return f"报告分析失败，LLM调用错误: {e}"

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")


class TerminateTool(BaseTool):
    """用于终止Agent流程并返回最终答案的工具"""
    name: str = "终止"
    description: str = (
        "当工具返回结果已足够回答用户问题、或数据缺失无法继续分析、或用户请求是闲聊时，"
        "必须调用此工具终止流程，并将最终回复填充到Action Input中。"
    )
    # 终止工具无需参数，所以args_schema用空的BaseModel
    args_schema: type[BaseModel] = BaseModel

    def _run(self):
        """调用此工具时直接返回空（核心是触发终止逻辑，无需实际执行）"""
        return "流程终止"

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")


# --- 3. Agent 2 核心组件定义 ---

# 我们将复用 Agent 1 的 LLM 配置和内存，但定义一个新的 Agent/Executor

def create_analysis_agent(llm: ChatOpenAI, memory: ConversationBufferWindowMemory):
    """创建并返回文件分析 Agent Executor"""

    # 1. 定义工具列表 (Agent 2 只需要分析工具)
    analysis_tools: List[BaseTool] = [FinancialReportAnalysisTool(), TerminateTool()]

    # 2. 定义 Agent 的 Prompt
    # analysis_system_prompt = (
    #     "你是一位资深数据分析师。你的任务是根据爬取到的公司财报、新闻与相关股票信息分析请求问题，并进行以下判断："
    #     # "1. **如果**用户并没有执行完数据爬取agent，则不调用此agent。只有在数据爬取agent执行完后，相应的文件数据已经下载下来，才调用此agent分析"
    #     # "2. **如果**相关数据已经过数据爬取agent下载好，并且用户的请求是分析一个具体文件并提出问题，"
    #     # "你**必须**调用 `financial_report_analysis_tool` 工具，"
    #     "3. 如果用户提出了一个关于报告的问题，你**必须**调用 `report_analysis_tool` 工具，"
    #     "   并严格只将用户提出的**具体问题**填充到 `user_query` 字段中。"
    #     "4. 绝对不要尝试提取文件路径或文件名，因为文件路径是固定的，已经在工具内部设置。"
    #     "5. 在调用工具之前，请勿以自然语言形式回答关于文件内容的问题。"
    # )

    react_system_prompt = """
    你是一位资深金融研究员，负责准确回答用户的金融分析提问。

    ### 核心规则
    1. 只有当用户的问题需要分析固定报告数据时，才调用工具；
    2. 工具调用后，若得到明确结果（或明确数据缺失），直接整理成最终答案，无需重复调用工具；
    3. 闲聊、无需分析数据的问题，直接回复，不调用工具。

    ### 可用工具
    你仅能使用以下工具：
    {tools}
    工具名称列表：{tool_names}

    ### 格式要求（二选一）
    #### 情况1：需要调用工具时（必须按此格式）
    Thought: [分析用户问题，说明为何需要调用工具，如何构造输入]
    Action: [工具名称，必须是 {tool_names} 中的一个]
    Action Input: [工具所需参数，仅填写用户的具体问题（纯文本）]

    #### 情况2：无需调用工具/已得到结果时（必须按此格式终止）
    Thought: [说明无需调用工具的原因（如：工具已返回足够数据/用户问题是闲聊）]
    Final Answer: [给用户的最终回复（纯文本）]

    ### 对话上下文
    --- 对话历史 ---
    {chat_history}
    --- 历史思考过程 ---
    {agent_scratchpad}
    --- 最新用户输入 ---
    {input}
    """

    analysis_prompt = ChatPromptTemplate.from_messages([
        ("system", react_system_prompt),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])

    # 修正3：正确创建PromptTemplate并填充{tools}/{tool_names}占位符
    # 提取工具描述和名称，用于填充prompt
    tools_description = "\n".join([f"- {tool.name}：{tool.description}" for tool in analysis_tools])
    tool_names = ", ".join([tool.name for tool in analysis_tools])  # 结果：report_analysis_tool, 终止

    react_prompt = PromptTemplate(
        template=react_system_prompt,
        input_variables=["input", "chat_history", "agent_scratchpad"],
        partial_variables={  # 提前填充固定的工具信息
            "tools": tools_description,
            "tool_names": tool_names
        }
    )

    # 3. 创建 Agent
    analysis_agent = create_react_agent(
        llm=llm,
        tools=analysis_tools,
        prompt=react_prompt,
    )

    # 4. 创建 Agent Executor
    analysis_agent_executor = AgentExecutor(
        agent=analysis_agent,
        tools=analysis_tools,
        verbose=True,
        memory=memory,
        handle_parsing_errors="抱歉，我无法处理你的请求，请重新描述问题。",
        max_iterations=3,  # 最多3次循环（1次调用工具+1次返回结果，足够用）
        early_stopping_method="generate"  # 达到最大迭代时生成最终回复
    )

    return analysis_agent_executor


# --- 4. 运行和测试函数 (将 Agent 2 集成到运行流程) ---

def run_analysis_agent_test():
    """模拟文件分析 Agent 的运行。"""

    # 🌟 重新使用 Agent 1 中已初始化的 LLM 和 Memory
    load_dotenv()
    global GLOBAL_LLM
    llm = ChatOpenAI(
        model='deepseek-r1-250528',
        base_url='https://ark.cn-beijing.volces.com/api/v3',
        temperature=0.1,
    )
    GLOBAL_LLM = llm
    memory = ConversationBufferWindowMemory(
        memory_key="chat_history",
        k=5,
        return_messages=True
    )

    analysis_agent_executor = create_analysis_agent(llm, memory)

    print("--- 📄 文件分析 Agent 模拟启动 ---")
    print("输入 '退出' 或 'exit' 结束对话。")
    print("-" * 60)


    # 🌟 交互式循环
    while True:
        user_input = input("用户: ")
        if user_input.lower() in ["退出", "exit", "quit"]:
            print("对话结束。")
            break

        try:
            result = analysis_agent_executor.invoke({"input": user_input})
            ai_response = result["output"]
        except Exception as e:
            ai_response = f"抱歉，处理您的请求时出现错误: {e}"
            print(f"Agent 错误: {e}")

        print(f"AI: {ai_response}")
        print("-" * 60)


if __name__ == "__main__":
    # 运行 Agent 2 的测试
    run_analysis_agent_test()