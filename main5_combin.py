import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferWindowMemory
from langchain.agents import AgentExecutor, create_react_agent
from langchain.tools import BaseTool
from langchain.agents.openai_functions_agent.base import create_openai_functions_agent
from langchain_core.prompts import PromptTemplate

from main_pipeline import UnifiedDataCollector

# ============================================================================
# 全局变量
# ============================================================================
GLOBAL_LLM = None
GLOBAL_DATA_COLLECTION_AGENT = None
GLOBAL_ANALYSIS_AGENT = None
GLOBAL_MEMORY = None
ROOT_PATH = None

# ============================================================================
# Agent 1: 数据收集 Agent 相关定义
# ============================================================================

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
        if value > current_year:
            raise ValueError(f"年份 {value} 超出有效范围")
        if value < 1990:
            raise ValueError(f"年份 {value} 过早")
        return value


class CollectFinancialDataTool(BaseTool):
    """用于严格提取和确认用户请求中的公司股票代码和财报年份的工具。"""

    name: str = "collect_financial_data_pipeline"
    description: str = (
        "当用户明确请求获取某公司（提供股票代码）的特定年份（例如 2023 年）的财报数据时，"
        "必须调用此工具，并严格填充 stock_code 和 start_date 和 end_date 字段。"
        "如果用户只给出一个年份，请将 start_date 和 end_date 的值设为相同"
    )
    args_schema: type[BaseModel] = FinancialDataParams

    def _run(self, stock_code: str, start_date: int, end_date: int):
        """Tool 的实际执行逻辑，Agent 决定调用它时会运行这里。"""
        return f"已成功提取参数并确认：股票代码='{stock_code}', 起始年份='{start_date}', 结束年份='{end_date}'。准备执行数据收集..."

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")


class ExecuteFinancialDataTool(BaseTool):
    """
    当用户明确**确认**了股票代码和年份信息后，用于执行实际数据收集流程的工具。
    """

    name: str = "execute_financial_data_collection"
    description: str = (
        "只有当用户明确回复 '确认', '是的', '继续' 等表示同意的词语后，"
        "且 Agent 已经从对话历史中获得了 'stock_code', 'start_date', 'end_date' 三个参数时，"
        "必须调用此工具来执行数据收集的最终操作。"
    )
    args_schema: type[BaseModel] = FinancialDataParams

    def _run(self, stock_code: str, start_date: int, end_date: int):
        """Tool 的实际执行逻辑"""
        output = {
            "stock_code": stock_code,
            "start_date": start_date,
            "end_date": end_date,
        }
        return output

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")


# ============================================================================
# Agent 2: 数据分析 Agent 相关定义
# ============================================================================

class FileAnalysisParams(BaseModel):
    """用于从用户请求中，严格提取出分析文件所需的具体问题。"""
    user_query: str = Field(description="用户希望对固定报告提出的具体问题，例如 '2023年营收同比增速是多少?'。")

    @field_validator("user_query")
    def validate_user_query(cls, value):
        if not value:
            raise ValueError("分析问题不能为空")
        return value


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

    def _read_file_content(self) -> str:
        """从固定的、确定的路径读取文件内容。"""
        # 动态获取最新的ROOT_PATH
        root_path = globals().get("ROOT_PATH")
        if root_path is None:
            return "系统配置错误：ROOT_PATH未设置，请先执行数据收集。"
        
        actual_path = f"{root_path}/integrated_stock_news_data.csv"
        try:
            with open(actual_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            return f"系统配置错误：找不到固定报告文件: {actual_path}"
        except Exception as e:
            return f"读取固定报告文件时出错: {e}"

    def _run(self, user_query: str):
        """Tool 的实际执行逻辑：读取固定文件，然后送给 LLM 分析。"""
        llm = globals().get("GLOBAL_LLM")
        report_content = self._read_file_content()

        if report_content.startswith("系统配置错误"):
            return report_content

        analysis_prompt = (
            f"你是一位专业的财务分析师。请根据以下固定报告内容，"
            f"**简洁、准确地**回答用户提出的问题。\n\n"
            f"--- 报告内容 ---\n"
            f"{report_content}\n"
            f"-----------------\n\n"
            f"用户问题: {user_query}"
        )

        try:
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
    args_schema: type[BaseModel] = BaseModel

    def _run(self):
        """调用此工具时直接返回空（核心是触发终止逻辑，无需实际执行）"""
        return "流程终止"

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")


# ============================================================================
# 路由 Agent 工具定义
# ============================================================================

class RouteToCollectionParams(BaseModel):
    """路由到数据收集agent的参数"""
    user_input: str = Field(description="用户的原始输入，需要传递给数据收集agent")

    @field_validator("user_input")
    def validate_user_input(cls, value):
        if not value:
            raise ValueError("用户输入不能为空")
        return value


class RouteToAnalysisParams(BaseModel):
    """路由到数据分析agent的参数"""
    user_input: str = Field(description="用户的原始输入，需要传递给数据分析agent")

    @field_validator("user_input")
    def validate_user_input(cls, value):
        if not value:
            raise ValueError("用户输入不能为空")
        return value


class RouteToCollectionTool(BaseTool):
    """路由到数据收集agent的工具"""
    name: str = "route_to_data_collection"
    description: str = (
        "当用户请求收集、爬取、下载财报数据，或提供股票代码、公司名称和年份要求获取数据时，"
        "必须通过function calling调用此工具将请求路由到数据收集agent。"
        "参数user_input应该是用户的完整原始输入。"
        "例如：'帮我收集002216公司2023年的财报'、'爬取600519的2024年数据'、'我想爬安井食品2024年的财报'等。"
    )
    args_schema: type[BaseModel] = RouteToCollectionParams

    def _run(self, user_input: str):
        """调用数据收集agent处理请求"""
        global GLOBAL_DATA_COLLECTION_AGENT
        if GLOBAL_DATA_COLLECTION_AGENT is None:
            return "错误：数据收集agent未初始化"
        
        try:
            result = GLOBAL_DATA_COLLECTION_AGENT.invoke({"input": user_input})
            return result["output"]
        except Exception as e:
            return f"数据收集agent执行错误: {e}"

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")


class RouteToAnalysisTool(BaseTool):
    """路由到数据分析agent的工具"""
    name: str = "route_to_data_analysis"
    description: str = (
        "当用户请求分析财报、询问财务数据相关问题，或对已有数据提出问题时，"
        "必须通过function calling调用此工具将请求路由到数据分析agent。"
        "参数user_input应该是用户的完整原始输入。"
        "例如：'2023年营收是多少？'、'分析一下这个公司的财务状况'、'营收同比增速是多少？'等。"
    )
    args_schema: type[BaseModel] = RouteToAnalysisParams

    def _run(self, user_input: str):
        """调用数据分析agent处理请求"""
        global GLOBAL_ANALYSIS_AGENT
        if GLOBAL_ANALYSIS_AGENT is None:
            return "错误：数据分析agent未初始化"
        
        try:
            result = GLOBAL_ANALYSIS_AGENT.invoke({"input": user_input})
            return result["output"]
        except Exception as e:
            return f"数据分析agent执行错误: {e}"

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")


# ============================================================================
# 创建子Agent的函数
# ============================================================================

def create_data_collection_agent(llm: ChatOpenAI, memory: ConversationBufferWindowMemory) -> AgentExecutor:
    """创建数据收集Agent Executor"""
    collection_tools: List[BaseTool] = [
        CollectFinancialDataTool(),
        ExecuteFinancialDataTool()
    ]

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
        "\n\n"
        "### 重要：公司名称转股票代码规则\n"
        "**当用户提供公司名称而不是股票代码时，你必须根据你的知识自动查找并转换为对应的6位数字股票代码。**\n"
        "常见公司名称与股票代码对应关系示例：\n"
        "- 安井食品 → 603345\n"
        "- 三全食品 → 002216\n"
        "- 贵州茅台 → 600519\n"
        "- 腾讯控股 → 00700（港股）\n"
        "- 平安银行 → 000001\n"
        "**如果用户只提供了公司名称，你必须自动查找对应的股票代码，不要将stock_code设为null。**\n"
        "**只有在完全无法确定股票代码时，才将stock_code设为null。**\n"
        "\n请严格遵循工具调用格式，确保JSON键名和工具名称的准确性。"
    )

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])

    agent = create_openai_functions_agent(
        llm=llm,
        tools=collection_tools,
        prompt=prompt,
    )

    agent_executor = AgentExecutor(
        agent=agent,
        tools=collection_tools,
        verbose=True,
        memory=memory,
        handle_parsing_errors=True
    )

    return agent_executor


def create_analysis_agent(llm: ChatOpenAI, memory: ConversationBufferWindowMemory) -> AgentExecutor:
    """创建数据分析Agent Executor"""
    analysis_tools: List[BaseTool] = [
        FinancialReportAnalysisTool(),
        TerminateTool()
    ]

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

    tools_description = "\n".join([f"- {tool.name}：{tool.description}" for tool in analysis_tools])
    tool_names = ", ".join([tool.name for tool in analysis_tools])

    react_prompt = PromptTemplate(
        template=react_system_prompt,
        input_variables=["input", "chat_history", "agent_scratchpad"],
        partial_variables={
            "tools": tools_description,
            "tool_names": tool_names
        }
    )

    analysis_agent = create_react_agent(
        llm=llm,
        tools=analysis_tools,
        prompt=react_prompt,
    )

    analysis_agent_executor = AgentExecutor(
        agent=analysis_agent,
        tools=analysis_tools,
        verbose=True,
        memory=memory,
        handle_parsing_errors="抱歉，我无法处理你的请求，请重新描述问题。",
        max_iterations=3,
        early_stopping_method="generate"
    )

    return analysis_agent_executor


# ============================================================================
# 主路由 Agent
# ============================================================================

def create_router_agent(llm: ChatOpenAI, memory: ConversationBufferWindowMemory) -> AgentExecutor:
    """创建主路由Agent Executor - 只负责判断并输出JSON，不调用工具"""
    
    router_system_prompt = (
        "你是一位智能路由助手，负责根据用户的意图判断应该使用哪个专业agent。\n\n"
        "### 核心任务\n"
        "分析用户的请求和对话历史，输出一个JSON格式的决策，包含以下字段：\n"
        "- `tool`: 字符串，值为 `\"route_to_data_collection\"` 或 `\"route_to_data_analysis\"`\n"
        "- `user_input`: 字符串，用户的完整原始输入\n\n"
        "### 路由规则\n"
        "1. **数据收集agent** (`route_to_data_collection`)："
        "   - 用户请求收集、爬取、下载财报数据"
        "   - 用户提供股票代码、公司名称和年份要求获取数据"
        "   - **重要**：如果对话历史显示数据收集agent刚刚提取了参数并等待确认，"
        "     且用户回复'确认'、'是的'、'继续'等表示同意的词语，"
        "     必须输出 `{{\"tool\": \"route_to_data_collection\", \"user_input\": \"用户的输入\"}}`"
        "   - 例如：'帮我收集002216公司2023年的财报'、'爬取600519的2024年数据'、'我想爬安井食品2024年的财报'、'确认'\n"
        "2. **数据分析agent** (`route_to_data_analysis`)："
        "   - 用户请求分析财报、询问财务数据相关问题"
        "   - 用户对已有数据提出具体问题"
        "   - 例如：'2023年营收是多少？'、'分析一下这个公司的财务状况'、'营收同比增速是多少？'\n\n"
        "### 输出格式要求\n"
        "**你必须严格按照以下JSON格式输出，不要添加任何解释性文字：**\n"
        "```json\n"
        "{{\n"
        "  \"tool\": \"route_to_data_collection\",\n"
        "  \"user_input\": \"用户的完整原始输入\"\n"
        "}}\n"
        "```\n"
        "或者\n"
        "```json\n"
        "{{\n"
        "  \"tool\": \"route_to_data_analysis\",\n"
        "  \"user_input\": \"用户的完整原始输入\"\n"
        "}}\n"
        "```\n\n"
        "### 严格规则\n"
        "- **必须输出有效的JSON格式**，包含 tool 和 user_input 两个字段\n"
        "- **禁止**在JSON前后添加任何解释性文字\n"
        "- **禁止**用自然语言解释路由逻辑\n"
        "- 如果用户意图完全无法判断，输出：`{{\"tool\": null, \"user_input\": \"用户的输入\"}}`，然后用自然语言询问用户\n"
        "- 如果用户只是闲聊，直接回复自然语言，不要输出JSON\n"
        "- `user_input` 字段必须是用户的完整原始输入，不要修改\n"
    )

    router_prompt = ChatPromptTemplate.from_messages([
        ("system", router_system_prompt),
        MessagesPlaceholder(variable_name="chat_history"),
        ("human", "{input}"),
    ])

    # 路由Agent不需要工具，只是一个简单的LLM调用
    # 我们直接使用LLM，不创建Agent Executor
    # 但为了保持接口一致，我们创建一个简单的包装
    
    class SimpleRouterAgent:
        def __init__(self, llm, prompt, memory):
            self.llm = llm
            self.prompt = prompt
            self.memory = memory
        
        def invoke(self, inputs):
            try:
                # 获取对话历史（使用memory的load_memory_variables方法）
                memory_variables = self.memory.load_memory_variables({})
                chat_history = memory_variables.get("chat_history", [])
                
                # 构建消息
                messages = self.prompt.format_messages(
                    input=inputs["input"],
                    chat_history=chat_history
                )
                
                # 调用LLM
                response = self.llm.invoke(messages)
                output = response.content
                
                # 保存到memory
                self.memory.save_context({"input": inputs["input"]}, {"output": output})
                
                return {"output": output}
            except Exception as e:
                # 如果format_messages失败，可能是prompt中有未转义的变量
                # 尝试直接调用LLM，不使用prompt模板
                error_msg = f"路由Agent错误: {str(e)}"
                print(f"调试信息: {error_msg}")
                # 直接使用简单的prompt
                simple_prompt = f"根据用户输入判断应该使用哪个agent，输出JSON格式：{{\"tool\": \"route_to_data_collection\"或\"route_to_data_analysis\", \"user_input\": \"用户输入\"}}\n\n用户输入：{inputs['input']}"
                response = self.llm.invoke(simple_prompt)
                output = response.content
                self.memory.save_context({"input": inputs["input"]}, {"output": output})
                return {"output": output}
    
    router_agent = SimpleRouterAgent(llm, router_prompt, memory)
    
    return router_agent


# ============================================================================
# 主运行函数
# ============================================================================

def run_combined_agent():
    """运行整合后的agent系统"""
    global GLOBAL_LLM, GLOBAL_DATA_COLLECTION_AGENT, GLOBAL_ANALYSIS_AGENT, GLOBAL_MEMORY, ROOT_PATH

    # 加载环境变量
    load_dotenv()
    if not os.getenv("OPENAI_API_KEY"):
        raise ValueError("环境变量 OPENAI_API_KEY 未设置或加载失败。请检查 .env 文件。")

    # 初始化 LLM
    llm = ChatOpenAI(
        model='deepseek-r1-250528',
        base_url='https://ark.cn-beijing.volces.com/api/v3',
        temperature=0.1,
    )
    GLOBAL_LLM = llm

    # 初始化共享内存（所有agent使用同一个内存，确保上下文共享）
    shared_memory = ConversationBufferWindowMemory(
        memory_key="chat_history",
        k=10,  # 增加窗口大小以保存更多上下文
        return_messages=True
    )
    GLOBAL_MEMORY = shared_memory

    # 创建子agents（所有agent共享同一个memory）
    print("正在初始化agents...")
    GLOBAL_DATA_COLLECTION_AGENT = create_data_collection_agent(llm, shared_memory)
    GLOBAL_ANALYSIS_AGENT = create_analysis_agent(llm, shared_memory)
    router_agent = create_router_agent(llm, shared_memory)
    print("Agents初始化完成！\n")

    print("=" * 60)
    print("--- 🤖 整合Agent系统启动 ---")
    print("输入 '退出' 或 'exit' 结束对话。")
    print("=" * 60)

    pending_confirmation_data: Optional[dict] = None

    while True:
        user_input = input("\n你: ")
        if user_input.lower() in ["退出", "exit"]:
            print("对话结束。")
            break

        try:
            # 调用路由agent
            result = router_agent.invoke({"input": user_input})
            router_output = result["output"]

            # 解析路由Agent的JSON输出
            router_decision = None
            if router_output and isinstance(router_output, str):
                # 尝试从输出中提取JSON（可能包含markdown代码块）
                json_str = router_output.strip()
                # 移除可能的markdown代码块标记
                if json_str.startswith("```json"):
                    json_str = json_str[7:]  # 移除 ```json
                if json_str.startswith("```"):
                    json_str = json_str[3:]   # 移除 ```
                if json_str.endswith("```"):
                    json_str = json_str[:-3]  # 移除结尾的```
                json_str = json_str.strip()
                
                # 尝试解析JSON
                if json_str.startswith('{'):
                    try:
                        router_decision = json.loads(json_str)
                    except json.JSONDecodeError:
                        pass

            # 根据路由决策调用对应的子Agent
            if router_decision and router_decision.get("tool"):
                tool_name = router_decision.get("tool")
                user_input_for_sub_agent = router_decision.get("user_input", user_input)
                
                if tool_name == "route_to_data_collection":
                    # 调用数据收集Agent
                    sub_result = GLOBAL_DATA_COLLECTION_AGENT.invoke({"input": user_input_for_sub_agent})
                    sub_output = sub_result["output"]
                    
                    # 处理数据收集agent的特殊输出格式
                    if sub_output and isinstance(sub_output, str):
                        # 清理可能的markdown代码块标记
                        json_str = sub_output.strip()
                        if json_str.startswith("```json"):
                            json_str = json_str[7:]  # 移除 ```json
                        if json_str.startswith("```"):
                            json_str = json_str[3:]   # 移除 ```
                        if json_str.endswith("```"):
                            json_str = json_str[:-3]  # 移除结尾的```
                        json_str = json_str.strip()
                        
                        # 检查是否是参数确认的JSON格式
                        if json_str.startswith('{') and any(
                                key in json_str for key in ["stock_code", "start_date", "tool"]):
                            try:
                                data = json.loads(json_str)

                                if data.get('tool') == "collect_financial_data_pipeline":
                                    pending_confirmation_data = data
                                    formatted_json = json.dumps(data, indent=2, ensure_ascii=False)
                                    ai_response = (
                                        "我已成功提取您请求的参数，请确认：\n"
                                        f"{formatted_json}\n"
                                        "请回复 **'确认'** 或 **'否认'**。"
                                    )

                                elif data.get('tool') == "execute_financial_data_collection":
                                    pending_confirmation_data = data
                                    params = data.get("parameters", data)
                                    stock_code = params.get("stock_code", "").split(".")[0]
                                    start_date = params.get("start_date")
                                    end_date = params.get("end_date")
                                    
                                    print(f"\n开始执行数据收集：股票代码={stock_code}, 起始年份={start_date}, 结束年份={end_date}")
                                    
                                    collector = UnifiedDataCollector(
                                        company_name=stock_code,
                                        stock_code=stock_code,
                                        start_date=start_date,
                                        end_date=end_date,
                                        exchange_type=None,
                                    )
                                    ROOT_PATH = collector.run_all()

                                    ai_response = (
                                        f"已按以下信息爬取财报数据：\n"
                                        f"股票代码: {stock_code}\n"
                                        f"起始年份: {start_date}\n"
                                        f"结束年份: {end_date}\n"
                                        f"储存地址: {ROOT_PATH}\n"
                                        f"数据收集完成！现在可以询问任何关于此公司的信息。"
                                    )
                                else:
                                    ai_response = sub_output
                            except json.JSONDecodeError:
                                ai_response = sub_output
                        else:
                            ai_response = sub_output
                    else:
                        ai_response = sub_output
                        
                elif tool_name == "route_to_data_analysis":
                    # 调用数据分析Agent
                    sub_result = GLOBAL_ANALYSIS_AGENT.invoke({"input": user_input_for_sub_agent})
                    ai_response = sub_result["output"]
                    
                else:
                    # tool为null或其他情况，直接返回路由Agent的输出
                    ai_response = router_output
            else:
                # 路由Agent没有输出JSON或tool为null，直接返回路由Agent的输出
                ai_response = router_output

            # 清理确认状态
            if pending_confirmation_data and (user_input.lower() in ["确认", "否认", "确定", "不要"]):
                pending_confirmation_data = None

        except Exception as e:
            ai_response = f"抱歉，处理您的请求时出现错误: {e}"
            print(f"错误详情: {e}")

        print(f"\nAI: {ai_response}")
        print("-" * 60)


if __name__ == "__main__":
    run_combined_agent()

