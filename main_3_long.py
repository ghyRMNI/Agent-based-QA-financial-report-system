import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from typing import Optional, List, Dict, Any, ClassVar

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

# 添加项目路径以导入 main_pipeline 模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


class FinancialDataParams(BaseModel):
    """
    用于从用户的自然语言请求中，严格提取出收集财报数据所需的参数。
    """
    stock_code: str = Field(
        description="股票代码或公司名称。如果是股票代码，例如 '000001', '00700', '600519' 等；如果是公司名称，例如 '平安银行', '腾讯控股' 等。系统会自动识别并查找对应的股票代码。")
    start_year: int = Field(description="需要获取的财报起始年份，例如 2023。")
    end_year: int = Field(description="需要获取的财报结束年份，例如2025")

    @field_validator("stock_code")
    def validate_stock_code(cls, value):
        # 允许股票代码或公司名称，不进行严格验证
        # 如果是纯数字且长度合理，认为是股票代码；否则认为是公司名称
        if value and isinstance(value, str):
            return value.strip()
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
    """用于收集公司财报数据的工具，会自动执行完整的数据收集流程。"""

    name: str = "collect_financial_data_pipeline"
    description: str = (
        "当用户明确请求获取某公司的特定年份（例如 2023 年）的财报数据时，"
        "必须调用此工具。stock_code 字段可以填写股票代码（如 '000001', '00700'）或公司名称（如 '平安银行', '腾讯控股'），"
        "系统会自动识别并查找对应的股票代码。"
        "必须严格填充 stock_code 和 start_year 和 end_year 字段。"
        "如果用户只给出一个年份，请将 start_year 和 end_year 的值设为相同。"
        "此工具会自动执行完整的数据收集流程，包括股票数据、新闻、公告和PDF提取。"
    )
    args_schema: type[BaseModel] = FinancialDataParams  # Tool 的输入 Schema 即 Pydantic 模型

    # 类变量用于保存最后一次执行的结果
    last_output_dir: ClassVar[Optional[str]] = None
    last_collector: ClassVar[Optional[Any]] = None
    last_merged_file: ClassVar[Optional[str]] = None

    def _run(self, stock_code: str, start_year: int, end_year: int):
        """Tool 的实际执行逻辑，调用 main_pipeline.py 执行数据收集"""
        try:
            # 导入 main_pipeline 模块
            from main_pipeline import data_collection

            # 验证并准备参数（使用之前定义的函数）
            validation_result = validate_and_prepare_params(stock_code, start_year, end_year)

            if "error" in validation_result:
                return f"参数验证失败：{validation_result['error']}"

            # 不在这里输出，让main_pipeline统一输出

            # 调用 main_pipeline 的 data_collection 函数，传入验证后的参数
            collector = data_collection(input_data={
                'company_name': validation_result['company_name'],
                'stock_code': validation_result['stock_code'],
                'start_date': validation_result['start_date'],
                'end_date': validation_result['end_date'],
                'exchange_type': validation_result.get('exchange_type')
            })

            if collector is None:
                return "数据收集失败或已取消"

            # 获取输出目录
            output_dir = collector.output_manager.get_root_dir()

            # 返回成功消息和输出目录信息
            output = (
                f"\n{'=' * 60}\n"
                f"数据收集任务已完成！\n"
                f"{'=' * 60}\n"
                f"股票代码：{validation_result['stock_code']}\n"
                f"公司名称：{validation_result['company_name']}\n"
                f"年份范围：{start_year} 至 {end_year}\n"
                f"输出目录：{output_dir}\n"
                f"{'=' * 60}\n"
                f"\n数据已准备就绪，可以进行后续分析。"
            )

            # 保存输出目录到类变量，供后续使用
            CollectFinancialDataTool.last_output_dir = output_dir
            CollectFinancialDataTool.last_collector = collector

            # 获取合并后的数据文件路径（如果存在）
            merged_file = None
            if collector and hasattr(collector, 'results') and 'merged_data' in collector.results:
                if collector.results['merged_data'].get('success'):
                    merged_file = collector.results['merged_data'].get('file')
            CollectFinancialDataTool.last_merged_file = merged_file

            return output

        except Exception as e:
            import traceback
            error_msg = f"数据收集过程中发生错误：{str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            return error_msg

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
        "且 Agent 已经从对话历史中获得了 'stock_code', 'start_year', 'end_year' 三个参数时，"
        "必须调用此工具来执行数据收集的最终操作。"
        "**重要：执行此工具后，必须直接返回工具的执行结果，不要添加任何额外的文字说明或服务提示。**"
    )
    args_schema: type[BaseModel] = FinancialDataParams

    def _run(self, stock_code: str, start_year: int, end_year: int):
        """Tool 的实际执行逻辑，调用 main_pipeline.py 执行数据收集"""
        try:
            # 导入 main_pipeline 模块
            from main_pipeline import data_collection

            # 验证并准备参数（使用之前定义的函数）
            validation_result = validate_and_prepare_params(stock_code, start_year, end_year)

            if "error" in validation_result:
                return f"参数验证失败：{validation_result['error']}"

            # 调用 main_pipeline 的 data_collection 函数，传入验证后的参数
            collector = data_collection(input_data={
                'company_name': validation_result['company_name'],
                'stock_code': validation_result['stock_code'],
                'start_date': validation_result['start_date'],
                'end_date': validation_result['end_date'],
                'exchange_type': validation_result.get('exchange_type')
            })

            if collector is None:
                return "数据收集失败或已取消"

            # 获取输出目录
            output_dir = collector.output_manager.get_root_dir()

            # 返回成功消息和输出目录信息
            output = (
                f"数据收集任务已完成！\n"
                f"股票代码：{validation_result['stock_code']}\n"
                f"公司名称：{validation_result['company_name']}\n"
                f"年份范围：{start_year} 至 {end_year}\n"
                f"输出目录：{output_dir}\n"
                f"\n数据已准备就绪，可以进行后续分析。"
            )

            # 保存输出目录到全局变量，供后续使用
            ExecuteFinancialDataTool.last_output_dir = output_dir
            ExecuteFinancialDataTool.last_collector = collector

            return output

        except Exception as e:
            import traceback
            error_msg = f"数据收集过程中发生错误：{str(e)}\n{traceback.format_exc()}"
            print(error_msg)
            return error_msg

    def _arun(self, *args, **kwargs):
        raise NotImplementedError("Async run not implemented")

    # 类变量用于保存最后一次执行的结果（使用 ClassVar 避免 Pydantic 将其识别为模型字段）
    last_output_dir: ClassVar[Optional[str]] = None
    last_collector: ClassVar[Optional[Any]] = None


# --- 3. Agent 核心组件定义 (Final Agent Fix) ---

# 1. 定义 Agent 的 Prompt (使用 ChatPromptTemplate, 更适合 ChatModel)
system_prompt = (
    "你是一位资深金融研究员，专门负责财报数据收集和执行。"
    "你的任务是接收用户的请求，并进行以下操作："
    "\n"
    "1. **闲聊场景**：如果用户的请求是闲聊或不涉及数据收集，请以自然语言回复。"
    "\n"
    "2. **数据收集场景**（关键流程）："
    "   - 当用户请求收集财报数据时（例如：'给我平安银行2020年的财报'），你必须立即执行以下两步操作："
    "   - **第一步**：调用 `collect_financial_data_pipeline` 工具提取参数（stock_code, start_year, end_year）"
    "   - **第二步**：收到工具返回的确认信息后，**无需等待用户确认**，立即使用相同的参数调用 `execute_financial_data_collection` 工具来执行实际的数据收集"
    "   - **重要**：这两步必须连续执行，不要等待用户输入，不要询问用户是否确认"
    "\n"
    "3. **参数格式**："
    "   - stock_code: 可以是股票代码（如 '000001', '00700'）或公司名称（如 '平安银行', '腾讯控股'）"
    "   - start_year: 起始年份（整数）"
    "   - end_year: 结束年份（整数），如果用户只提供一个年份，start_year 和 end_year 设为相同值"
    "\n"
    "4. **执行规则**："
    "   - 必须使用函数调用方式，不要返回文本格式的工具调用"
    "   - 执行完数据收集后，直接返回工具的执行结果，不要添加额外的说明文字"
    "   - 如果工具执行失败，向用户说明错误原因并请求重新输入"
    "\n"
    "请严格遵循上述流程，确保用户请求能够一次性完成数据收集任务。"
)

prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    MessagesPlaceholder(variable_name="chat_history"),
    ("human", "{input}"),
    # 关键：这个占位符在 functions agent 中用于传递历史 Function Call 消息
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
    k=5,  # 扩大窗口以更好地维持 Agent 流程
    return_messages=True
)

# 3. 定义工具列表
tools: List[BaseTool] = [
    CollectFinancialDataTool()
]

# 2. 创建 Agent
# 关键修正：重新切换到 create_openai_functions_agent
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
    verbose=False,  # 关闭 verbose 避免重复输出
    memory=memory,
    handle_parsing_errors=True
)


# --- 4. 参数验证和数据准备函数 ---

def validate_and_prepare_params(stock_code: str, start_year: int, end_year: int) -> Dict[str, Any]:
    """
    验证参数并准备数据收集所需的参数（类似 interactive_input 的逻辑）

    Args:
        stock_code: 股票代码或公司名称
        start_year: 起始年份
        end_year: 结束年份

    Returns:
        Dict[str, Any]:
            - 如果成功: 返回包含 'company_name', 'stock_code', 'start_date', 'end_date', 'year', 'exchange_type' 的字典
            - 如果失败: 返回包含 'error' 键的字典，值为错误信息
    """
    try:
        # 导入 main_pipeline 中的函数
        from main_pipeline import check_duplicate_stock_code
        import sys
        import os

        # 验证年份范围
        current_year = datetime.now().year
        if start_year < 2000 or start_year > current_year + 1:
            return {
                'error': f"起始年份 {start_year} 超出合理范围 (2000-{current_year + 1})"
            }
        if end_year < 2000 or end_year > current_year + 1:
            return {
                'error': f"结束年份 {end_year} 超出合理范围 (2000-{current_year + 1})"
            }
        if start_year > end_year:
            return {
                'error': f"起始年份 {start_year} 不能大于结束年份 {end_year}"
            }

        # 处理股票代码或公司名称
        company_name = stock_code  # 默认使用输入值
        exchange_type = None
        actual_stock_code = None

        # 判断输入是股票代码还是公司名称
        # 如果全是数字且长度合理，认为是股票代码
        if stock_code.isdigit():
            # 是股票代码
            actual_stock_code = stock_code

            # 如果代码是5位或以下，检查是否在两个交易所都存在
            if len(stock_code) <= 5:
                matches = check_duplicate_stock_code(stock_code)

                if len(matches) > 1:
                    # 有重复，默认选择第一个（在 Agent 场景中，无法交互选择）
                    selected = matches[0]
                    actual_stock_code = selected['code']
                    company_name = selected['company_name']
                    exchange_type = selected['exchange']
                elif len(matches) == 1:
                    # 只有一个匹配，直接使用
                    actual_stock_code = matches[0]['code']
                    company_name = matches[0]['company_name']
                    exchange_type = matches[0]['exchange']
        else:
            # 可能是公司名称，尝试查找股票代码
            try:
                sys.path.append(os.path.join(os.path.dirname(__file__), 'announcement_crawler'))
                from announcement_crawler.crawler_start import find_stock_info

                stock_info = find_stock_info(company_name=stock_code)
                if stock_info:
                    actual_stock_code = stock_info['code']
                    company_name = stock_info.get('company_name', stock_code)
                    exchange_type = stock_info.get('exchange')
                    # 静默处理，不输出
                else:
                    # 如果找不到，尝试作为股票代码处理（可能是带后缀的格式，如 "000001.SZ"）
                    code_part = stock_code.split('.')[0] if '.' in stock_code else stock_code
                    if code_part.isdigit():
                        actual_stock_code = code_part
                    else:
                        return {
                            'error': f"无法找到公司 '{stock_code}' 的股票代码，请检查公司名称是否正确"
                        }
            except Exception as e:
                return {
                    'error': f"查找股票代码时出错: {str(e)}"
                }

        # 如果没有找到股票代码，使用原输入
        if not actual_stock_code:
            actual_stock_code = stock_code

        # 计算日期范围
        if start_year == end_year:
            target_year = start_year
            start_date = f"{start_year}-01-01"
            end_date = f"{end_year}-12-31"
        else:
            target_year = f"{start_year}-{end_year}"
            start_date = f"{start_year}-01-01"
            end_date = f"{end_year}-12-31"

        # 返回成功的结果
        return {
            'company_name': company_name,
            'stock_code': actual_stock_code if actual_stock_code else stock_code,
            'start_date': start_date,
            'end_date': end_date,
            'year': target_year,
            'exchange_type': exchange_type
        }

    except Exception as e:
        return {
            'error': f"参数验证失败: {str(e)}"
        }


# --- 5. 运行和测试函数 ---

def run_chat_agent():
    """模拟多轮对话的 Agent 函数。"""
    print("--- Agent 多轮对话/Function Calling 模拟启动 ---")
    print("输入 '退出' 或 'exit' 结束对话。")
    print("-" * 60)

    # 初始化状态变量
    run_chat_agent.data_ready = False
    run_chat_agent.output_dir = None
    run_chat_agent.should_switch_to_react = False
    run_chat_agent.react_output_dir = None
    react_agent_executor = None

    while True:
        user_input = input("你: ")
        if user_input.lower() in ["退出", "exit"]:
            print("对话结束。")
            break

        pending_confirmation_data: Optional[dict] = None

        try:
            # ========== 新增：直接从用户输入提取参数并执行 ==========
            # 检查用户输入是否包含数据收集请求
            import json
            import re

            tool_executed = False

            # 检测是否是数据收集请求的关键词
            data_collection_keywords = ['财报', '年报', '数据', '收集', '报告']
            is_data_request = any(keyword in user_input for keyword in data_collection_keywords)

            if is_data_request:
                # 尝试从用户输入中直接提取参数
                # 提取年份（4位数字）
                year_match = re.search(r'(\d{4})', user_input)
                if year_match:
                    start_year = int(year_match.group(1))
                    end_year = start_year

                    # 提取公司名（移除年份和其他关键词）
                    # 先移除年份
                    cleaned = re.sub(r'\d{4}年?', '', user_input)
                    # 移除常见的前缀和后缀词
                    cleaned = re.sub(r'^(给我|帮我|查询|收集|要|需要|请|请帮我|请给我)\s*', '', cleaned)
                    cleaned = re.sub(r'\s*(的|年报|年报分析|财报|财报分析|数据|报告|分析|收集|查询)\s*$', '', cleaned)
                    # 移除中间的"的"字
                    cleaned = re.sub(r'\s*的\s*', '', cleaned)
                    stock_code = cleaned.strip()

                    if stock_code and start_year:
                        # 不在这里输出，让main_pipeline统一输出

                        # 直接执行工具
                        tool_result = CollectFinancialDataTool()._run(stock_code, start_year, end_year)
                        ai_response = tool_result
                        tool_executed = True

            # 如果没有直接执行工具，则调用 Agent
            if not tool_executed:
                # 调用 Agent Executor
                result = agent_executor.invoke({"input": user_input})
                agent_output = result["output"]
            else:
                # 工具已执行，跳过 Agent 调用
                agent_output = ai_response

            # 1. 检查是否包含 <function> 标签格式（DeepSeek R1 的特殊格式）
            if not tool_executed and isinstance(agent_output, str) and "<function>" in agent_output:
                # 提取函数名和参数
                # 格式: <function>function_name\n```json\n{params}\n```
                function_match = re.search(r'<function>(\w+)\s*```(?:json)?\s*(\{[^`]+\})\s*```', agent_output,
                                           re.DOTALL)
                if function_match:
                    tool_name = function_match.group(1)
                    json_str = function_match.group(2).strip()

                    try:
                        arguments = json.loads(json_str)

                        print(f"\n{'=' * 60}")
                        print(f"[检测到工具调用：{tool_name}]")
                        print(f"[参数：{json.dumps(arguments, ensure_ascii=False)}]")
                        print(f"{'=' * 60}\n")

                        # 根据工具名称执行相应的工具
                        if tool_name == "collect_financial_data_pipeline":
                            # 检查参数格式
                            stock_code = arguments.get("stock_code")
                            start_year = arguments.get("start_year")
                            end_year = arguments.get("end_year", start_year)

                            # 如果参数格式不对，尝试从 query 参数中提取
                            if not stock_code or start_year is None:
                                query = arguments.get("query", "")
                                if query:
                                    print(f"[警告] 参数格式错误，尝试从 query 中提取: {query}")
                                    # 使用简单的正则提取公司名和年份
                                    import re
                                    # 提取年份（4位数字）
                                    year_match = re.search(r'(\d{4})', query)
                                    if year_match:
                                        start_year = int(year_match.group(1))
                                        end_year = start_year

                                        # 提取公司名（移除年份和其他关键词）
                                        # 先移除年份
                                        cleaned = re.sub(r'\d{4}年?', '', query)
                                        # 移除常见的前缀和后缀词
                                        cleaned = re.sub(r'^(给我|帮我|查询|收集|要|需要|请|请帮我|请给我)\s*', '',
                                                         cleaned)
                                        cleaned = re.sub(
                                            r'\s*(的|年报|年报分析|财报|财报分析|数据|报告|分析|收集|查询)\s*$', '',
                                            cleaned)
                                        # 移除中间的"的"字
                                        cleaned = re.sub(r'\s*的\s*', '', cleaned)
                                        stock_code = cleaned.strip()

                                    if stock_code and start_year:
                                        print(f"[提取结果] 公司: {stock_code}, 年份: {start_year}")

                            if stock_code and start_year is not None:
                                tool_result = CollectFinancialDataTool()._run(stock_code, start_year, end_year)
                                agent_output = tool_result
                                tool_executed = True
                            else:
                                print(f"[错误] 无法提取有效参数：stock_code={stock_code}, start_year={start_year}")
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        print(f"[警告：解析工具参数失败: {e}]")

            # 2. 检查是否是 OpenAI function calling 格式：{"name": "...", "arguments": {...}}
            if not tool_executed and isinstance(agent_output, str):
                # 提取所有 JSON 代码块
                json_blocks = re.findall(r'```(?:json)?\s*(\{[^`]+\})\s*```', agent_output, re.DOTALL)

                if json_blocks:
                    # 遍历所有 JSON 代码块，找到第一个有效的工具调用
                    for json_str in json_blocks:
                        try:
                            tool_call_data = json.loads(json_str.strip())

                            # 检查是否是 OpenAI function calling 格式
                            if isinstance(tool_call_data,
                                          dict) and "name" in tool_call_data and "arguments" in tool_call_data:
                                tool_name = tool_call_data["name"]
                                arguments = tool_call_data["arguments"]

                                print(f"\n{'=' * 60}")
                                print(f"[检测到工具调用：{tool_name}]")
                                print(f"[参数：{json.dumps(arguments, ensure_ascii=False)}]")
                                print(f"{'=' * 60}\n")

                                # 根据工具名称执行相应的工具
                                if tool_name == "collect_financial_data_pipeline":
                                    # 检查参数格式
                                    stock_code = arguments.get("stock_code")
                                    start_year = arguments.get("start_year")
                                    end_year = arguments.get("end_year", start_year)

                                    # 如果参数格式不对，尝试从 query 参数中提取
                                    if not stock_code or start_year is None:
                                        query = arguments.get("query", "")
                                        if query:
                                            print(f"[警告] 参数格式错误，尝试从 query 中提取: {query}")
                                            # 使用简单的正则提取公司名和年份
                                            import re
                                            # 提取年份（4位数字）
                                            year_match = re.search(r'(\d{4})', query)
                                            if year_match:
                                                start_year = int(year_match.group(1))
                                                end_year = start_year

                                            # 提取公司名（移除年份和其他关键词）
                                            # 先移除年份
                                            cleaned = re.sub(r'\d{4}年?', '', query)
                                            # 移除常见的前缀和后缀词
                                            cleaned = re.sub(r'^(给我|帮我|查询|收集|要|需要|请|请帮我|请给我)\s*', '',
                                                             cleaned)
                                            cleaned = re.sub(
                                                r'\s*(的|年报|年报分析|财报|财报分析|数据|报告|分析|收集|查询)\s*$', '',
                                                cleaned)
                                            # 移除中间的"的"字
                                            cleaned = re.sub(r'\s*的\s*', '', cleaned)
                                            stock_code = cleaned.strip()

                                            if stock_code and start_year:
                                                print(f"[提取结果] 公司: {stock_code}, 年份: {start_year}")

                                    if stock_code and start_year is not None:
                                        tool_result = CollectFinancialDataTool()._run(stock_code, start_year, end_year)
                                        agent_output = tool_result
                                        tool_executed = True
                                        break  # 执行第一个有效的工具调用后退出
                                    else:
                                        print(
                                            f"[错误] 无法提取有效参数：stock_code={stock_code}, start_year={start_year}")
                        except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
                            continue  # 尝试下一个代码块

            # 检查其他格式的工具调用（兼容旧格式）
            if isinstance(agent_output, str) and ("<execute_collection>" in agent_output or (
                    "execute_financial_data_collection" in agent_output.lower() and "parameters" in agent_output)):
                # Agent 返回了工具调用的文本格式，但没有真正执行
                # 尝试从文本中提取参数并手动执行工具

                # 尝试从文本中提取 JSON（支持多行和嵌套）
                # 匹配包含 "tool" 和 "parameters" 的 JSON 对象
                json_pattern = r'\{[^{}]*"tool"[^{}]*"parameters"[^{}]*\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}[^{}]*\}'
                json_matches = re.findall(json_pattern, agent_output, re.DOTALL)

                # 如果没找到，尝试更简单的模式
                if not json_matches:
                    json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                    json_matches = re.findall(json_pattern, agent_output, re.DOTALL)

                for json_str in json_matches:
                    try:
                        tool_data = json.loads(json_str)

                        # 检查是否包含 parameters
                        params = None
                        if "parameters" in tool_data:
                            params = tool_data["parameters"]
                        elif "tool" in tool_data and isinstance(tool_data["tool"], dict) and "parameters" in tool_data[
                            "tool"]:
                            params = tool_data["tool"]["parameters"]

                        if params and isinstance(params, dict):
                            stock_code = params.get("stock_code", "")
                            start_year = params.get("start_year")
                            end_year = params.get("end_year", start_year)

                            if stock_code and start_year is not None:
                                # 手动执行工具
                                print("\n[检测到工具调用格式，手动执行工具...]")
                                tool_result = ExecuteFinancialDataTool()._run(stock_code, start_year, end_year)
                                agent_output = tool_result
                                break  # 找到并执行后退出
                    except (json.JSONDecodeError, KeyError, TypeError, AttributeError) as e:
                        continue

            # --- 步骤 1 检查: 是否是 Tool 1 返回的参数 JSON? ---
            # 检查 Agent 的输出是否是 Tool Call 返回的 JSON 字符串 (通常 Agent 会返回 Tool 的结果)
            # 注意：Agent 可能返回嵌套格式 {"tool": "...", "parameters": {...}} 或直接返回工具结果
            import json

            # 尝试解析 JSON（可能是字符串或已经是字典）
            data = None
            try:
                if isinstance(agent_output, str):
                    # 尝试解析字符串
                    if agent_output.strip().startswith('{'):
                        data = json.loads(agent_output)
                elif isinstance(agent_output, dict):
                    # 如果已经是字典，直接使用
                    data = agent_output
            except (json.JSONDecodeError, AttributeError):
                pass

            # 如果成功解析为 JSON，检查是否包含参数
            if data and (any(key in data for key in ["stock_code", "start_year", "parameters"])):
                # 处理嵌套格式：如果参数在 "parameters" 字段中
                if "parameters" in data and isinstance(data["parameters"], dict):
                    params = data["parameters"]
                    stock_code = params.get("stock_code", "")
                    start_year = params.get("start_year")
                    end_year = params.get("end_year", start_year)  # 如果没有 end_year，使用 start_year
                else:
                    # 直接格式：参数直接在顶层
                    stock_code = data.get("stock_code", "")
                    start_year = data.get("start_year")
                    end_year = data.get("end_year", start_year)  # 如果没有 end_year，使用 start_year

                # 检查必要参数是否存在
                if start_year is None:
                    ai_response = (
                        "参数验证失败：\n"
                        "缺少必要参数 'start_year'，请重新输入。"
                    )
                else:
                    # 调用验证函数
                    validation_result = validate_and_prepare_params(stock_code, start_year, end_year)

                    # 处理两种返回情况
                    if "error" in validation_result:
                        # 第一种：错误情况（比如年份不对的报错）
                        ai_response = (
                            f"参数验证失败：\n"
                            f"{validation_result['error']}\n"
                            f"请重新输入正确的参数。"
                        )
                        # 错误情况下不设置 pending_confirmation_data
                    else:
                        # 第二种：正常返回的 JSON
                        # 捕获待确认数据 (虽然这里仅用于格式化，但保留状态变量能防止 Agent 意外回复)
                        # 将验证后的完整数据保存，包含所有必要字段
                        pending_confirmation_data = {
                            "stock_code": validation_result["stock_code"],
                            "start_year": start_year,
                            "end_year": end_year,
                            "company_name": validation_result.get("company_name", stock_code),
                            "start_date": validation_result["start_date"],
                            "end_date": validation_result["end_date"],
                            "year": validation_result["year"],
                            "exchange_type": validation_result.get("exchange_type")
                        }

                        # 主循环构造固定格式的回复（使用验证后的数据）
                        formatted_json = json.dumps(pending_confirmation_data, indent=2, ensure_ascii=False)
                        ai_response = (
                            "我已成功提取您请求的参数，请确认：\n"
                            f"{formatted_json}\n"
                            "请回复 **'确认'** 或 **'否认'**。"
                        )
            else:
                # 不是预期的 JSON 格式，按普通回复处理（步骤 2：Agent 内部自己处理了确认/执行或闲聊）
                # 可能是自然语言回复（闲聊、否认后的重输要求、或 Tool 2 成功执行后的返回结果）
                ai_response = agent_output

                # 检查是否是数据收集完成的消息
                if "数据收集任务已完成" in agent_output and CollectFinancialDataTool.last_output_dir:
                    # 数据收集完成，标记数据已准备好
                    run_chat_agent.data_ready = True
                    run_chat_agent.output_dir = CollectFinancialDataTool.last_output_dir

                    # 设置切换标志，在打印完消息后切换到 react agent
                    run_chat_agent.should_switch_to_react = True
                    run_chat_agent.react_output_dir = run_chat_agent.output_dir
                    # 保存合并后的CSV文件路径
                    run_chat_agent.react_merged_file = CollectFinancialDataTool.last_merged_file

                    # 清理回复，移除不必要的服务提示
                    # 只保留工具返回的核心信息
                    lines = ai_response.split('\n')
                    cleaned_lines = []
                    skip_section = False
                    for line in lines:
                        # 如果遇到服务提示相关的行，跳过
                        if any(keyword in line for keyword in
                               ["如需以下服务", "获取其他年份", "获取其他上市公司", "导出数据", "支持Excel",
                                "支持CSV"]):
                            skip_section = True
                            continue
                        if skip_section and (
                                line.strip() == "" or line.strip().isdigit() or line.strip().startswith("-")):
                            continue
                        if skip_section and not line.strip().startswith(("1.", "2.", "3.", "-")):
                            skip_section = False
                        if not skip_section:
                            cleaned_lines.append(line)
                    ai_response = '\n'.join(cleaned_lines).strip()

                    # 在回复末尾添加切换提示（只添加一次）
                    if "数据收集已完成" not in ai_response:
                        ai_response += (
                                "\n\n" + "=" * 60 + "\n"
                                                    "数据收集已完成！正在切换到数据分析模式...\n"
                                                    "=" * 60
                        )

                # 如果 Agent 成功执行了 Tool 2，或者用户回复了否认，我们可以清空状态
                if pending_confirmation_data and (user_input.lower() in ["确认", "否认", "确定", "不要"]):
                    pending_confirmation_data = None


        except Exception as e:
            ai_response = f"抱歉，处理您的请求时出现错误: {e}"
            print(f"Agent 错误: {e}")

        # 如果数据已准备好，且用户的问题看起来像是分析问题，使用 react agent
        if run_chat_agent.data_ready and run_chat_agent.output_dir:
            # 检查是否是分析类问题（不是数据收集相关的问题）
            analysis_keywords = ["分析", "查询", "问", "什么", "多少", "如何", "为什么", "查看", "显示"]
            is_analysis_question = any(keyword in user_input for keyword in analysis_keywords)

            # 如果用户没有明确说要收集数据，且问题是分析类问题，使用 react agent
            if is_analysis_question and "收集" not in user_input and "数据收集" not in user_input:
                # 延迟导入 react agent，避免循环依赖
                if react_agent_executor is None:
                    try:
                        from main_2_fixed_output_react import agent_executor as react_executor
                        react_agent_executor = react_executor
                        print("\n[切换到数据分析模式 - 使用 ReAct Agent]")
                    except Exception as e:
                        print(f"\n[警告：无法加载 ReAct Agent: {e}]")

                if react_agent_executor:
                    try:
                        # 在用户输入中添加数据目录信息
                        enhanced_input = f"{user_input}\n\n[数据目录: {run_chat_agent.output_dir}]"
                        react_result = react_agent_executor.invoke({"input": enhanced_input})
                        ai_response = react_result["output"]
                    except Exception as e:
                        ai_response = f"ReAct Agent 处理时发生错误: {e}"

        # 只有在工具未执行时才输出 AI 的原始回复
        if not tool_executed:
            print(f"\nAI: {ai_response}")
        else:
            # 工具已执行，直接输出结果
            print(f"\n{ai_response}")
        print("-" * 60)

        # 如果数据收集完成，切换到 react agent（只执行一次）
        if run_chat_agent.should_switch_to_react and run_chat_agent.react_output_dir:
            # 立即重置标志，避免重复执行
            run_chat_agent.should_switch_to_react = False
            print("\n" + "=" * 60)
            print("切换到数据分析模式 (ReAct Agent)")
            print("=" * 60 + "\n")
            try:
                from main_2_fixed_output_react import run_chat_agent as run_react_agent
                # 启动 react agent，传入输出目录和合并后的CSV文件路径
                run_react_agent(
                    data_output_dir=run_chat_agent.react_output_dir,
                    merged_data_file=run_chat_agent.react_merged_file
                )
                break  # react agent 结束后退出
            except Exception as e:
                print(f"启动 ReAct Agent 时出错: {e}")
                import traceback
                traceback.print_exc()
                print("\n继续使用当前 Agent 模式...")


if __name__ == "__main__":
    run_chat_agent()
