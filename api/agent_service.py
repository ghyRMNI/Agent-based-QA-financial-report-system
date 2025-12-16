"""
Agent 服务类 - 将 main.py 中的 agent 逻辑封装为可调用的服务
"""
import os
import json
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferWindowMemory

# 导入 main.py 中的所有 agent 创建函数和工具
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from main import (
    create_data_collection_agent,
    create_analysis_agent,
    create_router_agent,
)
from main_pipeline import UnifiedDataCollector
from api.streaming_handler import LangChainCallbackHandler

# 由于 main.py 使用了全局变量，我们需要在这里管理它们
# 但为了更好的封装，我们使用实例变量


class AgentService:
    """Agent 服务类，封装 agent 的初始化和调用逻辑"""
    
    def __init__(self):
        """初始化 Agent 服务"""
        self.llm: Optional[ChatOpenAI] = None
        self.data_collection_agent = None
        self.analysis_agent = None
        self.router_agent = None
        self.memory: Optional[ConversationBufferWindowMemory] = None
        self.root_path: Optional[str] = None
        self.pending_confirmation_data: Optional[dict] = None
        self._initialized = False
    
    def initialize(self):
        """初始化所有 agents（如果还未初始化）"""
        if self._initialized:
            return
        
        # 加载环境变量
        load_dotenv()
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("环境变量 OPENAI_API_KEY 未设置或加载失败。请检查 .env 文件。")
        
        # 初始化 LLM
        self.llm = ChatOpenAI(
            model='deepseek-r1-250528',
            base_url='https://ark.cn-beijing.volces.com/api/v3',
            temperature=0.1,
        )
        
        # 初始化共享内存
        self.memory = ConversationBufferWindowMemory(
            memory_key="chat_history",
            k=10,
            return_messages=True
        )
        
        # 同步全局变量（在创建 agents 之前，因为工具类可能使用全局变量）
        import main as main_module
        main_module.GLOBAL_LLM = self.llm
        main_module.GLOBAL_MEMORY = self.memory
        main_module.ROOT_PATH = None  # 初始化为 None
        
        # 创建子agents
        # 注意：main.py 中的 agent 创建函数设置了 verbose=True
        # 这会产生包含 ANSI 转义码的输出，我们会在 PrintCapture 中过滤这些转义码
        self.data_collection_agent = create_data_collection_agent(self.llm, self.memory)
        self.analysis_agent = create_analysis_agent(self.llm, self.memory)
        self.router_agent = create_router_agent(self.llm, self.memory)
        
        # 保持 verbose=True 以显示执行过程，但 ANSI 转义码会被 PrintCapture 过滤
        # 这样用户可以看到完整的 Agent 执行过程（如 "> Entering new AgentExecutor chain..."）
        
        # 同步全局变量（创建后）
        main_module.GLOBAL_DATA_COLLECTION_AGENT = self.data_collection_agent
        main_module.GLOBAL_ANALYSIS_AGENT = self.analysis_agent
        
        self._initialized = True
    
    def process_user_input(self, user_input: str, callbacks: Optional[List] = None) -> Dict[str, Any]:
        """
        处理用户输入，返回 AI 回复
        
        Args:
            user_input: 用户输入
            callbacks: LangChain 回调列表，用于捕获中间步骤
            
        Returns:
            包含 'response' 字段的字典，可能包含其他元数据
        """
        if not self._initialized:
            self.initialize()
        
        try:
            # 调用路由agent
            # 注意：router_agent 不是标准的 AgentExecutor，直接传递 input
            result = self.router_agent.invoke({"input": user_input})
            router_output = result["output"]
            
            # 解析路由Agent的JSON输出
            router_decision = None
            if router_output and isinstance(router_output, str):
                json_str = router_output.strip()
                # 移除可能的markdown代码块标记
                if json_str.startswith("```json"):
                    json_str = json_str[7:]
                if json_str.startswith("```"):
                    json_str = json_str[3:]
                if json_str.endswith("```"):
                    json_str = json_str[:-3]
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
                    # 调用数据收集Agent（传入 callbacks）
                    if callbacks:
                        sub_result = self.data_collection_agent.invoke(
                            {"input": user_input_for_sub_agent},
                            config={"callbacks": callbacks}
                        )
                    else:
                        sub_result = self.data_collection_agent.invoke({"input": user_input_for_sub_agent})
                    sub_output = sub_result["output"]
                    
                    # 处理数据收集agent的特殊输出格式
                    ai_response = self._handle_collection_agent_output(sub_output, user_input)
                    
                elif tool_name == "route_to_data_analysis":
                    # 调用数据分析Agent（传入 callbacks）
                    if callbacks:
                        sub_result = self.analysis_agent.invoke(
                            {"input": user_input_for_sub_agent},
                            config={"callbacks": callbacks}
                        )
                    else:
                        sub_result = self.analysis_agent.invoke({"input": user_input_for_sub_agent})
                    ai_response = sub_result["output"]
                    
                else:
                    ai_response = router_output
            else:
                ai_response = router_output
            
            # 清理确认状态
            if self.pending_confirmation_data and (user_input.lower() in ["确认", "否认", "确定", "不要"]):
                self.pending_confirmation_data = None
            
            return {
                "response": ai_response,
                "success": True
            }
            
        except Exception as e:
            error_msg = f"抱歉，处理您的请求时出现错误: {e}"
            return {
                "response": error_msg,
                "success": False,
                "error": str(e)
            }
    
    def _handle_collection_agent_output(self, sub_output: str, user_input: str) -> str:
        """处理数据收集agent的输出"""
        if not sub_output or not isinstance(sub_output, str):
            return sub_output
        
        json_str = sub_output.strip()
        # 清理可能的markdown代码块标记
        if json_str.startswith("```json"):
            json_str = json_str[7:]
        if json_str.startswith("```"):
            json_str = json_str[3:]
        if json_str.endswith("```"):
            json_str = json_str[:-3]
        json_str = json_str.strip()
        
        # 检查是否是参数确认的JSON格式
        if json_str.startswith('{') and any(
                key in json_str for key in ["stock_code", "start_date", "tool"]):
            try:
                data = json.loads(json_str)
                
                if data.get('tool') == "collect_financial_data_pipeline":
                    self.pending_confirmation_data = data
                    formatted_json = json.dumps(data, indent=2, ensure_ascii=False)
                    return (
                        "我已成功提取您请求的参数，请确认：\n"
                        f"{formatted_json}\n"
                        "请回复 **'确认'** 或 **'否认'**。"
                    )
                
                elif data.get('tool') == "execute_financial_data_collection":
                    self.pending_confirmation_data = data
                    params = data.get("parameters", data)
                    stock_code = params.get("stock_code", "").split(".")[0]
                    start_date = params.get("start_date")
                    end_date = params.get("end_date")
                    
                    # 执行数据收集（这会触发很多 print 输出）
                    collector = UnifiedDataCollector(
                        company_name=stock_code,
                        stock_code=stock_code,
                        start_date=str(start_date),
                        end_date=str(end_date),
                        exchange_type=None,
                    )
                    self.root_path = collector.run_all()
                    
                    # 同步全局变量
                    import main as main_module
                    main_module.ROOT_PATH = self.root_path
                    
                    return (
                        f"已按以下信息爬取财报数据：\n"
                        f"股票代码: {stock_code}\n"
                        f"起始年份: {start_date}\n"
                        f"结束年份: {end_date}\n"
                        f"储存地址: {self.root_path}\n"
                        f"数据收集完成！现在可以询问任何关于此公司的信息。"
                    )
                else:
                    return sub_output
            except json.JSONDecodeError:
                return sub_output
        else:
            return sub_output
    
    def reset_memory(self):
        """重置对话内存"""
        if self.memory:
            # 清空内存
            self.memory.clear()
        # 重新初始化
        self.memory = ConversationBufferWindowMemory(
            memory_key="chat_history",
            k=10,
            return_messages=True
        )
        # 更新agents的内存
        if self.data_collection_agent:
            self.data_collection_agent.memory = self.memory
        if self.analysis_agent:
            self.analysis_agent.memory = self.memory

