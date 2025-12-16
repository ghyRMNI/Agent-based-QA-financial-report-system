"""
捕获 print 输出并支持流式传输
包括 logging 输出和 LangChain 的详细输出
"""
import sys
import io
import logging
from queue import Queue
from typing import Optional, Callable, Any, Dict, List
from threading import Lock
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult


class LangChainCallbackHandler(BaseCallbackHandler):
    """LangChain 回调处理器，用于捕获 Agent 的思考过程和关键输出"""
    
    def __init__(self, output_callback: Optional[Callable[[str], None]] = None):
        """
        初始化回调处理器
        
        Args:
            output_callback: 当有输出时的回调函数
        """
        super().__init__()
        self.output_callback = output_callback
        
    def _output(self, text: str):
        """输出文本"""
        if self.output_callback:
            self.output_callback(text)
        else:
            print(text, end='', flush=True)
    
    def on_llm_start(self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any) -> None:
        """LLM 开始调用时 - 不输出"""
        pass
    
    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        """LLM 调用结束时 - 不输出"""
        pass
    
    def on_llm_error(self, error: Exception, **kwargs: Any) -> None:
        """LLM 调用出错时 - 只输出错误"""
        self._output(f"[错误] {str(error)}\n")
    
    def on_chain_start(self, serialized: Dict[str, Any], inputs: Dict[str, Any], **kwargs: Any) -> None:
        """Chain 开始执行时 - 输出开始标记"""
        try:
            if serialized is None:
                return
            # 尝试获取 chain 名称
            chain_name = "Unknown"
            if isinstance(serialized, dict):
                chain_name = serialized.get("name") or serialized.get("id")
                if isinstance(chain_name, list) and len(chain_name) > 0:
                    chain_name = chain_name[-1]
                if chain_name is None:
                    chain_name = str(serialized)
            else:
                chain_name = str(serialized)
            
            # 只输出 AgentExecutor chain
            if "AgentExecutor" in str(chain_name):
                self._output("> Entering new AgentExecutor chain...\n")
        except Exception as e:
            # 忽略错误，避免影响主要流程
            pass
    
    def on_chain_end(self, outputs: Dict[str, Any], **kwargs: Any) -> None:
        """Chain 执行结束时 - 不输出"""
        pass
    
    def on_chain_error(self, error: Exception, **kwargs: Any) -> None:
        """Chain 执行出错时 - 只输出错误"""
        self._output(f"[错误] {str(error)}\n")
    
    def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs: Any) -> None:
        """工具开始调用时 - 只输出工具名称"""
        try:
            if serialized is None:
                return
            tool_name = serialized.get("name", "Unknown") if isinstance(serialized, dict) else "Unknown"
            self._output(f"\n[调用工具] {tool_name}\n")
        except Exception:
            pass
    
    def on_tool_end(self, output: str, **kwargs: Any) -> None:
        """工具调用结束时 - 不输出"""
        pass
    
    def on_tool_error(self, error: Exception, **kwargs: Any) -> None:
        """工具调用出错时 - 只输出错误"""
        self._output(f"[工具错误] {str(error)}\n")
    
    def on_agent_action(self, action, **kwargs: Any) -> None:
        """Agent 执行动作时 - 输出工具调用结果（JSON 格式）"""
        try:
            if action is None:
                return
            # 输出工具调用的参数（JSON 格式）
            if hasattr(action, 'tool_input'):
                import json
                try:
                    # 如果是字典，格式化为 JSON
                    if isinstance(action.tool_input, dict):
                        tool_input_json = json.dumps(action.tool_input, indent=2, ensure_ascii=False)
                        self._output(f"{tool_input_json}\n")
                    else:
                        self._output(f"{action.tool_input}\n")
                except Exception:
                    self._output(f"{action.tool_input}\n")
        except Exception:
            pass
    
    def on_agent_finish(self, finish, **kwargs: Any) -> None:
        """Agent 完成时 - 输出完成标记"""
        try:
            self._output("\n> Finished chain.\n\n")
            # 最终回复会在 main.py 中通过 print 输出，这里不需要重复输出
        except Exception:
            pass


class PrintCapture:
    """捕获 stdout 和 stderr 的输出，以及 logging 输出"""
    
    def __init__(self, callback: Optional[Callable[[str], None]] = None):
        """
        初始化输出捕获器
        
        Args:
            callback: 当有输出时的回调函数，接收字符串参数
        """
        self.callback = callback
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
        self.buffer = io.StringIO()
        self.lock = Lock()
        self.capturing = False
        
        # 保存原始的 logging handlers
        self.logging_handlers = []
        self.loggers = []
        
    def write(self, text: str):
        """写入输出，去除 ANSI 转义码"""
        with self.lock:
            if self.capturing:
                # 去除 ANSI 转义码（颜色控制字符）
                import re
                ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
                clean_text = ansi_escape.sub('', text)
                
                # 写入缓冲区
                self.buffer.write(clean_text)
                # 调用回调函数
                if self.callback:
                    self.callback(clean_text)
            else:
                # 不在捕获状态时，直接写入原始输出
                self.original_stdout.write(text)
                
    def flush(self):
        """刷新缓冲区"""
        with self.lock:
            self.buffer.flush()
            if not self.capturing:
                self.original_stdout.flush()
    
    def start_capture(self):
        """开始捕获输出，包括 logging"""
        with self.lock:
            self.capturing = True
            self.buffer = io.StringIO()
            sys.stdout = self
            sys.stderr = self
            
            # 设置 logging 捕获
            # 获取所有相关的 logger（包括 langchain）
            self.loggers = [
                logging.getLogger(),
                logging.getLogger('langchain'),
                logging.getLogger('langchain.agents'),
                logging.getLogger('langchain.chains'),
                logging.getLogger('langchain.agent'),
                logging.getLogger('langchain.callbacks'),
            ]
            
            # 不捕获 logging 输出，因为 verbose=True 的 AgentExecutor 会输出很多冗余信息
            # 我们只通过 callbacks 捕获思考过程
            pass
    
    def stop_capture(self) -> str:
        """停止捕获并返回捕获的内容"""
        with self.lock:
            self.capturing = False
            sys.stdout = self.original_stdout
            sys.stderr = self.original_stderr
            
            # 恢复 logging handlers
            for logger, original_handlers in self.logging_handlers:
                # 移除我们添加的 handler
                current_handlers = list(logger.handlers)
                for h in current_handlers:
                    if h not in original_handlers:
                        logger.removeHandler(h)
            
            self.logging_handlers = []
            self.loggers = []
            
            content = self.buffer.getvalue()
            self.buffer = io.StringIO()
            return content
    
    def get_content(self) -> str:
        """获取当前缓冲区的内容（不停止捕获）"""
        with self.lock:
            return self.buffer.getvalue()


class OutputQueue:
    """用于存储输出消息的队列"""
    
    def __init__(self):
        self.queue: Queue[str] = Queue()
        self.lock = Lock()
    
    def put(self, message: str):
        """添加消息到队列"""
        with self.lock:
            self.queue.put(message)
    
    def get(self, timeout: Optional[float] = None) -> Optional[str]:
        """从队列获取消息"""
        try:
            return self.queue.get(timeout=timeout)
        except:
            return None
    
    def empty(self) -> bool:
        """检查队列是否为空"""
        return self.queue.empty()
    
    def clear(self):
        """清空队列"""
        with self.lock:
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()
                except:
                    pass
