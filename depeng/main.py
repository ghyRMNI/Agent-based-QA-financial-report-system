import os  # 导入 os 模块，用于操作系统相关功能（如检查文件存在、获取环境变量）
import torch  # 导入 PyTorch 库，用于设备检测（CPU/CUDA）和底层模型操作
from langchain_community.document_loaders import PyPDFLoader, TextLoader  # 导入 LangChain 社区库的文档加载器
from langchain.text_splitter import RecursiveCharacterTextSplitter  # 导入递归字符文本分割器
from langchain_huggingface import HuggingFaceEmbeddings  # 导入 Hugging Face 嵌入模型类（用于本地模型）
from langchain_community.vectorstores import Chroma  # 导入 Chroma 向量数据库类
from langchain_community.chat_models import ChatOpenAI  # 导入 ChatOpenAI 类（兼容OpenAI协议的模型接口）
from langchain.prompts import ChatPromptTemplate  # 导入聊天提示模板类
from langchain.schema.output_parser import StrOutputParser  # 导入字符串输出解析器
from langchain.schema.runnable import RunnablePassthrough  # 导入 RunnablePassthrough，用于 LCEL 管道中传递输入


if os.path.exists("./data/三全食品_2023年年度报告_text.txt"):
    print("Yes")  # 检查数据文件是否存在，如果存在则打印 "Yes"
device = "cuda" if torch.cuda.is_available() else "cpu"  # 检查是否有可用的 NVIDIA GPU，决定使用 "cuda" 或 "cpu" 设备

# 1. 加载数据
loader = TextLoader("./data/三全食品_2023年年度报告_text.txt", encoding='utf-8')  # 实例化 TextLoader，指定要加载的文本文件路径和编码
# 如果是文本文件：loader = TextLoader("my_document.txt", encoding="utf-8")  # 示例注释：如何加载另一个文本文件
documents = loader.load()  # 执行加载操作，获取文档信息


# 2. 分割文档中的数据为一个个文本块
text_splitter = RecursiveCharacterTextSplitter(  # 实例化递归字符文本分割器
    chunk_size=1000,  # 设置每个文档块的最大长度（按字符或 Token 计，取决于模型）
    chunk_overlap=200,  # 设置相邻文档块之间的重叠长度，有助于保留上下文
    length_function=len,  # 设置计算长度的函数（这里是标准 Python len()）
    add_start_index=True,  # 是否在元数据中添加块在原文档中的起始索引
)
splits = text_splitter.split_documents(documents)  # 执行分割操作，生成小块 Document 列表

# 3. 建立索引 (创建嵌入和向量存储)
model_name = "moka-ai/m3e-base"  # 指定使用的 Hugging Face 嵌入模型名称（中文常用模型）

# 创建嵌入模型
embedding = HuggingFaceEmbeddings(  # 实例化 HuggingFaceEmbeddings
    model_name=model_name,  # 传入模型名称
    # 启用 CUDA 以加速计算 (如果您的机器有 GPU)
    model_kwargs={'device': device}  # 将前面检测到的设备（'cuda'或'cpu'）传递给模型
)

vectorstore = Chroma.from_documents(  # 使用分割后的文档块和嵌入模型创建 Chroma 向量存储
    documents=splits,  # 传入分割后的文档块
    embedding=embedding,  # 传入嵌入模型
    persist_directory="./chroma_db"  # 设置向量数据库的持久化存储目录
)
retriever = vectorstore.as_retriever(search_kwargs={"k": 3})  # 将向量存储转换为检索器，并设置检索参数 k=3（每次检索返回 3 个最相关的文档块）

# 创建系统提示词
template = """
您是一位乐于助人的AI助手。  # 设定 AI 助手的角色
请仅根据提供的以下上下文来回答问题。  # 限制模型只能使用提供的上下文
如果上下文中没有足够的信息，请说您不知道，不要编造答案。 # 避免幻觉（Hallucination）
请使用中文回答。

上下文:  # 上下文占位符
{context}

问题:  # 问题占位符
{question}
"""
prompt = ChatPromptTemplate.from_template(template)  # 使用上述模板创建 ChatPromptTemplate 实例，用来储存提示词，方便后续操作

# 访问并连接大语言模型
llm = ChatOpenAI(  # 实例化 ChatOpenAI 类，用于调用兼容 OpenAI 协议的模型服务
    model='模型名称，需填入',  # 指定要调用的模型名称
    base_url='模型访问地址，需填入',  # 指定自定义的 API 地址（Base URL）
    api_key='模型的密钥，需填入',  # 传入自定义的 API Key
    temperature=0.2,  # 设置模型的随机性/创造性，较低的值（0.2）使得回答更稳定、更忠实于上下文
)

def format_docs(docs):  # 定义一个函数，用于格式化检索器返回的文档列表
    return "\n\n".join(doc.page_content for doc in docs)  # 提取每个文档块的纯文本内容，把rag检索到的相关文本块连接到一起

# 构建 RAG Chain
rag_chain = (  # 使用 LangChain Expression Language (LCEL) 构建 RAG 管道
    {"context": retriever | format_docs, "question": RunnablePassthrough()}  # 定义输入映射：context 由检索器和格式化函数生成；question 由用户输入直接穿透传递
    | prompt  # 将格式化的 context 和 question 填入 prompt 模板
    | llm  # 将完整的提示词发送给 LLM 进行生成
    | StrOutputParser()  # 解析 LLM 的输出，只返回纯文本字符串
)

# 运行 RAG Chain
question = "三全食品年度报告提到了哪些关于未来发展战略的信息？"  # 定义用户要提出的问题
response = rag_chain.invoke(question)  # 执行整个 RAG 管道，获取最终回答（此行代码是运行代码）
print("\n📝 AI 助手的回答:")  # 打印提示信息
print(response)  # 打印 LLM 生成的最终回答