# 📄 RAG 问答系统 (基于 LangChain & 本地嵌入)

## 1. 启动项目：环境准备

本项目依赖 conda 环境运行。请确保您已安装安装好 conda 虚拟环境

### 1.1. 进入环境
打开cmd，进入已创建好的虚拟环境
```bash
conda activate "环境名称"
```

### 1.2. 安装依赖

通过 `requirements.txt` 文件一次性安装所有必需的库：

```bash
pip install -r requirements.txt
```

### 1.3. 安装额外依赖

如果运行代码时有其它需要安装的包，可以手动在cmd中下载

```bash
pip install "包的名称"
```

## 2. 项目目录
```
depeng/
├── data/            # 用来存储数据集，目前只存放了一份财报
│   └── 三全食品_2023年年度报告_text.txt
├── chroma_db/       # 向量数据库文件，当执行完main.py会自动创建
│   └── chroma.sqlite3    
├── main.py          # 运行文件，执行此代码即可运行
├── model_access.docx  # 用来展示如何获取模型的名称(model)，地址(base_url)与密钥(api_key)
├── requirements.txt   # 这个文件用来安装必要的包
└── README.md        # 解释文件，讲解一些东西
```

## 3. 需要改写的地方
### 3.1 模型访问权限获取
main.py文件中64行-69行的llm变量中的参数需要自己填写
- model： 模型名称
- bash_url：模型的地址
- api_key：模型访问的密码，拥有此密码才有权限使用
### 注：这3个参数的获取方法参考文件 model_access.docx

### 3.2 问题
想问的问题可以写在83行 question 变量中


