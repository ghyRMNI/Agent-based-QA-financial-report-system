# HKTableExtractor 使用说明（Markdown）

本文档介绍 `pdf_extractor/hk_table_extractor.py` 的功能、端口（对外使用方式）、处理流程与可配置阈值，便于在项目内/外部调用与调优。

---

## 1. 模块定位与特性

- 面向港股年报（HKEX）表格抽取的专用模块，已在项目中作为“统一表格抽取器”默认启用（不再区分 hke/szse）。
- 完整流程三阶段：
  1) 抽取与构建（多策略）；
  2) 二次筛选（质量清洗、垃圾表删除）；
  3) 第三次筛选（同页多 CSV 自动择优，复制到 `csv_selected/`）。

- 核心能力：
  - 多层表头重建（仅在“明显优于直接用第一行”的情况下启用）；
  - 拆分“单格多行”（对齐复制 + 换行炸开）；
  - 切裂数字合并（例：`95,88` + `8` → `95,888`，包含括号负数）；
  - 二次筛选：小表/空值重/中文长文本/连续空值/中文占比高等剔除；
  - 第三次筛选：面向“财务主表”版式的评分器，自动挑选“每页Top-2”复制到 `csv_selected/`；
  - 页顶短标题识别：若识别到，写入 CSV 第一行（不改变列结构），便于复核与下游处理。

---

## 2. 端口一览（对外使用方式）

### 2.1 从 Python 代码调用

```python
from pdf_extractor.hk_table_extractor import HKTableExtractor

extractor = HKTableExtractor()  # 可传入 logger
files = extractor.extract_tables(
    pdf_path="input/长和_2022年年报.pdf",
    pages=None,                      # Set[int]，不传表示全页
    output_dir="output",             # 输出根目录
    output_format="csv"              # 仅支持 csv
)
print(files)  # 第一阶段落地到 output/【PDF名】/csv/ 的路径列表
# 第三次筛选的“每页Top-2”会同步复制到 csv_selected/ 目录
```

- 唯一“正式端口”是 `HKTableExtractor.extract_tables(...)`，其余均为内部方法。

### 2.2 通过 CLI/启动脚本调用（推荐）

- 交互模式
  ```
  python start.py -i
  ```
  - Step 1：输入 PDF 路径
  - Step 2：选择 Extract Text/Tables/Images/OCR
  - Step 3：选择页码（可跳过 → 全页）
  - Step 4：选择输出目录（默认项目根 output/）
  - Step 5：选择表格格式（csv）

- 简易模式（默认提取文本+表格）
  ```
  python start.py
  ```
  - 按提示输入 PDF 路径即可

- 直接 CLI（非交互）
  ```
  python pdf_extractor_cli.py --file "input/长和_2022年年报.pdf" --tables --output output --verbose
  ```
  - 说明：主程序已统一使用 `HKTableExtractor`，无需再传 `--column`。

---

## 3. 输出目录结构

以 `长和_2022年年报.pdf` 为例：

```
output/
 └─ 长和_2022年年报/
     ├─ csv/            # 第一阶段的所有 CSV 表格
     ├─ csv_selected/   # 第三次筛选：同页Top-2 拷贝于此（仅复制，不删除原文件）
     ├─ images/         # （若勾选）图片
     └─ txt/            # （若勾选）全文文本
```

- 页顶短标题（若识别到）会作为 CSV 第一行写入（第一列为标题文本，其余列留空），不改变表结构。

---

## 4. 三阶段处理流程（简述）

1) 抽取与构建
   - `pdfplumber` 多策略：带线、纯文本、混合等；`find_tables()` 和 `extract_tables()` 组合；
   - 同内容去重：基于“文本+数字签名”分组去重；
   - DataFrame 构建：
     - “简单头（第一行为表头）” 与 “表头重建（顶部1~3行拼接）”双方案评分择优；
     - 多行对齐+拆分（换行炸开）；
     - 切裂数字合并；
     - 去重列名、首列命名“项目”、清理全空列。

2) 二次筛选（质量清洗）
   - 删除以下类型：
     - 列数<3；行数<7；
     - 中文>20 的单元格累计>8；
     - 中文≥30 的单元格出现≥1（出现一个就删）；
     - 中文≥40 的单元格出现≥1（严格规则，也会删）；
     - 空值比例>30%（空值定义：去空白后为空字符串）；
     - 连续空值≥6（仅列数<4 时删除）；
     - 中文字符占比>90%（整体偏说明文本）。
   - 完成后，返回保留下来的 CSV 列表（位于 `csv/`）。

3) 第三次筛选（同页Top-2 择优）
   - 对同一页中的多份 CSV 打分排序，复制前2名到 `csv_selected/`；
   - 评分器 `_score_hk_financial_layout` 偏好特征：
     - 列数 4–8；
     - 第一列中文项目列明显（文本多、数字少）；
     - 存在 2 个强数值/年份列（列名含 2022/2021/港幣/百萬元/% 或数字密度高）；
     - 存在“附註/附注”列或疑似附注列（短中文数字）；
     - 标题关键词命中（如 綜合(全面)收益表/財務狀況表/現金流量表/權益變動表/財務表現概要/概覽 等）；
     - 长文本比例低、非空均衡性好；
     - 对“多行合并为一格”的垃圾表强惩罚（单格≥40中文或≥90字符；第一列特长且同行非空少等）。

---

## 5. 内部关键函数（仅概览）

- `extract_tables(pdf_path, pages=None, output_dir="output", output_format="csv") -> List[str]`
  - 唯一对外端口。完成三阶段处理（落地到 `csv/`，并复制 Top-2 到 `csv_selected/`）。

- 表头/构建相关（内部）：
  - `_reconstruct_headers(raw) -> (columns, data_rows)`
  - `_df_structure_score(df) -> float`（基本结构评分）
  - `_distribute_and_explode(df) -> df`（单元格内换行对齐+拆分）
  - `_merge_split_numbers(df) -> df`（切裂数字合并）
  - `_dedup_columns(df) -> df`（列名去重并首列命名“项目”）
  - `_detect_short_title(page) -> Optional[str]`（页顶短标题，写入CSV首行）

- 筛选相关（内部）：
  - `_secondary_filter_files(files) -> List[str]`
  - `_score_hk_financial_layout(df) -> float`（第三次筛选评分器）
  - `_third_select_best_per_page(files, csv_dir) -> List[str]`（同页Top-2 复制到 `csv_selected/`）

---

## 6. 可调阈值（在文件顶部常量，可按需微调）

- 列/行阈值
  - `MIN_COLUMNS_THRESHOLD = 3`
  - 行数最少：`df.shape[0] < 7` → 删除

- 文本判定
  - `TEXT_HEAVY_LONG_CN_LEN = 20`，`TEXT_HEAVY_MAX_CELLS = 8`
  - `LONG_CELL_CN_LEN = 30`，`LONG_CELL_CN_MAX_COUNT = 0`（出现一个就删）
  - `STRICT_LONG_CELL_CN_LEN = 40`（严格超长，出现≥1即删）
  - `CHINESE_TEXT_RATIO_MAX = 0.90`（中文占比>90% 删除）

- 空值判定
  - 空值定义：`str(val).strip() == ""`
  - 空值比例阈值：`> 0.30` 删除
  - `EMPTY_RUN_THRESHOLD = 6`（连续空值阈值，列数<4 时删除）

- 第三次筛选（合并行垃圾表）核心惩罚
  - 行内“极端长文本”：单格 ≥60中文或 ≥120字符 → 强惩罚
  - 第一列非常长且同行其它列大多为空 → 作为“合并征兆”
  - 合并征兆占比 >5/10/20% 分别递进惩罚

---

## 7. 常见问题（FAQ）

- Q：为什么 `csv/` 里仍有“垃圾表”？
  - A：第三次筛选是“复制优选”，不会删除原始 CSV；优选结果在 `csv_selected/`。若希望移动到 `csv_rejected/` 或直接删除，可再加一个“第四阶段”动作（保留/移动/删），按需我可补上。

- Q：为什么没有输出？
  - A：请先确认 output 下的子目录：`output/【PDF名】/csv/` 与 `csv_selected/`；也可加 `--verbose` 查看二次筛选删除原因（行少、列少、空值比例高、长文本等）。

---

## 8. 最简使用示例

- 交互：`python start.py -i`
- 简易：`python start.py`
- CLI：`python pdf_extractor_cli.py --file "input/长和_2022年年报.pdf" --tables --output output --verbose`

---

如需把第三次筛选“Top-2”改为 Top-K、或者希望把“非最佳且判定为垃圾表”的 CSV 移动至 `csv_rejected/`，告知我目标，我可以追加参数与实现。

