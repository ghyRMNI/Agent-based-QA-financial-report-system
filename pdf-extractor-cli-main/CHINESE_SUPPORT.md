# 中文支持说明 / Chinese Support Guide

## 问题说明

如果您的 PDF 文件包含中文内容但无法正确提取，可能有以下几个原因：

1. **PDF 中的中文是文本形式**：代码已优化以更好地提取中文文本
2. **PDF 中的中文是图片形式**：需要使用 OCR 功能，并安装中文语言包
3. **文件编码问题**：已修复，所有输出文件使用 UTF-8 编码

## 解决方案

### 1. 文本提取（适用于文本型 PDF）

如果 PDF 中的中文是文本形式（不是图片），使用 `--text` 选项：

```bash
python pdf_extractor_cli.py --file "你的文件.pdf" --text
```

代码已优化，使用更好的文本提取方法以支持中文。

### 2. OCR 提取（适用于图片型 PDF）

如果 PDF 中的中文是图片形式（扫描件），需要使用 `--ocr` 选项，并安装 Tesseract 中文语言包。

#### 安装 Tesseract 中文语言包

**Windows:**
1. 下载并安装 Tesseract OCR：https://github.com/UB-Mannheim/tesseract/wiki
2. 在安装时，确保选择安装中文语言包（chi_sim 简体中文 或 chi_tra 繁体中文）
3. 或者手动下载语言包：
   - 简体中文：https://github.com/tesseract-ocr/tessdata/raw/main/chi_sim.traineddata
   - 繁体中文：https://github.com/tesseract-ocr/tessdata/raw/main/chi_tra.traineddata
4. 将下载的 `.traineddata` 文件放到 Tesseract 的 `tessdata` 目录（通常是 `C:\Program Files\Tesseract-OCR\tessdata\`）

**验证安装：**
```bash
tesseract --list-langs
```
应该能看到 `chi_sim` 或 `chi_tra`。

**使用 OCR 提取：**
```bash
python pdf_extractor_cli.py --file "你的文件.pdf" --ocr
```

### 3. 表格提取

表格提取已支持中文，输出文件使用 UTF-8 编码：

```bash
python pdf_extractor_cli.py --file "你的文件.pdf" --tables --table-format csv
```

CSV 文件使用 UTF-8-BOM 编码，可以在 Excel 中正确打开并显示中文。

### 4. 组合使用

可以同时使用多个选项：

```bash
# 提取文本和表格
python pdf_extractor_cli.py --file "你的文件.pdf" --text --tables

# 提取文本、表格和 OCR（如果 PDF 是扫描件）
python pdf_extractor_cli.py --file "你的文件.pdf" --text --tables --ocr
```

## 常见问题

### Q: 提取的中文显示为乱码？

A: 确保：
1. 输出文件使用 UTF-8 编码（代码已修复）
2. 使用支持 UTF-8 的文本编辑器打开（如 VS Code、Notepad++）
3. 如果使用 Excel 打开 CSV，确保选择 UTF-8 编码导入

### Q: OCR 无法识别中文？

A: 检查：
1. Tesseract 是否已安装中文语言包
2. 运行 `tesseract --list-langs` 查看已安装的语言
3. 如果没有中文语言包，按照上面的说明安装

### Q: 某些中文无法提取？

A: 可能原因：
1. PDF 中的中文是图片形式，需要使用 `--ocr`
2. PDF 使用了特殊字体或编码，尝试使用 OCR
3. PDF 文件损坏或加密

## 技术细节

### 文本提取优化
- 使用 `get_text("text")` 方法以获得更好的 Unicode 支持
- 如果标准方法失败，自动尝试字典模式提取
- 所有输出文件使用 UTF-8 编码

### OCR 优化
- 自动检测并优先使用简体中文（chi_sim）
- 如果简体中文不可用，尝试繁体中文（chi_tra）
- 如果中文语言包不可用，回退到英文
- 支持中英文混合识别

### 表格提取优化
- CSV 文件使用 UTF-8-BOM 编码，Excel 兼容性更好
- JSON 文件使用 UTF-8 编码，`force_ascii=False` 保留中文字符

