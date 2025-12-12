"""
HKEX specialized table extractor (aligned with table_extractor.py architecture with HK annual report customs)

Overview:
- Reuse the same multi-strategy extraction -> de-duplication pipeline as table_extractor.py
- Add HKEX-specific repairs during DataFrame construction:
  1) Multi-row header reconstruction (scan the top few rows / compose column names / force first column name to "项目" / detect note column / detect year columns)
  2) Split multi-line content inside one cell into multiple rows (align-and-duplicate + explode by newlines)
  3) Merge split numeric chunks across columns (e.g., 95,88 + 8 -> 95,888; support parenthesis negatives)
- Very lenient filtering to avoid over-deleting (only minimal sanity checks: empty/small tables)
- Keep the same output directory layout as the base extractor
- Secondary filtering adds:
  a) Remove CSVs with more than 8 cells having >20 Chinese chars
  b) Remove CSVs with too few columns (default <3)
  c) Remove CSVs whose empty-cell ratio > 30%
  d) Remove CSVs with >=6 consecutive empty cells along row/column (with extra column-count guard)

Note: All runtime strings (logs) may still be Chinese per original project conventions. Only comments and docstrings here are translated to English.
"""
from __future__ import annotations
import os
import re
import logging
from typing import Any, List, Optional, Set, Tuple
from pathlib import Path

import pdfplumber
import pandas as pd
import shutil

from .utils import get_pdf_output_dirs, sanitize_filename


HK_HEADER_HINTS = [
    "IFRS", "港幣", "百萬元", "變動", "%", "年度", "年", "每股", "附註", "附注", "以常地貨幣"
]
HK_ROW_KEYWORDS = [
    "收入", "收益", "收益總額", "EBITDA", "EBIT", "除稅前溢利", "稅前溢利", "稅項", "本期",
    "綜合收益表", "綜合全面收益表", "綜合財務狀況表", "綜合權益變動表", "綜合現金流量表",
    "普通股東應佔溢利", "非控股權益", "投資收益", "折舊及攤銷", "金融資產負債"
]

# Secondary filter thresholds (tune as appropriate)
TEXT_HEAVY_LONG_CN_LEN = 20      # count as long text if Chinese chars > 20
TEXT_HEAVY_MAX_CELLS = 8         # remove CSV if long-text cells > 8
MIN_COLUMNS_THRESHOLD = 3        # remove CSV if column count < 3
SPACE_RATIO_MAX = 0.30           # (kept for reference) previous whitespace threshold; now we use empty-ratio instead
EMPTY_RUN_THRESHOLD = 6          # remove CSV if there are >=6 consecutive empty cells (with extra guards)
LONG_CELL_CN_LEN = 30            # remove CSV if any cell has >=30 Chinese chars (count >=1)
LONG_CELL_CN_MAX_COUNT = 0       # allowed count of >=30-Chinese cells (0 means any occurrence triggers removal)
STRICT_LONG_CELL_CN_LEN = 40     # strict long cell (>=40 Chinese) triggers removal
CHINESE_TEXT_RATIO_MAX = 0.90    # remove CSV if overall Chinese char ratio > 90% (likely narrative text)


class HKTableExtractor:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    # ---------------- HK financial-layout quick heuristic -----------------
    def _is_financial_hk(self, table_data: List[List[Any]]) -> bool:
        if not table_data or len(table_data) < 2:
            return False
        rows = [r for r in table_data if any(c and str(c).strip() for c in r)]
        if len(rows) < 2:
            return False
        max_cols = max(len(r) for r in rows if r)
        if max_cols < 2:
            return False
        text = "\n".join("\t".join(str(c) for c in (r or [])) for r in rows)
        has_kw = any(k in text for k in HK_ROW_KEYWORDS)
        has_hint = any(h in text for h in HK_HEADER_HINTS)
        digits = len(re.findall(r"\d", text))
        return (has_kw and has_hint) or digits >= 20

    # ---------------- Detect short title at page top (does not affect table structure) -----------------
    def _detect_short_title(self, page: pdfplumber.page.Page) -> Optional[str]:
        try:
            h = page.height
            top_cut = h * 0.18  # top 18% region
            words = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
            # group by approximate line (bucket by top)
            lines: dict[float, List[str]] = {}
            for w in words:
                top = float(w.get("top", 0.0))
                if top > top_cut:
                    continue
                key = round(top / 3) * 3
                lines.setdefault(key, []).append(w.get("text", ""))
            if not lines:
                # fallback: use first line from plain text
                text = (page.extract_text() or "").splitlines()
                cand = text[0].strip() if text else ""
                if cand:
                    cn = re.findall(r"[\u4e00-\u9fff]", cand)
                    if 2 <= len(cn) <= 20:
                        return cand[:20]
                return None
            # pick first line that looks like a short title
            for _, parts in sorted(lines.items(), key=lambda x: x[0]):
                s = "".join(parts).strip()
                if not s:
                    continue
                cn_chars = re.findall(r"[\u4e00-\u9fff]", s)
                cn_len = len(cn_chars)
                # require 2~20 Chinese chars and typical title keywords
                if 2 <= cn_len <= 20 and (
                    ("表" in s) or any(k in s for k in ["摘要", "概要", "概覽", "综", "綜", "财务", "財務", "收益"]) 
                ):
                    return s[:20]
            return None
        except Exception:
            return None

    # ---------------- Header reconstruction -----------------
    def _reconstruct_headers(self, raw: List[List[Any]]) -> Tuple[List[str], List[List[str]]]:
        rows = [[(str(c).replace("\r\n", "\n").replace("\r", "\n") if c is not None else "").strip() for c in r] for r in raw]
        top = min(8, len(rows))
        header_idxs: List[int] = []

        def looks_like_header(r: List[str]) -> bool:
            if not any(r):
                return False
            hint = sum(any(h in c for h in HK_HEADER_HINTS) for c in r)
            year = sum(1 for c in r if re.search(r"20\d{2}\s*年", c))
            short = sum(1 for c in r if 0 < len(c) <= 6)
            num_like = sum(1 for c in r if re.fullmatch(r"[\(（]?[-+]?\d[\d,，]*[\)）]?", c))
            score = hint*2 + year + short*0.2 - num_like*0.5
            return score >= 2

        for i in range(top):
            if looks_like_header(rows[i]):
                header_idxs.append(i)
            elif header_idxs:
                break
        header_idxs = header_idxs[:3]
        if not header_idxs:
            cols = [v if v else f"列{j+1}" for j, v in enumerate(rows[0])]
            if cols:
                cols[0] = cols[0] or "项目"
                if cols[0].lower().startswith("unnamed:"):
                    cols[0] = "项目"
            return cols, rows[1:]

        max_cols = max(len(r) for r in rows)
        parts_by_col: List[List[str]] = [[] for _ in range(max_cols)]
        for hi in header_idxs:
            r = rows[hi]
            for j in range(max_cols):
                if j < len(r):
                    v = r[j]
                    if v and v.lower().startswith("unnamed:"):
                        v = ""
                    if v:
                        parts_by_col[j].append(v)
        cols = [" ".join(p) if p else f"列{i+1}" for i, p in enumerate(parts_by_col)]
        if cols:
            cols[0] = "项目"
        data_rows = rows[header_idxs[-1]+1:]
        return cols[:max(len(data_rows[0]) if data_rows else len(cols), len(cols))], data_rows

    # ---------------- Structure scoring (baseline, similar to table_extractor) -----------------
    def _df_structure_score(self, df: pd.DataFrame) -> float:
        if df is None or df.empty:
            return 0.0
        rows, cols = df.shape
        total = rows * cols
        if total == 0:
            return 0.0
        non_empty_cells = 0
        col_non_empty = [0]*cols
        row_non_empty = [0]*rows
        for r_i, (_, r) in enumerate(df.iterrows()):
            for c_i, v in enumerate(r):
                s = str(v).strip()
                if s != "" and s not in ["—", "–", "-"]:
                    non_empty_cells += 1
                    col_non_empty[c_i] += 1
                    row_non_empty[r_i] += 1
        score = 0.0
        ratio = non_empty_cells/total
        if 0.25 <= ratio <= 0.95:
            score += 30
        elif ratio > 0.95:
            score -= 10
        else:
            score -= 15
        col_cov = sum(1 for x in col_non_empty if x > 0)/cols
        if col_cov >= 0.8:
            score += 20
        elif col_cov >= 0.6:
            score += 10
        else:
            score -= 8
        row_cov = sum(1 for x in row_non_empty if x > 0)/rows
        if row_cov >= 0.8:
            score += 20
        elif row_cov >= 0.6:
            score += 10
        else:
            score -= 8
        if cols > 0:
            first_ratio = col_non_empty[0]/rows
            if first_ratio >= 0.6:
                score += 10
            elif first_ratio < 0.2:
                score -= 5
        return score

    # ---------------- Explode multi-line cells into multiple rows -----------------
    def _distribute_and_explode(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        def segs(val: str) -> List[str]:
            if pd.isna(val):
                return []
            s = str(val).replace("\r\n", "\n").replace("\r", "\n")
            out = [p.strip() for p in s.split("\n")]
            out = [x for x in out if x and x not in ["-", "—", "–"]]
            return out
        rows = []
        for _, row in df.iterrows():
            cells = ["" if pd.isna(x) else str(x) for x in row.tolist()]
            seg_list = [segs(v) for v in cells]
            L = max((len(s) for s in seg_list), default=1)
            if L >= 2:
                new_cells = []
                for v, sgs in zip(cells, seg_list):
                    if len(sgs) == 0:
                        new_cells.append("")
                    elif len(sgs) == 1:
                        new_cells.append("\n".join([sgs[0]]*L))
                    else:
                        new_cells.append(v)
                rows.append(new_cells)
            else:
                rows.append(cells)
        df = pd.DataFrame(rows, columns=df.columns)
        new_rows: List[List[str]] = []
        for _, row in df.iterrows():
            seg_list = [segs(v) for v in row.tolist()]
            if any(len(s) >= 2 for s in seg_list):
                L = max(len(s) for s in seg_list)
                for i in range(L):
                    new_rows.append([s[i] if i < len(s) else "" for s in seg_list])
            else:
                new_rows.append([str(x) if not pd.isna(x) else "" for x in row.tolist()])
        return pd.DataFrame(new_rows, columns=df.columns)

    # ---------------- Merge split numeric chunks across columns -----------------
    def _merge_split_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or df.shape[1] <= 2:
            return df
        def is_frag(s: str) -> bool:
            if not s:
                return False
            if re.fullmatch(r"[\(（]?\d{1,3}[,，]?\)?", s):
                return True
            if re.fullmatch(r"[\(（]?\d{1,3}[,，]\d{1,2}\)?", s):
                return True
            if re.search(r"[,，]$", s):
                return True
            return False
        def clean(s: str) -> Tuple[bool, str]:
            neg = ("(" in s) or ("（" in s)
            digits = re.sub(r"[^0-9]", "", s)
            return neg, digits
        def fmt(neg: bool, s: str) -> str:
            if not s:
                return s
            parts = []
            while len(s) > 3:
                parts.insert(0, s[-3:])
                s = s[:-3]
            parts.insert(0, s)
            out = ",".join(parts)
            return f"({out})" if neg else out
        cols = list(df.columns)[1:]
        for _ in range(3):
            changed = False
            for i in range(len(cols)-1):
                c1, c2 = cols[i], cols[i+1]
                s1 = df[c1].astype(str).fillna("")
                s2 = df[c2].astype(str).fillna("")
                new1 = s1.copy(); new2 = s2.copy()
                for idx, (v1, v2) in enumerate(zip(s1, s2)):
                    a, b = v1.strip(), v2.strip()
                    if not a and not b:
                        continue
                    if (is_frag(a) and re.fullmatch(r"\d{1,3}$", b)) or \
                       (re.fullmatch(r"\d{1,3}$", a) and is_frag(b)) or \
                       (is_frag(a) and is_frag(b)):
                        n1, d1 = clean(a)
                        n2, d2 = clean(b)
                        neg = n1 or n2
                        join = d1 + d2
                        if join:
                            new1.iloc[idx] = fmt(neg, join)
                            new2.iloc[idx] = ""
                            changed = True
                if changed:
                    df[c1] = new1; df[c2] = new2
            if not changed:
                break
        return df

    # ---------------- Column de-dup / first-column naming / cleanup -----------------
    def _dedup_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        seen = {}
        new_cols = []
        for i, c in enumerate(df.columns):
            base = str(c).strip() or f"列{i+1}"
            if base.lower().startswith("unnamed:"):
                base = f"列{i+1}"
            if base in seen:
                seen[base] += 1
                base = f"{base}_{seen[base]}"
            else:
                seen[base] = 1
            new_cols.append(base)
        df.columns = new_cols
        if new_cols:
            df.rename(columns={new_cols[0]: "项目"}, inplace=True)
        return df

    def _keep_minimal_valid(self, df: pd.DataFrame) -> bool:
        if df.empty:
            return False
        if df.shape[1] <= 1:
            return False
        if df.shape[0] < 2:
            return False
        return True

    # ---------------- Secondary filtering -----------------
    def _secondary_filter_files(self, files: List[str]) -> List[str]:
        kept: List[str] = []
        removed_cnt = 0

        def has_long_empty_run(df: pd.DataFrame, threshold: int = EMPTY_RUN_THRESHOLD) -> bool:
            def is_empty_str(s: str) -> bool:
                t = str(s).strip()
                return (t == "")            
            mat = df.astype(str).applymap(is_empty_str).values
            for row in mat:
                run = 0
                for v in row:
                    run = run + 1 if v else 0
                    if run >= threshold:
                        return True
            for col in mat.T:
                run = 0
                for v in col:
                    run = run + 1 if v else 0
                    if run >= threshold:
                        return True
            return False

        for f in files:
            try:
                df = pd.read_csv(f, dtype=str, keep_default_na=False, encoding='utf-8-sig')
            except Exception as e:
                self.logger.warning(f"[HKEX 二次筛选] 读取失败，保留: {f}, err={e}")
                kept.append(f)
                continue
            if df.shape[1] < MIN_COLUMNS_THRESHOLD:
                try:
                    os.remove(f); removed_cnt += 1
                    self.logger.info(f"[HKEX 二次筛选] 删除列数过小(<{MIN_COLUMNS_THRESHOLD})文件: {os.path.basename(f)}")
                except Exception:
                    pass
                continue
            if df.shape[0] < 7:
                try:
                    os.remove(f); removed_cnt += 1
                    self.logger.info(f"[HKEX 二次筛选] 删除行数过小(<7)文件: {os.path.basename(f)}")
                except Exception:
                    pass
                continue
            long_cn_cells = 0
            for _, row in df.iterrows():
                for val in row:
                    if pd.isna(val):
                        continue
                    s = str(val)
                    cn_count = len(re.findall(r"[\u4e00-\u9fff]", s))
                    if cn_count > TEXT_HEAVY_LONG_CN_LEN:
                        long_cn_cells += 1
                        if long_cn_cells > TEXT_HEAVY_MAX_CELLS:
                            break
                if long_cn_cells > TEXT_HEAVY_MAX_CELLS:
                    break
            if long_cn_cells > TEXT_HEAVY_MAX_CELLS:
                try:
                    os.remove(f); removed_cnt += 1
                    self.logger.info(f"[HKEX 二次筛选] 删除文本过长单元格过多(>{TEXT_HEAVY_MAX_CELLS})文件: {os.path.basename(f)}")
                except Exception:
                    pass
                continue
            long_cell_cn = 0
            try:
                for _, row in df.iterrows():
                    for val in row:
                        if pd.isna(val):
                            continue
                        s = str(val)
                        s_no_space = re.sub(r"\s+", "", s)
                        cn_len = len(re.findall(r"[\u4e00-\u9fff]", s_no_space))
                        if cn_len >= LONG_CELL_CN_LEN:
                            long_cell_cn += 1
                            if long_cell_cn > LONG_CELL_CN_MAX_COUNT:
                                break
                    if long_cell_cn > LONG_CELL_CN_MAX_COUNT:
                        break
            except Exception as e:
                self.logger.debug(f"[HKEX 二次筛选] 中文长文本(> {LONG_CELL_CN_LEN})统计失败: {f}, err={e}")
            if long_cell_cn > LONG_CELL_CN_MAX_COUNT:
                try:
                    os.remove(f); removed_cnt += 1
                    self.logger.info(f"[HKEX 二次筛选] 删除存在超过{LONG_CELL_CN_MAX_COUNT}个> {LONG_CELL_CN_LEN}个中文字的单元格的文件: {os.path.basename(f)}")
                except Exception:
                    pass
                continue
            strict_long_cn = 0
            try:
                for _, row in df.iterrows():
                    for val in row:
                        s = str(val)
                        s_no_space = re.sub(r"\s+", "", s)
                        cn_len = len(re.findall(r"[\u4e00-\u9fff]", s_no_space))
                        if cn_len >= STRICT_LONG_CELL_CN_LEN:
                            strict_long_cn += 1
                            break
                    if strict_long_cn >= 1:
                        break
            except Exception:
                pass
            if strict_long_cn >= 1:
                try:
                    os.remove(f); removed_cnt += 1
                    self.logger.info(f"[HKEX 二次筛选] 删除存在>=1个>= {STRICT_LONG_CELL_CN_LEN}个中文字单元格的文件: {os.path.basename(f)}")
                except Exception:
                    pass
                continue
            try:
                total_cells = int(df.shape[0] * df.shape[1])
                if total_cells > 0:
                    empty_cnt = 0
                    for val in df.astype(str).values.ravel().tolist():
                        t = str(val).strip()
                        if t == "":
                            empty_cnt += 1
                    empty_ratio = empty_cnt / total_cells
                    if empty_ratio > 0.40:
                        try:
                            os.remove(f); removed_cnt += 1
                            self.logger.info(f"[HKEX 二次筛选] 删除空值比例过高({empty_ratio:.1%})文件: {os.path.basename(f)}")
                        except Exception:
                            pass
                        continue
            except Exception as e:
                self.logger.debug(f"[HKEX 二次筛选] 空值比例计算失败: {f}, err={e}")
            try:
                if has_long_empty_run(df, EMPTY_RUN_THRESHOLD):
                    if df.shape[1] < 4:
                        try:
                            os.remove(f); removed_cnt += 1
                            self.logger.info(f"[HKEX 二次筛选] 删除相连空值>= {EMPTY_RUN_THRESHOLD} 且列数({df.shape[1]})<7 的文件: {os.path.basename(f)}")
                        except Exception:
                            pass
                        continue
                    else:
                        self.logger.debug(f"[HKEX 二次筛选] 发现相连空值>= {EMPTY_RUN_THRESHOLD} 但列数>=4，保留: {os.path.basename(f)}")
            except Exception as e:
                self.logger.debug(f"[HKEX 二次筛选] 连续空值检测失败: {f}, err={e}")

            kept.append(f)
        if removed_cnt:
            self.logger.info(f"[HKEX 二次筛选] 已删除 {removed_cnt} 个文件，保留 {len(kept)} 个")
        return kept

    # ---------------- Tertiary selection: pick Top-2 per page into csv_selected -----------------
    def _score_hk_financial_layout(self, df: pd.DataFrame) -> float:
        """Enhanced scoring on top of _df_structure_score for HK financial main statements.
        Preference:
        - First column is a label column (mostly text, fewer numbers)
        - Two strong numeric/year columns (high numeric density; ideal total columns: 4~8)
        - A note column exists (header contains 附註/附注 or column dominated by short Chinese numerals)
        - Title keywords in first 3 rows (e.g., 綜合(全面)收益表/財務狀況表/現金流量表/權益變動表/財務表現概要/概覽)
        - Balanced non-empty distribution, low long-text ratio
        """
        if df is None or df.empty:
            return -1e9
        base = self._df_structure_score(df)
        rows, cols = df.shape
        score = base

        # 1) Column count preference
        if 4 <= cols <= 8:
            score += 12
        elif 3 <= cols <= 10:
            score += 6
        else:
            score -= 4

        # 2) Per-column numeric/text ratios
        num_pat = re.compile(r"^[\(（]?[-+]?\d[\d,，]*[\)）]?$")
        cn_pat = re.compile(r"[\u4e00-\u9fff]")
        numeric_ratio = []
        text_ratio = []
        non_empty_by_col = []
        long_text_cells = 0
        total_cells = rows * cols if rows and cols else 1
        for c in range(cols):
            col_vals = df.iloc[:, c].astype(str)
            total = 0; num_cnt = 0; txt_cnt = 0
            for v in col_vals:
                s = v.strip()
                if s == "":
                    continue
                total += 1
                if num_pat.match(s):
                    num_cnt += 1
                if cn_pat.search(s):
                    txt_cnt += 1
                # count long text
                if len(re.findall(r"[\u4e00-\u9fff]", s)) > 20 or len(s) > 40:
                    long_text_cells += 1
            non_empty_by_col.append(total)
            r_num = (num_cnt/total) if total else 0
            r_txt = (txt_cnt/total) if total else 0
            numeric_ratio.append(r_num)
            text_ratio.append(r_txt)

        long_text_ratio = long_text_cells / max(1, total_cells)
        if long_text_ratio > 0.25:
            score -= 15
        elif long_text_ratio > 0.15:
            score -= 8
        else:
            score += 5

        # 2.5) Row-wise checks to detect collapsed multi-line rows
        merged_like_rows = 0
        extreme_long_cells = 0
        non_empty_per_row = []
        for r in range(rows):
            row_vals = df.iloc[r].astype(str)
            non_empty = 0
            row_has_extreme = False
            for s in row_vals:
                s = s.strip()
                if s != "":
                    non_empty += 1
                    cn_len = len(re.findall(r"[\u4e00-\u9fff]", s))
                    if cn_len >= 60 or len(s) >= 120:
                        row_has_extreme = True
            non_empty_per_row.append(non_empty)
            # first cell extremely long while the rest of the row mostly empty -> merged lines symptom
            first_cell = str(row_vals.iloc[0]).strip()
            first_cn = len(re.findall(r"[\u4e00-\u9fff]", first_cell))
            if (first_cn >= 40 or len(first_cell) >= 80) and non_empty <= max(2, int(cols*0.4)):
                merged_like_rows += 1
            if row_has_extreme:
                extreme_long_cells += 1
        # penalties for extreme long cells
        if extreme_long_cells >= 1:
            score -= 18
        if extreme_long_cells >= 3:
            score -= 10
        # penalties by merged-row ratio
        if rows > 0:
            merged_ratio = merged_like_rows / rows
            if merged_ratio > 0.20:
                score -= 28
            elif merged_ratio > 0.10:
                score -= 16
            elif merged_ratio > 0.05:
                score -= 8
        # penalties for low median/average non-empty cells per row
        if non_empty_per_row:
            med = sorted(non_empty_per_row)[len(non_empty_per_row)//2]
            avg = sum(non_empty_per_row)/len(non_empty_per_row)
            if med < 2:
                score -= 12
            elif avg < 2.2:
                score -= 6

        # 3) First column behaves like labels (text >> numbers)
        if cols > 0:
            if text_ratio[0] >= 0.60 and numeric_ratio[0] <= 0.20:
                score += 14
            elif text_ratio[0] >= 0.45 and numeric_ratio[0] <= 0.30:
                score += 6
            else:
                score -= 4

        # 4) Strong year/value columns: take top-2 by numeric ratio
        sorted_num = sorted([(numeric_ratio[i], i) for i in range(cols)], reverse=True)
        top_nums = [i for _, i in sorted_num[:2]] if sorted_num else []
        strong_year_cols = 0
        for i in top_nums:
            cname = str(df.columns[i])
            if re.search(r"20\d{2}|港幣|百萬元|%", cname) or numeric_ratio[i] >= 0.70:
                strong_year_cols += 1
        if strong_year_cols == 2:
            score += 18
        elif strong_year_cols == 1:
            score += 8

        # 5) Detect note column by header or short Chinese numerals
        def looks_like_note_col(series: pd.Series) -> bool:
            total = 0; short_like = 0
            for v in series.astype(str):
                s = v.strip()
                if s == "":
                    continue
                total += 1
                if re.fullmatch(r"[一二三四五六七八九十百千〇零]{1,3}$|^\d{1,3}$", s):
                    short_like += 1
            return (total > 0 and short_like/total >= 0.5)

        has_note = False
        for i in range(cols):
            cname = str(df.columns[i])
            if ("附註" in cname) or ("附注" in cname) or looks_like_note_col(df.iloc[:, i]):
                has_note = True; break
        if has_note:
            score += 10

        # 6) Title keywords in first 3 rows
        head_text = "\n".join(" ".join(df.iloc[i].astype(str).tolist()) for i in range(min(3, rows)))
        if re.search(r"綜合(全面)?收益表|財務狀況表|現金流量表|權益變動表|財務表現概要|財務表現概覽", head_text):
            score += 20

        # 7) Column-wise non-empty balance (lower CV is better)
        import math
        if cols > 0:
            avg_col = sum(non_empty_by_col)/cols
            if avg_col > 0:
                var = sum((x-avg_col)**2 for x in non_empty_by_col)/cols
                cv = math.sqrt(var)/avg_col if avg_col else 1.0
                if cv < 0.30:
                    score += 8
                elif cv < 0.50:
                    score += 4
                else:
                    score -= 2
        return score

    def _third_select_best_per_page(self, files: List[str], csv_dir: str) -> List[str]:
        """Pick Top-2 CSVs per page by score and copy to csv_selected directory.
        Only copy; do not delete originals. If only one file exists, copy it as well."""
        try:
            base_dir = os.path.dirname(csv_dir)
            selected_dir = os.path.join(base_dir, 'csv_selected')
            os.makedirs(selected_dir, exist_ok=True)
            # group by page
            by_page: dict[int, List[str]] = {}
            for f in files:
                name = os.path.basename(f)
                m = re.search(r'page(\d+)', name)
                if not m:
                    continue
                p = int(m.group(1))
                by_page.setdefault(p, []).append(f)
            copied: List[str] = []
            for p, flist in by_page.items():
                scored: List[Tuple[float, str]] = []
                for f in flist:
                    try:
                        df = pd.read_csv(f, dtype=str, keep_default_na=False, encoding='utf-8-sig')
                        score = self._score_hk_financial_layout(df)
                    except Exception as e:
                        self.logger.debug(f"[HKEX 第三筛] 评分失败: {f}, err={e}")
                        score = -1e9
                    scored.append((score, f))
                if not scored:
                    continue
                scored.sort(key=lambda x: x[0], reverse=True)
                top_k = scored[:min(2, len(scored))]
                for best_score, best_file in top_k:
                    dst = os.path.join(selected_dir, os.path.basename(best_file))
                    try:
                        shutil.copy2(best_file, dst)
                        copied.append(dst)
                        self.logger.info(f"[HKEX 第三筛] Page {p} 选优复制: {os.path.basename(best_file)} → csv_selected (score={best_score:.2f})")
                    except Exception as e:
                        self.logger.warning(f"[HKEX 第三筛] 复制失败: {best_file} → {dst}, err={e}")
            return copied
        except Exception as e:
            self.logger.warning(f"[HKEX 第三筛] 失败: {e}")
            return []

    # ---------------- Convert a single table_data into DataFrame (with repairs) -----------------
    def _table_to_df(self, table_data: List[List[Any]]) -> Optional[pd.DataFrame]:
        non_empty = [r for r in table_data if any(c and str(c).strip() for c in r)]
        if not non_empty:
            return None
        max_cols = max(len(r) for r in non_empty)
        padded = [list(r) + [""]*(max_cols - len(r)) for r in non_empty]

        # Plan A: simple header (use first row as header)
        simple_cols = [str(c).strip() if c is not None else "" for c in padded[0]]
        if simple_cols:
            if simple_cols[0] == "" or simple_cols[0].lower().startswith("unnamed:"):
                simple_cols[0] = "项目"
        df_simple = pd.DataFrame(padded[1:], columns=simple_cols[:max_cols])

        # Plan B: reconstructed header
        cols_recon, data_rows_recon = self._reconstruct_headers(padded)
        df_recon = pd.DataFrame(data_rows_recon, columns=cols_recon[:max_cols]) if data_rows_recon else None

        # choose better structure by score
        score_simple = self._df_structure_score(df_simple)
        score_recon = self._df_structure_score(df_recon) if df_recon is not None else -1e9

        # only switch to recon if significantly better AND header hints exist
        use_recon = False
        if df_recon is not None:
            has_hint_in_cols = any(any(h in str(c) for h in HK_HEADER_HINTS) for c in df_recon.columns)
            if has_hint_in_cols and score_recon >= score_simple + 8:
                use_recon = True

        df = df_recon if use_recon else df_simple

        # multi-line explode & numeric repair
        df = self._dedup_columns(df)
        df = self._distribute_and_explode(df)
        df = self._merge_split_numbers(df)
        df = self._dedup_columns(df)

        # remove fully empty columns
        keep = []
        for i, c in enumerate(df.columns):
            s = df[c].astype(str).str.strip()
            if (s == "").all():
                continue
            keep.append(c)
        df = df[keep]
        return df if self._keep_minimal_valid(df) else None

    # ---------------- Main entry -----------------
    def extract_tables(self, pdf_path: str, pages: Optional[Set[int]] = None,
                       output_dir: str = "output",
                       output_format: str = "csv") -> List[str]:
        if output_format != "csv":
            raise ValueError("Output format must be 'csv'")

        pdf_dirs = get_pdf_output_dirs(output_dir, pdf_path)
        out_dir = pdf_dirs['csv']
        basename = Path(pdf_path).stem
        output_files: List[str] = []

        self.logger.info(f"[HKEX] Extracting tables from {pdf_path}")

        try:
            with pdfplumber.open(pdf_path) as pdf:
                pdf_pages = pdf.pages if not pages else [pdf.pages[i-1] for i in sorted(pages) if 0 < i <= len(pdf.pages)]
                for page in pdf_pages:
                    page_number = page.page_number + 1
                    all_tables: List[List[List[Any]]] = []

                    # detect page title (written to CSV first row; does not affect structure)
                    page_title = self._detect_short_title(page)

                    strategies = [
                        (None, "find_tables_default"),
                        ({}, "extract_tables_default"),
                        ({"vertical_strategy": "lines", "horizontal_strategy": "lines", "snap_tolerance": 5, "join_tolerance": 3, "edge_min_length": 3}, "find_tables_lines"),
                        ({"vertical_strategy": "text", "horizontal_strategy": "text", "snap_tolerance": 5, "join_tolerance": 3, "text_tolerance": 3, "intersection_tolerance": 3}, "extract_text_text"),
                        ({"vertical_strategy": "text", "horizontal_strategy": "lines", "snap_tolerance": 8, "join_tolerance": 5, "text_tolerance": 8, "intersection_tolerance": 5}, "find_tables_mixed"),
                        ({"vertical_strategy": "lines", "horizontal_strategy": "lines", "snap_tolerance": 5, "join_tolerance": 3, "edge_min_length": 3}, "extract_lines"),
                        ({"vertical_strategy": "lines", "horizontal_strategy": "text", "snap_tolerance": 5, "join_tolerance": 3, "text_tolerance": 3, "intersection_tolerance": 3}, "extract_mixed"),
                        ({"vertical_strategy": "text", "horizontal_strategy": "text", "snap_tolerance": 10, "join_tolerance": 5, "text_tolerance": 5, "intersection_tolerance": 5, "min_words_vertical": 1, "min_words_horizontal": 1}, "extract_loose_text"),
                    ]

                    try:
                        ft = page.find_tables()
                        for obj in (ft or []):
                            try:
                                data = obj.extract()
                                if data:
                                    all_tables.append(data)
                            except Exception:
                                pass
                    except Exception:
                        pass

                    for ts, tag in strategies[1:]:
                        try:
                            if tag.startswith("extract_"):
                                tables = page.extract_tables(table_settings=ts) if ts else page.extract_tables()
                                if tables:
                                    all_tables.extend(tables)
                            else:
                                ft = page.find_tables(table_settings=ts)
                                if ft:
                                    for obj in ft:
                                        try:
                                            data = obj.extract()
                                            if data:
                                                all_tables.append(data)
                                        except Exception:
                                            pass
                        except Exception:
                            continue

                    def signature(tb: List[List[Any]]) -> Optional[Tuple]:
                        if not tb:
                            return None
                        sig = []
                        for r in tb[:min(15, len(tb))]:
                            if not r:
                                continue
                            first_text = None
                            for cell in r:
                                if cell and str(cell).strip() and not re.match(r"^[\d,，\(（\)）\s\-–—\.]+$", str(cell).strip()):
                                    first_text = str(cell).strip()[:50]
                                    break
                            nums = re.findall(r"\d+", " ".join(str(c) for c in r if c))
                            comb = []
                            i = 0
                            while i < len(nums):
                                n = nums[i]
                                if i < len(nums)-1 and len(n) <= 3 and len(nums[i+1]) <= 3 and len(n+nums[i+1]) >= 4:
                                    comb.append(n+nums[i+1]); i += 2
                                else:
                                    comb.append(n); i += 1
                            sig.append((first_text or "", tuple(comb[:8])))
                        return tuple(sig)

                    groups: dict = {}
                    for tb in all_tables:
                        sig = signature(tb)
                        if not sig:
                            continue
                        groups.setdefault(sig, []).append(tb)

                    unique_tables: List[List[List[Any]]] = []
                    for sig, tbs in groups.items():
                        best = max(tbs, key=lambda x: len(x))
                        unique_tables.append(best)

                    if not unique_tables:
                        self.logger.info(f"[HKEX] Page {page_number}: no tables found")
                        continue

                    self.logger.info(f"[HKEX] Page {page_number}: {len(unique_tables)} unique tables")

                    for idx, tb in enumerate(unique_tables, start=1):
                        try:
                            if not self._is_financial_hk(tb):
                                has_digit = any(re.search(r"\d", str(c)) for r in tb for c in (r or []))
                                if not has_digit:
                                    continue
                            df = self._table_to_df(tb)
                            if df is None or df.empty:
                                continue
                            if df.shape[1] <= 1 or df.shape[0] < 2:
                                continue
                            # write short title (if any) into the first row (does not change header)
                            if page_title and list(df.columns):
                                try:
                                    meta = pd.DataFrame([{df.columns[0]: f"{page_title}"}], columns=df.columns)
                                    df = pd.concat([meta, df], ignore_index=True)
                                except Exception:
                                    pass
                            out_name = f"{sanitize_filename(basename)}_page{page_number}_table{idx}.csv"
                            out_path = os.path.join(out_dir, out_name)
                            df.to_csv(out_path, index=False, encoding='utf-8-sig')
                            output_files.append(out_path)
                            self.logger.info(f"[HKEX] Saved table -> {out_name}")
                        except Exception as e:
                            self.logger.warning(f"[HKEX] Page {page_number} table {idx} build failed: {e}")

        except Exception as e:
            self.logger.error(f"[HKEX] Error: {e}")
            raise

        try:
            pdf_csv_dir = out_dir
            all_pdf_csv = []
            for fname in os.listdir(pdf_csv_dir):
                if fname.lower().endswith('.csv') and fname.startswith(sanitize_filename(basename) + '_'):
                    all_pdf_csv.append(os.path.join(pdf_csv_dir, fname))
            if all_pdf_csv:
                kept_after_filter = set(self._secondary_filter_files(all_pdf_csv))
                output_files = [f for f in output_files if f in kept_after_filter and os.path.exists(f)]
                # third selection: copy Top-2 per page into csv_selected
                self._third_select_best_per_page(list(kept_after_filter), pdf_csv_dir)
        except Exception as e:
            self.logger.warning(f"[HKEX 二次筛选] 目录级扫描失败，退回只筛选本次产出: {e}")
            if output_files:
                output_files = self._secondary_filter_files(output_files)
                # still try a best-effort third selection
                try:
                    self._third_select_best_per_page(output_files, out_dir)
                except Exception:
                    pass

        if not output_files:
            self.logger.info("[HKEX] no tables after HK pipeline")
        return output_files
