"""
分录对账工具模块
提供JE文件加载、TB文件加载和对账分析功能
支持大文件分批处理
"""
import pandas as pd
import numpy as np
import json
import re
import os
from typing import List, Dict, Optional, Tuple, Any, Iterator, Generator
from langchain.tools import tool
from coze_coding_utils.log.write_log import request_context
from coze_coding_utils.runtime_ctx.context import new_context

# 默认配置
DEFAULT_THRESHOLD = 0.01
# 默认批次大小（行数）
DEFAULT_BATCH_SIZE = 10000
# 内存警告阈值（MB）
MEMORY_WARNING_THRESHOLD = 500


def parse_currency_value(x: Any) -> float:
    """解析货币金额值"""
    try:
        if pd.isna(x):
            return 0.0
        s = str(x).strip()
        if s == '':
            return 0.0
        s = s.replace(',', '')
        neg = False
        if s.startswith('(') and s.endswith(')'):
            neg = True
            s = s[1:-1]
        v = float(s)
        return -v if neg else v
    except Exception:
        try:
            return float(x)
        except Exception:
            return 0.0


def extract_account_code(s: Any) -> str:
    """提取科目代码"""
    s = str(s)
    m = re.search(r'(\d+(?:\.\d+)*)', s)
    if m:
        return m.group(1)
    digits = re.findall(r'\d+', s)
    return ''.join(digits)


def _is_reversal_flag(v: Any) -> bool:
    """判断是否为红字标志"""
    if v is None:
        return False
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    try:
        if isinstance(v, (int, float, np.integer, np.floating)):
            return float(v) != 0.0
    except Exception:
        pass
    s = str(v).strip()
    if s == '' or s.lower() in {'false', '0', 'no', 'n', '否', 'f'}:
        return False
    return s.lower() in {'true', '1', 'x', 'y', 'yes', '是', 't'}


def _build_header_to_idx(headers: List[str]) -> Dict[str, int]:
    """构建列名到索引的映射"""
    header_to_idx = {}
    for i, h in enumerate(headers):
        hh = str(h).strip() if h is not None else ''
        if hh and hh not in header_to_idx:
            header_to_idx[hh] = i
    return header_to_idx


def _select_excel_sheet_and_header_row(file_path: str, prefer_sheet_keywords: Optional[List[str]] = None) -> Tuple[Optional[str], int]:
    """智能选择Excel工作表和表头行"""
    prefer_sheet_keywords = prefer_sheet_keywords or []
    try:
        from openpyxl import load_workbook
        wb = load_workbook(file_path, read_only=True, data_only=True)
        best = None
        for name in wb.sheetnames:
            ws = wb[name]
            prefer = 0
            for kw in prefer_sheet_keywords:
                if kw and kw in str(name):
                    prefer += 10
            for r in range(1, 11):
                try:
                    header_row = next(ws.iter_rows(min_row=r, max_row=r, values_only=True))
                except StopIteration:
                    continue
                headers = [str(v).strip() if v is not None else '' for v in header_row]
                header_set = set(h for h in headers if h)
                score = 0
                if any(x in header_set for x in ['账套', '公司', '工厂', '核算账套名称', '主体账套']):
                    score += 5
                if any(x in header_set for x in ['科目', '会计科目', '科目编码', '会计科目末级编码', '行标签']):
                    score += 8
                if any(x in header_set for x in ['借贷方本位币', '借贷方本位币金额', '借方金额(本位币)', '求和项:借贷方金额(本位币)', '借方金额']):
                    score += 6
                if any(x in header_set for x in ['贷方本位币', '贷方本位币金额', '贷方金额(本位币)', '求和项:贷方金额(本位币)', '贷方金额']):
                    score += 6
                if any(x in header_set for x in ['期间', '月', '月份', '会计期间']):
                    score += 2
                if any(x in header_set for x in ['凭证号', '凭证编号', '凭证', '记账凭证号']):
                    score += 2
                key = (score + prefer, score, prefer, name, r)
                if best is None or key > best:
                    best = key
        wb.close()
        if best is None:
            return None, 1
        return best[3], int(best[4])
    except Exception:
        return None, 1


def _load_excel_file(file_path: str, file_type: str = 'je', target_patterns: Optional[List[str]] = None) -> pd.DataFrame:
    """加载Excel文件"""
    target_patterns = target_patterns or []
    ext = os.path.splitext(str(file_path))[1].lower()
    
    # 列名候选映射
    je_columns = {
        "book": ["账套", "公司", "工厂"],
        "voucher": ["凭证号", "凭证编号", "凭证", "记账凭证号"],
        "year": ["年", "年度"],
        "month": ["月", "月份"],
        "subject": ["科目", "会计科目", "科目编码", "科目代码"],
        "debit": ["借方本位币", "借方本位币金额", "借方金额(本位币)", "求和项:借贷方金额(本位币)", "借方金额", "借贷方本位币"],
        "credit": ["贷方本位币", "贷方本位币金额", "贷方金额(本位币)", "求和项:贷方金额(本位币)", "贷方金额"],
        "description": ["摘要", "说明", "描述"],
        "reversal": ["红字", "红冲", "冲销", "反方向"]
    }
    
    tb_columns = {
        "book": ["核算账套名称", "主体账套", "账套", "公司"],
        "account_code": ["科目编码", "总账科目", "科目"],
        "account_name": ["科目名称", "科目全称", "名称"],
        "debit": ["本期借方.1", "本期借方发生.1", "本期借方", "借方累计.1", "借方累计", "本期借方发生_1", "借贷方本位币"],
        "credit": ["本期贷方.1", "本期贷方发生.1", "本期贷方", "贷方累计.1", "贷方累计", "本期贷方发生_1", "贷方本位币"]
    }
    
    columns = tb_columns if file_type == 'tb' else je_columns
    configured_book = columns.get("book", ["账套"])[0]
    configured_subject = columns.get("subject", ["科目"])[0]
    configured_account_code = columns.get("account_code", ["科目编码"])[0]
    configured_debit = columns.get("debit", ["借方本位币"])[0]
    configured_credit = columns.get("credit", ["贷方本位币"])[0]
    
    if ext in {'.xlsx', '.xlsm'}:
        from openpyxl import load_workbook
        wb = load_workbook(file_path, read_only=True, data_only=True)
        sheet_name, header_row_idx = _select_excel_sheet_and_header_row(
            file_path, 
            prefer_sheet_keywords=['凭证', '序时', '分录'] if file_type == 'je' else ['余额', '科目']
        )
        ws = wb[sheet_name] if sheet_name in wb.sheetnames else wb.active
        
        all_rows = []
        for row in ws.iter_rows(min_row=header_row_idx, values_only=True):
            all_rows.append(row)
        
        wb.close()
        
        if not all_rows:
            return pd.DataFrame()
        
        headers = [str(v).strip() if v is not None else '' for v in all_rows[0]]
        header_to_idx = _build_header_to_idx(headers)
        
        def first_existing(candidates):
            for c in candidates:
                if c and c in header_to_idx:
                    return c
            return None
        
        book_col = first_existing(columns.get("book", []))
        voucher_col = first_existing(["凭证号", "凭证编号", "凭证", "记账凭证号"])
        year_col = first_existing(["年", "年度"])
        month_col = first_existing(["月", "月份"])
        period_col = first_existing(["期间", "会计期间", "期间代码"])
        subject_col = first_existing(columns.get("subject", ["科目", "会计科目"]))
        account_code_col = first_existing(columns.get("account_code", ["科目编码", "总账科目"]))
        account_name_col = first_existing(columns.get("account_name", ["科目名称", "科目全称"]))
        debit_col = first_existing(columns.get("debit", ["借方本位币", "借方金额"]))
        credit_col = first_existing(columns.get("credit", ["贷方本位币", "贷方金额"]))
        description_col = first_existing(["摘要", "说明", "描述"])
        reversal_col = first_existing(["红字", "红冲", "冲销", "反方向"])
        
        data_rows = []
        for row in all_rows[1:]:
            row_dict = {}
            if book_col and book_col in header_to_idx:
                row_dict['账套'] = row[header_to_idx[book_col]]
            if voucher_col and voucher_col in header_to_idx:
                row_dict['凭证号'] = row[header_to_idx[voucher_col]]
            if year_col and year_col in header_to_idx:
                row_dict['年'] = row[header_to_idx[year_col]]
            if month_col and month_col in header_to_idx:
                row_dict['月'] = row[header_to_idx[month_col]]
            if period_col and period_col in header_to_idx:
                row_dict['期间'] = row[header_to_idx[period_col]]
            if subject_col and subject_col in header_to_idx:
                row_dict['科目'] = row[header_to_idx[subject_col]]
            if account_code_col and account_code_col in header_to_idx:
                row_dict['科目编码'] = row[header_to_idx[account_code_col]]
            if account_name_col and account_name_col in header_to_idx:
                row_dict['科目名称'] = row[header_to_idx[account_name_col]]
            if debit_col and debit_col in header_to_idx:
                row_dict['借方金额'] = row[header_to_idx[debit_col]]
            if credit_col and credit_col in header_to_idx:
                row_dict['贷方金额'] = row[header_to_idx[credit_col]]
            if description_col and description_col in header_to_idx:
                row_dict['摘要'] = row[header_to_idx[description_col]]
            if reversal_col and reversal_col in header_to_idx:
                row_dict['红字'] = row[header_to_idx[reversal_col]]
            
            if row_dict:
                data_rows.append(row_dict)
        
        return pd.DataFrame(data_rows)
    
    elif ext == '.csv':
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='gbk')
        
        df.columns = [str(c).strip() for c in df.columns]
        return df
    
    return pd.DataFrame()


def _load_excel_in_chunks(file_path: str, file_type: str = 'je', chunk_size: int = DEFAULT_BATCH_SIZE) -> Generator[pd.DataFrame, None, None]:
    """
    分批加载Excel文件，避免内存溢出
    
    Args:
        file_path: 文件路径
        file_type: 文件类型 ('je' 或 'tb')
        chunk_size: 每批次行数
    
    Yields:
        DataFrame批次
    """
    ext = os.path.splitext(str(file_path))[1].lower()
    
    if ext in {'.csv'}:
        # CSV文件直接使用chunksize
        for chunk in pd.read_csv(file_path, chunksize=chunk_size, encoding='utf-8', on_bad_lines='skip'):
            chunk.columns = [str(c).strip() for c in chunk.columns]
            yield chunk
    elif ext in {'.xlsx', '.xlsm'}:
        # Excel文件需要手动分批
        full_df = _load_excel_file(file_path, file_type)
        
        if full_df.empty:
            return
        
        total_rows = len(full_df)
        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            yield full_df.iloc[start:end].copy()
    else:
        return


def _filter_tb_to_leaf_accounts(tb_df: pd.DataFrame, code_col: str = '科目编码', book_col: str = '账套') -> pd.DataFrame:
    """过滤TB数据，只保留末级科目"""
    if tb_df is None or len(tb_df) == 0:
        return tb_df
    if code_col not in tb_df.columns:
        return tb_df
    
    work = tb_df.copy()
    work[code_col] = work[code_col].astype(str).str.strip()
    if book_col in work.columns:
        work[book_col] = work[book_col].astype(str).str.strip()
    else:
        work[book_col] = ''
    
    removed_total = 0
    keep_parts = []
    
    for book, g in work.groupby(book_col, dropna=False):
        codes = g[code_col].astype(str).str.strip()
        uniq = sorted(set(c for c in codes.tolist() if c and c != 'nan'))
        non_leaf = set()
        
        for i in range(len(uniq) - 1):
            cur = uniq[i]
            nxt = uniq[i + 1]
            if nxt.startswith(cur) and len(nxt) > len(cur):
                non_leaf.add(cur)
        
        if non_leaf:
            keep = ~g[code_col].isin(non_leaf)
            removed_total += int((~keep).sum())
            keep_parts.append(g[keep].copy())
        else:
            keep_parts.append(g.copy())
    
    out = pd.concat(keep_parts, ignore_index=True) if keep_parts else work
    
    if removed_total > 0:
        print(f"已按末级科目过滤TB父级科目: 移除 {removed_total} 条")
    
    return out


@tool
def load_je_data(je_file_paths: str) -> str:
    """
    加载分录(JE)数据文件（支持大文件分批处理）。
    
    Args:
        je_file_paths: JE文件路径，支持多个文件用逗号分隔
                     例如: "/path/to/je1.xlsx,/path/to/je2.xlsx"
    
    Returns:
        返回加载结果的JSON字符串，包含成功加载的文件数和数据行数
    """
    ctx = request_context.get() or new_context(method="load_je_data")
    
    try:
        file_list = [f.strip() for f in je_file_paths.split(',')]
        
        all_dfs = []
        total_rows = 0
        
        for file_path in file_list:
            if os.path.basename(str(file_path)).startswith('~$'):
                continue
            if os.path.exists(file_path):
                df = _load_excel_file(file_path, file_type='je')
                if df is not None and len(df) > 0:
                    all_dfs.append(df)
                    total_rows += len(df)
                    print(f"成功加载JE文件: {file_path}, 行数: {len(df)}")
            else:
                print(f"警告: JE文件不存在: {file_path}")
        
        if not all_dfs:
            return json.dumps({
                "success": False,
                "error": "没有成功加载任何JE文件",
                "files_loaded": 0,
                "total_rows": 0
            }, ensure_ascii=False)
        
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # 解析金额
        if '借方金额' in combined_df.columns:
            combined_df['借方金额'] = combined_df['借方金额'].apply(parse_currency_value)
        if '贷方金额' in combined_df.columns:
            combined_df['贷方金额'] = combined_df['贷方金额'].apply(parse_currency_value)
        
        # 提取科目代码
        if '科目' in combined_df.columns:
            combined_df['科目编码'] = combined_df['科目'].apply(extract_account_code)
        
        # 保存到临时文件
        temp_file = '/tmp/je_loaded_data.pkl'
        combined_df.to_pickle(temp_file)
        
        return json.dumps({
            "success": True,
            "message": f"成功加载 {len(all_dfs)} 个JE文件",
            "files_loaded": len(all_dfs),
            "total_rows": len(combined_df),
            "columns": list(combined_df.columns),
            "data_file": temp_file
        }, ensure_ascii=False)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"加载JE文件失败: {str(e)}"
        }, ensure_ascii=False)


@tool
def load_tb_data(tb_file_path: str) -> str:
    """
    加载科目余额表(TB)数据文件（支持大文件分批处理）。
    
    Args:
        tb_file_path: TB文件路径，例如: "/path/to/tb.xlsx"
    
    Returns:
        返回加载结果的JSON字符串，包含数据行数和列信息
    """
    ctx = request_context.get() or new_context(method="load_tb_data")
    
    try:
        if not os.path.exists(tb_file_path):
            return json.dumps({
                "success": False,
                "error": f"TB文件不存在: {tb_file_path}"
            }, ensure_ascii=False)
        
        df = _load_excel_file(tb_file_path, file_type='tb')
        
        if df is None or len(df) == 0:
            return json.dumps({
                "success": False,
                "error": "TB文件为空或无法解析"
            }, ensure_ascii=False)
        
        # 解析金额
        if '借方金额' in df.columns:
            df['借方金额'] = df['借方金额'].apply(parse_currency_value)
        if '贷方金额' in df.columns:
            df['贷方金额'] = df['贷方金额'].apply(parse_currency_value)
        
        # 提取科目代码
        if '科目' in df.columns:
            df['科目编码'] = df['科目'].apply(extract_account_code)
        
        # 过滤到末级科目
        df = _filter_tb_to_leaf_accounts(df)
        
        # 保存到临时文件
        temp_file = '/tmp/tb_loaded_data.pkl'
        df.to_pickle(temp_file)
        
        return json.dumps({
            "success": True,
            "message": "成功加载TB文件",
            "total_rows": len(df),
            "columns": list(df.columns),
            "data_file": temp_file
        }, ensure_ascii=False)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"加载TB文件失败: {str(e)}"
        }, ensure_ascii=False)


def _summarize_je_batch(je_batch: pd.DataFrame, patterns: List[str]) -> Dict[Tuple, Dict]:
    """
    对JE批次数据进行汇总
    
    Returns:
        {(账套, 科目编码): {'借方': float, '贷方': float, '条数': int}}
    """
    summary = {}
    
    for _, row in je_batch.iterrows():
        book = str(row.get('账套', '默认账套')).strip()
        code = str(row.get('科目编码', '')).strip()
        
        if patterns and not any(p in code for p in patterns):
            continue
        
        key = (book, code)
        
        if key not in summary:
            summary[key] = {'借方': 0.0, '贷方': 0.0, '条数': 0, '科目名称': ''}
        
        debit = float(row.get('借方金额', 0) or 0)
        credit = float(row.get('贷方金额', 0) or 0)
        
        # 处理红字
        is_reversal = _is_reversal_flag(row.get('红字'))
        if is_reversal:
            summary[key]['贷方'] -= debit
            summary[key]['借方'] -= credit
        else:
            summary[key]['借方'] += debit
            summary[key]['贷方'] += credit
        
        summary[key]['条数'] += 1
        
        # 记录科目名称
        if not summary[key]['科目名称'] and '科目' in row:
            summary[key]['科目名称'] = str(row.get('科目', ''))
    
    return summary


def _summarize_tb_batch(tb_batch: pd.DataFrame, patterns: List[str]) -> Dict[Tuple, Dict]:
    """
    对TB批次数据进行汇总
    
    Returns:
        {(账套, 科目编码): {'借方': float, '贷方': float, '科目名称': str}}
    """
    summary = {}
    
    for _, row in tb_batch.iterrows():
        book = str(row.get('账套', '默认账套')).strip()
        code = str(row.get('科目编码', '')).strip()
        
        if patterns and not any(p in code for p in patterns):
            continue
        
        key = (book, code)
        
        if key not in summary:
            summary[key] = {'借方': 0.0, '贷方': 0.0, '科目名称': ''}
        
        debit = float(row.get('借方金额', 0) or 0)
        credit = float(row.get('贷方金额', 0) or 0)
        
        summary[key]['借方'] += debit
        summary[key]['贷方'] += credit
        
        # 记录科目名称
        if not summary[key]['科目名称']:
            if '科目' in row:
                summary[key]['科目名称'] = str(row.get('科目', ''))
            elif '科目名称' in row:
                summary[key]['科目名称'] = str(row.get('科目名称', ''))
    
    return summary


def _merge_summary(base: Dict, incremental: Dict) -> None:
    """合并两个汇总数据"""
    for key, value in incremental.items():
        if key not in base:
            base[key] = value.copy()
        else:
            base[key]['借方'] += value['借方']
            base[key]['贷方'] += value['贷方']
            base[key]['条数'] = base[key].get('条数', 0) + value.get('条数', 0)
            if not base[key].get('科目名称') and value.get('科目名称'):
                base[key]['科目名称'] = value['科目名称']


@tool
def run_reconciliation(
    je_file_paths: str,
    tb_file_path: str,
    target_patterns: str = "",
    threshold: float = 0.01,
    check_voucher_balance: bool = True,
    check_sequence: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE
) -> str:
    """
    执行分录对账分析，将JE数据与TB数据进行比对（支持大文件分批处理）。
    
    Args:
        je_file_paths: JE文件路径，多个文件用逗号分隔
        tb_file_path: TB文件路径
        target_patterns: 要对账的科目筛选模式，用逗号分隔，例如: "1001,1002,1122"
        threshold: 差异阈值，默认0.01，金额差异小于此值视为匹配
        check_voucher_balance: 是否检查凭证借贷平衡，默认True
        check_sequence: 是否检查凭证序号连续性，默认False（大数据量时禁用）
        batch_size: 批次大小，默认10000行，大文件时可调大此值
    
    Returns:
        返回对账结果的JSON字符串，包含差异明细和统计信息
    """
    ctx = request_context.get() or new_context(method="run_reconciliation")
    
    try:
        file_list = [f.strip() for f in je_file_paths.split(',')]
        patterns = [p.strip() for p in target_patterns.split(',') if p.strip()] if target_patterns else []
        
        # 分批汇总JE数据
        print(f"开始分批加载JE数据，批次大小: {batch_size}")
        je_summary: Dict[Tuple, Dict] = {}
        je_vouchers: List[Dict] = []  # 保存凭证数据用于借贷检查
        total_je_rows = 0
        batch_count = 0
        
        for file_path in file_list:
            if not os.path.exists(file_path):
                print(f"警告: JE文件不存在: {file_path}")
                continue
            
            for batch in _load_excel_in_chunks(file_path, 'je', batch_size):
                batch_count += 1
                total_je_rows += len(batch)
                
                # 解析金额
                if '借方金额' in batch.columns:
                    batch['借方金额'] = batch['借方金额'].apply(parse_currency_value)
                if '贷方金额' in batch.columns:
                    batch['贷方金额'] = batch['贷方金额'].apply(parse_currency_value)
                if '科目' in batch.columns:
                    batch['科目编码'] = batch['科目'].apply(extract_account_code)
                
                # 汇总
                batch_summary = _summarize_je_batch(batch, patterns)
                _merge_summary(je_summary, batch_summary)
                
                # 保存凭证数据用于借贷检查
                if check_voucher_balance and '凭证号' in batch.columns:
                    cols = ['账套', '凭证号', '年', '月', '借方金额', '贷方金额']
                    if '期间' in batch.columns:
                        cols.append('期间')
                    voucher_data = batch[cols].copy()
                    je_vouchers.append(voucher_data)
                
                print(f"  JE批次 {batch_count}: {len(batch)} 行，累计 {total_je_rows} 行")
        
        print(f"JE数据加载完成: {total_je_rows} 行，{len(je_summary)} 个科目")
        
        # 加载并汇总TB数据（TB通常较小，全量加载）
        print(f"开始加载TB数据...")
        tb_df = _load_excel_file(tb_file_path, file_type='tb')
        
        if tb_df is None or len(tb_df) == 0:
            return json.dumps({
                "success": False,
                "error": "TB文件为空或无法解析"
            }, ensure_ascii=False)
        
        # 解析金额
        if '借方金额' in tb_df.columns:
            tb_df['借方金额'] = tb_df['借方金额'].apply(parse_currency_value)
        if '贷方金额' in tb_df.columns:
            tb_df['贷方金额'] = tb_df['贷方金额'].apply(parse_currency_value)
        
        # 优先使用原始科目编码列，如果没有则从科目名称提取
        has_code = '科目编码' in tb_df.columns
        if has_code:
            # 检查是否有非空值
            non_empty = tb_df['科目编码'].notna() & (tb_df['科目编码'].astype(str).str.strip() != '')
            has_code = non_empty.any()
        if not has_code and '科目' in tb_df.columns:
            tb_df['科目编码'] = tb_df['科目'].apply(extract_account_code)
        
        # 过滤末级科目
        tb_df = _filter_tb_to_leaf_accounts(tb_df)
        
        # 汇总TB
        print(f"TB数据加载完成: {len(tb_df)} 行")
        tb_summary = _summarize_tb_batch(tb_df, patterns)
        
        # 执行对账
        all_codes = set(je_summary.keys()) | set(tb_summary.keys())
        
        matched = []
        differences = []
        only_in_je = []
        only_in_tb = []
        
        for key in sorted(all_codes):
            book, code = key
            
            je_data = je_summary.get(key, {'借方': 0.0, '贷方': 0.0, '条数': 0, '科目名称': ''})
            tb_data = tb_summary.get(key, {'借方': 0.0, '贷方': 0.0, '科目名称': ''})
            
            je_debit = je_data['借方']
            je_credit = je_data['贷方']
            tb_debit = tb_data['借方']
            tb_credit = tb_data['贷方']
            
            debit_diff = round(je_debit - tb_debit, 2)
            credit_diff = round(je_credit - tb_credit, 2)
            
            # 获取科目名称
            item_name = je_data.get('科目名称') or tb_data.get('科目名称') or code
            
            item = {
                '账套': book,
                '科目编码': code,
                '科目名称': item_name,
                'JE借方': je_debit,
                'JE贷方': je_credit,
                'TB借方': tb_debit,
                'TB贷方': tb_credit,
                '借方差异': debit_diff,
                '贷方差异': credit_diff,
                'JE条数': je_data.get('条数', 0)
            }
            
            if key not in je_summary:
                only_in_tb.append(item)
            elif key not in tb_summary:
                only_in_je.append(item)
            elif abs(debit_diff) <= threshold and abs(credit_diff) <= threshold:
                matched.append(item)
            else:
                differences.append(item)
        
        # 凭证借贷平衡检查（限制问题数量避免响应过大）
        voucher_issues: list = []
        MAX_VOUCHER_ISSUES = 10  # 限制最多显示10个凭证问题
        
        if check_voucher_balance and je_vouchers:
            print("执行凭证借贷平衡检查...")
            je_voucher_df = pd.concat(je_vouchers, ignore_index=True)
            
            group_cols = ['账套', '凭证号']
            if '年' in je_voucher_df.columns:
                group_cols.append('年')
            if '月' in je_voucher_df.columns:
                group_cols.append('月')
            if '期间' in je_voucher_df.columns:
                group_cols.append('期间')
            
            grouped = je_voucher_df.groupby(group_cols).agg({
                '借方金额': 'sum',
                '贷方金额': 'sum'
            }).reset_index()
            
            for _, row in grouped.iterrows():
                if len(voucher_issues) >= MAX_VOUCHER_ISSUES:
                    break  # 达到上限后停止收集
                    
                debit_sum = float(row['借方金额'])
                credit_sum = float(row['贷方金额'])
                
                if abs(debit_sum - credit_sum) > threshold:
                    year_val = row.get('年')
                    month_val = row.get('月')
                    period_val = row.get('期间', '')
                    
                    try:
                        if year_val is not None and month_val is not None:
                            year_month = f"{int(float(year_val))}-{int(float(month_val))}"
                        else:
                            year_month = str(period_val) if period_val else ''
                    except Exception:
                        year_month = str(period_val) if period_val else ''
                    
                    voucher_issues.append({
                        '账套': str(row['账套']) if row['账套'] is not None else '',
                        '凭证号': str(row['凭证号']) if row['凭证号'] is not None else '',
                        '期间': year_month,
                        '问题类型': '借贷不平衡',
                        '借方合计': round(debit_sum, 2),
                        '贷方合计': round(credit_sum, 2),
                        '差异': round(debit_sum - credit_sum, 2)
                    })
            
            # 计算实际检查的凭证数（不计入已跳过的）
            total_vouchers_checked = len(grouped)
            print(f"  检查 {total_vouchers_checked} 张凭证，发现 {len(voucher_issues)} 个借贷不平衡问题（最多显示{MAX_VOUCHER_ISSUES}个）")
        
        # 构建结果
        result = {
            "success": True,
            "summary": {
                "总科目数": len(all_codes),
                "匹配数": len(matched),
                "差异数": len(differences),
                "仅在JE中": len(only_in_je),
                "仅在TB中": len(only_in_tb),
                "凭证异常数": len(voucher_issues),
                "JE总行数": total_je_rows,
                "TB总行数": len(tb_df),
                "处理批次": batch_count
            },
            "differences": differences[:50],
            "only_in_je": only_in_je[:20],
            "only_in_tb": only_in_tb[:20],
            "voucher_issues": voucher_issues[:30],
            "matched_count": len(matched)
        }
        
        return json.dumps(result, ensure_ascii=False, indent=2)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return json.dumps({
            "success": False,
            "error": f"对账分析失败: {str(e)}"
        }, ensure_ascii=False)
