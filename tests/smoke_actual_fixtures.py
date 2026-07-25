#!/usr/bin/env python3
"""Smoke test reconciliation_tool with local JE/TB Excel fixtures.

This script intentionally accepts local file paths instead of committing fixture data.
It stubs the LangChain decorator so the parsing and reconciliation logic can be checked
with only pandas/openpyxl/numpy installed.
"""

from __future__ import annotations

import argparse
import json
import sys
import types
from pathlib import Path


def _install_import_stubs() -> None:
    langchain_mod = types.ModuleType("langchain")
    tools_mod = types.ModuleType("langchain.tools")

    def tool(fn=None, *args, **kwargs):
        if fn is None:
            return lambda f: f
        return fn

    tools_mod.tool = tool
    sys.modules.setdefault("langchain", langchain_mod)
    sys.modules.setdefault("langchain.tools", tools_mod)



def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--je", required=True, help="Path to journal-entry Excel fixture")
    parser.add_argument("--tb", required=True, help="Path to trial-balance Excel fixture")
    args = parser.parse_args()

    je = Path(args.je)
    tb = Path(args.tb)
    if not je.exists():
        raise FileNotFoundError(je)
    if not tb.exists():
        raise FileNotFoundError(tb)

    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "src"))
    _install_import_stubs()

    from tools.reconciliation_tool import load_je_data, load_tb_data, run_reconciliation

    je_res = json.loads(load_je_data(str(je)))
    assert je_res["success"], je_res
    assert je_res["total_rows"] > 0, je_res

    tb_res = json.loads(load_tb_data(str(tb)))
    assert tb_res["success"], tb_res
    assert tb_res["total_rows"] > 0, tb_res

    recon = json.loads(run_reconciliation(str(je), str(tb), batch_size=10000, check_voucher_balance=True))
    assert recon["success"], recon
    assert "preview" not in recon, "tool output should stay ultra compact by default"
    assert recon["summary"]["JE总行数"] == je_res["total_rows"]
    assert recon["summary"]["TB总行数"] == tb_res["total_rows"]
    assert recon["summary"]["返回样例条数上限"] == 0

    print(json.dumps({
        "je": {"rows": je_res["total_rows"], "columns": je_res["columns"]},
        "tb": {"rows": tb_res["total_rows"], "columns": tb_res["columns"]},
        "reconciliation_summary": recon["summary"],
        "result_files": recon.get("result_files", {}),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
