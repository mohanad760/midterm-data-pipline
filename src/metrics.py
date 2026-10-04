import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import sys

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings


def save_run_metrics(metrics: Dict[str, Any]) -> str:
    """
    Appends or updates run metrics in reports/results.json and regenerates reports/results.md.
    """
    json_path = Path(settings.RESULTS_JSON_PATH)
    md_path = Path(settings.RESULTS_MD_PATH)

    json_path.parent.mkdir(parents=True, exist_ok=True)

    all_results: List[Dict[str, Any]] = []
    if json_path.exists():
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                all_results = json.load(f)
                if not isinstance(all_results, list):
                    all_results = [all_results]
        except Exception:
            all_results = []

    # Add timestamp if not present
    metrics["recorded_at"] = datetime.utcnow().isoformat()
    all_results.append(metrics)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    # Generate Markdown Summary
    generate_markdown_report(all_results, md_path)

    print(f"Metrics saved to: {json_path}")
    print(f"Report updated at: {md_path}")
    return str(json_path)


def generate_markdown_report(all_results: List[Dict[str, Any]], md_path: Path):
    """Generates an executive summary table and audit report in Markdown."""
    lines = [
        "# Data Pipeline Execution Metrics Report",
        "",
        f"**Generated:** {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "## Summary of Pipeline Runs",
        "",
        "| Run ID | Engine | File | Size (MB) | Raw Loaded | Valid | Corrected | Quarantine | Elapsed (s) | Speed (rows/s) | Upsert (Ins/Upd/Unch) |",
        "| :--- | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |",
    ]

    for r in all_results:
        upsert_str = f"{r.get('inserted_count', 0)} / {r.get('updated_count', 0)} / {r.get('unchanged_count', 0)}"
        lines.append(
            f"| `{r.get('run_id')}` | {r.get('engine_used')} | {r.get('file_name')} | "
            f"{r.get('file_size_mb', 0):.2f} | {r.get('raw_loaded', 0):,} | "
            f"{r.get('valid_count', 0):,} | {r.get('corrected_count', 0):,} | "
            f"{r.get('quarantine_count', 0):,} | {r.get('elapsed_seconds', 0):.2f} | "
            f"{r.get('throughput', 0):,.0f} | {upsert_str} |"
        )

    lines.extend([
        "",
        "## Consistency Check Formula Verification",
        "Condition: `run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count`",
        "",
    ])

    for r in all_results:
        raw = r.get("raw_loaded", 0)
        if "valid_count" in r:
            v = r.get("valid_count", 0)
            c = r.get("corrected_count", 0)
            q = r.get("quarantine_count", 0)
            status = "PASSED" if raw == (v + c + q) else "FAILED"
            lines.append(f"- **{r.get('run_id')}**: {raw:,} Raw = {v:,} Valid + {c:,} Corrected + {q:,} Quarantined [{status}]")
        else:
            lines.append(f"- **{r.get('run_id')}**: {raw:,} Raw Ingested to orders_raw [{r.get('engine_used', '').upper()} INGESTION COMPLETE]")

    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
