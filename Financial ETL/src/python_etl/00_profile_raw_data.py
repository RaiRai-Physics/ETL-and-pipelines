"""
Raw data profiling for the advanced financial ETL project.

Run:
    python src/python_etl/00_profile_raw_data.py
"""

from pathlib import Path
import sys
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from src.common.utils import read_raw, write_csv, load_config

CONFIG = load_config()
REPORT_DIR = PROJECT_ROOT / CONFIG["report_path"]


def profile_table(table_name: str) -> tuple[dict, list[dict]]:
    df = read_raw(table_name)
    table_summary = {
        "table_name": table_name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicate_full_rows": int(df.duplicated().sum()),
    }

    column_rows = []
    for column in df.columns:
        values = df[column].astype(str).str.strip()
        column_rows.append({
            "table_name": table_name,
            "column_name": column,
            "blank_count": int((values == "").sum()),
            "distinct_count": int(values.nunique(dropna=False)),
            "sample_values": " | ".join(values.drop_duplicates().head(5).tolist()),
        })

    return table_summary, column_rows


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    raw_files = sorted((PROJECT_ROOT / "data" / "raw").glob("*.csv"))

    summary_rows = []
    column_rows = []

    for path in raw_files:
        summary, columns = profile_table(path.stem)
        summary_rows.append(summary)
        column_rows.extend(columns)

    write_csv(pd.DataFrame(summary_rows), REPORT_DIR / "raw_table_profile.csv")
    write_csv(pd.DataFrame(column_rows), REPORT_DIR / "raw_column_profile.csv")

    print("Raw profiling complete.")
    print(f"Tables profiled: {len(summary_rows)}")
    print(f"Reports written to: {REPORT_DIR}")


if __name__ == "__main__":
    main()
