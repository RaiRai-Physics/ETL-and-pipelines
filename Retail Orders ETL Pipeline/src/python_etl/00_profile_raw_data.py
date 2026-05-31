"""
Profile raw CSV files before cleaning.
"""

from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORT_DIR = PROJECT_ROOT / "reports" / "python"


def profile_file(csv_path: Path) -> tuple[dict, list[dict]]:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    table_summary = {
        "table_name": csv_path.stem,
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicate_full_rows": int(df.duplicated().sum()),
    }

    column_rows = []
    for column in df.columns:
        values = df[column].astype(str).str.strip()
        column_rows.append({
            "table_name": csv_path.stem,
            "column_name": column,
            "null_or_blank_count": int((values == "").sum()),
            "distinct_count": int(values.nunique(dropna=False)),
            "sample_values": ", ".join(values.drop_duplicates().head(5).tolist()),
        })

    return table_summary, column_rows


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    summary_rows = []
    column_rows = []

    for csv_path in sorted(RAW_DIR.glob("*.csv")):
        summary, columns = profile_file(csv_path)
        summary_rows.append(summary)
        column_rows.extend(columns)

    pd.DataFrame(summary_rows).to_csv(REPORT_DIR / "raw_table_profile.csv", index=False)
    pd.DataFrame(column_rows).to_csv(REPORT_DIR / "raw_column_profile.csv", index=False)

    print("Raw data profiling complete.")
    print(f"Wrote: {REPORT_DIR / 'raw_table_profile.csv'}")
    print(f"Wrote: {REPORT_DIR / 'raw_column_profile.csv'}")


if __name__ == "__main__":
    main()
