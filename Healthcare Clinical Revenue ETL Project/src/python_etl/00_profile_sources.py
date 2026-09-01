from pathlib import Path
import sys,pandas as pd
ROOT=Path(__file__).resolve().parents[2]; sys.path.append(str(ROOT))
from src.common.utils import read_csv,write_csv,load_config
def main():
    out=ROOT/load_config()["report_path"]; out.mkdir(parents=True,exist_ok=True); s=[]; c=[]
    for p in sorted((ROOT/"data/raw").rglob("*.csv")):
        df=read_csv(p); s.append({"table_name":p.stem,"relative_path":str(p.relative_to(ROOT)),"row_count":len(df),"column_count":len(df.columns),"duplicate_full_rows":int(df.duplicated().sum())})
        for col in df.columns:
            x=df[col].astype(str).str.strip(); c.append({"table_name":p.stem,"column_name":col,"blank_count":int((x=="").sum()),"distinct_count":int(x.nunique()),"sample_values":" | ".join(x.drop_duplicates().head(5))})
    write_csv(pd.DataFrame(s),out/"raw_table_profile.csv"); write_csv(pd.DataFrame(c),out/"raw_column_profile.csv"); print(f"Profiled {len(s)} raw files.")
if __name__=="__main__": main()
