from pathlib import Path
import sys,pandas as pd
ROOT=Path(__file__).resolve().parents[2]; sys.path.append(str(ROOT))
from src.common.utils import read_csv,write_csv,config

def main():
    out=ROOT/config()['report_path']; out.mkdir(parents=True,exist_ok=True); tabs=[]; cols=[]
    for p in sorted((ROOT/'data/raw').rglob('*.csv')):
        df=read_csv(p); tabs.append({'table_name':p.stem,'relative_path':str(p.relative_to(ROOT)),'row_count':len(df),'column_count':len(df.columns),'duplicate_full_rows':int(df.duplicated().sum())})
        for c in df.columns:
            s=df[c].astype(str).str.strip(); cols.append({'table_name':p.stem,'column_name':c,'blank_count':int((s=='').sum()),'distinct_count':int(s.nunique()),'sample_values':' | '.join(s.drop_duplicates().head(5))})
    write_csv(pd.DataFrame(tabs),out/'raw_table_profile.csv'); write_csv(pd.DataFrame(cols),out/'raw_column_profile.csv'); print(f'Profiled {len(tabs)} raw files')
if __name__=='__main__': main()
