from pathlib import Path
import sys, pandas as pd
PROJECT_ROOT=Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
from src.common.utils import read_csv, write_csv, load_config, ensure_dirs
CONFIG=load_config(); REPORT=PROJECT_ROOT/CONFIG['report_path']
def main():
    ensure_dirs(REPORT); rows=[]; cols=[]
    for p in sorted((PROJECT_ROOT/'data/raw').rglob('*.csv')):
        df=read_csv(p); rows.append({'table_name':p.stem,'relative_path':str(p.relative_to(PROJECT_ROOT)),'row_count':len(df),'column_count':len(df.columns),'duplicate_full_rows':int(df.duplicated().sum())})
        for c in df.columns:
            v=df[c].astype(str).str.strip()
            cols.append({'table_name':p.stem,'column_name':c,'blank_count':int((v=='').sum()),'distinct_count':int(v.nunique(dropna=False)),'sample_values':' | '.join(v.drop_duplicates().head(5).tolist())})
    write_csv(pd.DataFrame(rows), REPORT/'raw_table_profile.csv'); write_csv(pd.DataFrame(cols), REPORT/'raw_column_profile.csv')
    print(f'Profiled {len(rows)} raw files. Reports written to {REPORT}')
if __name__=='__main__': main()
