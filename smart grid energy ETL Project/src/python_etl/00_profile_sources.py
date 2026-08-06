from pathlib import Path
import sys,pandas as pd
ROOT=Path(__file__).resolve().parents[2]; sys.path.append(str(ROOT))
from src.common.utils import read_csv,write_csv,load_config,ensure_dirs
CFG=load_config(); REPORT=ROOT/CFG['report_path']
def main():
    ensure_dirs(REPORT); tables=[]; cols=[]
    for p in sorted((ROOT/'data/raw').rglob('*.csv')):
        df=read_csv(p); tables.append({'table_name':p.stem,'relative_path':str(p.relative_to(ROOT)),'row_count':len(df),'column_count':len(df.columns),'duplicate_full_rows':int(df.duplicated().sum())})
        for c in df.columns:
            v=df[c].astype(str).str.strip(); cols.append({'table_name':p.stem,'column_name':c,'blank_count':int((v=='').sum()),'distinct_count':int(v.nunique(dropna=False)),'sample_values':' | '.join(v.drop_duplicates().head(5).tolist())})
    write_csv(pd.DataFrame(tables),REPORT/'raw_table_profile.csv'); write_csv(pd.DataFrame(cols),REPORT/'raw_column_profile.csv'); print(f'Profiled {len(tables)} raw files.')
if __name__=='__main__': main()
