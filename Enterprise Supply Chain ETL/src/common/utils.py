from pathlib import Path
import json, hashlib
import pandas as pd
import numpy as np
PROJECT_ROOT = Path(__file__).resolve().parents[2]
def load_config():
    return json.loads((PROJECT_ROOT/'config/pipeline_config.json').read_text(encoding='utf-8'))
def ensure_dirs(*paths):
    for p in paths: Path(p).mkdir(parents=True, exist_ok=True)
def read_csv(path): return pd.read_csv(path, dtype=str, keep_default_na=False)
def write_csv(df, path):
    path.parent.mkdir(parents=True, exist_ok=True); df.to_csv(path, index=False)
def clean_text(s, default='Unknown', case=None):
    out=s.fillna('').astype(str).str.strip().replace('', default)
    if case=='upper': out=out.str.upper()
    elif case=='lower': out=out.str.lower()
    elif case=='title': out=out.str.title()
    return out
def parse_date(s): return pd.to_datetime(s.replace('', pd.NA), errors='coerce').dt.date.astype('string')
def parse_ts(s): return pd.to_datetime(s.replace('', pd.NA), errors='coerce')
def parse_number(s, default=np.nan):
    out=s.fillna('').astype(str).str.strip().str.replace('$','',regex=False).str.replace(',','',regex=False).replace({'':'nan','free':'0','FREE':'0'})
    num=pd.to_numeric(out, errors='coerce')
    if not pd.isna(default): num=num.fillna(default)
    return num
def bool_from_text(s):
    v=s.fillna('').astype(str).str.strip().str.upper()
    return np.where(v.isin({'Y','YES','TRUE','1','ACTIVE'}), True, np.where(v.isin({'N','NO','FALSE','0','INACTIVE'}), False, pd.NA))
def stable_hash(*values): return hashlib.sha256('|'.join(str(v) for v in values).encode()).hexdigest()[:20]
def safe_divide(a,b): return np.where(b!=0, a/b, 0)
