import pandas as pd
from src.common.utils import clean_text,parse_num,parse_bool,safe_div,stable_hash

def test_clean_text():
    assert clean_text(pd.Series([" abc ","",None]),"NA","upper").tolist()==["ABC","NA","NA"]

def test_parse_num():
    assert parse_num(pd.Series(["10","$20.5","bad",""]),0).tolist()==[10.0,20.5,0.0,0.0]

def test_bool():
    assert parse_bool(pd.Series(["Y","No","TRUE","0"])).tolist()==[True,False,True,False]

def test_hash_stable():
    assert stable_hash("x","y")==stable_hash("x","y")

def test_safe_div():
    assert safe_div(pd.Series([10,3]),pd.Series([2,0])).tolist()==[5.0,0.0]
