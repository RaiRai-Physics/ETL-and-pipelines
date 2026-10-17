import pandas as pd
from src.common.utils import clean_text,parse_num,parse_bool,safe_div,stable_hash

def test_clean_text():
    assert clean_text(pd.Series([" atl ","",None]),"NA","upper").tolist()==["ATL","NA","NA"]

def test_parse_num():
    assert parse_num(pd.Series(["10","$20.5","bad",""]),0).tolist()==[10.0,20.5,0.0,0.0]

def test_parse_bool():
    assert parse_bool(pd.Series(["Y","No","true","0"])).tolist()==[True,False,True,False]

def test_safe_div():
    assert safe_div(pd.Series([10,3]),pd.Series([2,0])).tolist()==[5.0,0.0]

def test_hash_repeatable():
    assert stable_hash("FLT1","A320")==stable_hash("FLT1","A320")
