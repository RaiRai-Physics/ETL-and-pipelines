import pandas as pd
from src.common.utils import clean_text,num,b,h,safe_div
def test_text(): assert clean_text(pd.Series([" a ","",None]),"NA","upper").tolist()==["A","NA","NA"]
def test_num(): assert num(pd.Series(["10","bad",""]),0).tolist()==[10.0,0.0,0.0]
def test_bool(): assert b(pd.Series(["Y","No","1","0"])).tolist()==[True,False,True,False]
def test_hash(): assert h("a","b")==h("a","b")
def test_div(): assert safe_div(pd.Series([4,2]),pd.Series([2,0])).tolist()==[2.0,0.0]
