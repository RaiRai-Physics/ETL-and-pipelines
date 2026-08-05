import pandas as pd
from src.common.utils import clean_text,parse_number,bool_from_text,safe_divide,stable_hash
def test_clean_text(): assert clean_text(pd.Series([' a ','',None]),'NA','upper').tolist()==['A','NA','NA']
def test_number(): assert parse_number(pd.Series(['10','$2.5','bad','']),0).tolist()==[10.0,2.5,0.0,0.0]
def test_bool(): assert bool_from_text(pd.Series(['Y','No','1','0'])).tolist()==[True,False,True,False]
def test_hash(): assert stable_hash('a','b')==stable_hash('a','b')
