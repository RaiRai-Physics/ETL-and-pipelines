import pandas as pd
from src.common.utils import clean_text, parse_number, bool_from_text, stable_hash


def test_clean_text_upper():
    s = pd.Series([" abc ", "", None])
    result = clean_text(s, default="NA", case="upper").tolist()
    assert result == ["ABC", "NA", "NA"]


def test_parse_number_handles_bad_values():
    s = pd.Series(["10.5", "$20", "abc", ""])
    result = parse_number(s, default=0).tolist()
    assert result == [10.5, 20.0, 0.0, 0.0]


def test_bool_from_text():
    s = pd.Series(["Y", "No", "TRUE", "0", "maybe"])
    result = bool_from_text(s).tolist()
    assert result[:4] == [True, False, True, False]


def test_stable_hash_length():
    assert len(stable_hash("C00001")) == 16
