from lib.ithub.parser import clean_text


def test_clean_text_returns_empty_string_for_none() -> None:
    assert clean_text(None) == ""


def test_clean_text_collapses_extra_whitespace() -> None:
    assert clean_text("  Data    Engineer \n  Remote  ") == "Data Engineer Remote"