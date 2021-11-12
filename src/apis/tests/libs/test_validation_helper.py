
from libs.validation_helper import parse_csv, not_valid_csv


def test_parse_csv():
    csv = '1,2,3,4,5'
    assert list(parse_csv(csv)) == [1, 2, 3, 4, 5]

    trailing_and_leading_commas = ',1,2,3,'
    assert list(parse_csv(trailing_and_leading_commas)) == [1, 2, 3]

    duplicates = '1,1,1,1,2,3'
    assert list(parse_csv(duplicates)) == [1, 2, 3]


def test_not_valid_csv():
    csv = '1,2,Hello,World,5'
    assert not_valid_csv(csv)

    csv = '1,2,3'
    assert not_valid_csv(csv) is False
