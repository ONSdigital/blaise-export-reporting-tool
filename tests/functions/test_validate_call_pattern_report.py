from pandas.testing import assert_frame_equal

from functions.validate_call_pattern_report import validate_dataframe, invalid_data_found


def test_validate_dataframe_drops_invalid_data_and_returns_information(missing_dataframe):
    valid_dataframe, invalid_dataframe = validate_dataframe(missing_dataframe)
    assert len(valid_dataframe.index) == 2
    assert len(invalid_dataframe.index) == 1
    assert len(valid_dataframe.index) + len(invalid_dataframe.index) == len(missing_dataframe.index)


def test_validate_dataframe_returns_valid_dataframe_with_lower_case_column_names(valid_dataframe):
    valid_dataframe, invalid_dataframe = validate_dataframe(valid_dataframe)
    assert all(i.islower() for i in list(valid_dataframe.columns))


def test_validate_dataframe_returns_valid_dataframe_with_no_discounted_records_or_fields(valid_dataframe):
    new_valid_dataframe, invalid_dataframe = validate_dataframe(valid_dataframe)
    assert_frame_equal(left=new_valid_dataframe, right=valid_dataframe, check_dtype=False)
    assert invalid_dataframe.empty


def test_invalid_data_found_returns_false(valid_dataframe):
    assert invalid_data_found(valid_dataframe) is False


def test_invalid_data_found_returns_true(missing_dataframe):
    assert invalid_data_found(missing_dataframe) is True
