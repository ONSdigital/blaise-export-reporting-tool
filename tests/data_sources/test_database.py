from data_sources.database import convert_list_to_string


def test_convert_list_to_string():
    list_of_strings = ["this", "is", "a", "list", "of", "strings"]

    string = convert_list_to_string(list_of_strings)

    assert string == "this, is, a, list, of, strings"
