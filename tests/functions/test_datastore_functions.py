from functions.datastore_functions import identify_invalid_phone_number_cases


def test_identify_invalid_phone_number_cases_returns_correct_call_result_and_status_when_hout_is_320():
    record = [{
        "outcome_code": "320",
        "call_result": "NoAnswer",
        "status": "NoAnswer",
    }]

    assert identify_invalid_phone_number_cases(record) == [{
        "outcome_code": "320",
        "call_result": "InvalidPhoneNumber",
        "status": "InvalidPhoneNumber",
    }]


def test_identify_invalid_phone_number_cases_does_not_alter_call_result_and_status_when_hout_is_not_320():
    record = [{
        "outcome_code": "310",
        "call_result": "NoAnswer",
        "status": "NoAnswer",
    }]

    assert identify_invalid_phone_number_cases(record) == [{
        "outcome_code": "310",
        "call_result": "NoAnswer",
        "status": "NoAnswer",
    }]


def test_identify_invalid_phone_number_cases_alters_one_case_when_only_one_case_has_a_hout_of_320():
    record = [
        {
            "outcome_code": "310",
            "call_result": "NoAnswer",
            "status": "NoAnswer",
        },
        {
            "outcome_code": "320",
            "call_result": "NoAnswer",
            "status": "NoAnswer",
        },
    ]

    assert identify_invalid_phone_number_cases(record) == [
        {
            "outcome_code": "310",
            "call_result": "NoAnswer",
            "status": "NoAnswer",
        },
        {
            "outcome_code": "320",
            "call_result": "InvalidPhoneNumber",
            "status": "InvalidPhoneNumber",
        },
    ]
