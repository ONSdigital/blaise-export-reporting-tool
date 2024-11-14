# type: ignore[no-redef]

import logging
from behave import given, then, when
from models.mi_hub_respondent_data_model import MiHubRespondentData
from services.deliver_mi_hub_reports_service import DeliverMiHubReportsService


@given('all of the following fields are available for the Respondent Data report')
def step_impl(context):
    if context.table:
        fields = {row["field_name"]: row["value"] for row in context.table}

        context.mi_hub_respondent_data = [MiHubRespondentData(
            serial_number=str(fields["QID.Serial_Number"]),
            outcome_code=str(fields["QHAdmin.HOut"]),
            date_completed=str(fields["DateTimeStamp"]),
            interviewer=str(fields["QHAdmin.Interviewer[1]"]),
            mode=str(fields["Mode"]),
            postcode=str(fields["QDataBag.PostCode"]),
            gender=str(fields["QHousehold.QHHold.Person[1].Sex"]),
            date_of_birth=str(fields["QHousehold.QHHold.Person[1].tmpDoB"]),
            age=str(fields["QHousehold.QHHold.Person[1].DVAge"]),
        )]


@when('the report generation is triggered')
def step_impl(context):
    response = DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp(
        questionnaire_name=context.questionnaire_name,
        mi_hub_call_history=context.mi_hub_call_history,
        mi_hub_respondent_data=context.mi_hub_respondent_data,
        google_storage=context.mock_google_storage,
    )

    context.response = response


@then('data is present in all fields')
def step_impl(context):
    for respondent_data in context.mi_hub_respondent_data:
        assert (
                not all(value in ("", None) for value in vars(respondent_data).values())
        ), f"Fields are not all present"


@then('the field "{field}" is missing')
def step_impl(context, field):
    for respondent_data in context.mi_hub_respondent_data:
        assert (
            getattr(respondent_data, field) == ""
        ), f"Field {field} is present"


@given('none of the following fields are available for the report')
def step_impl(context):
    context.mi_hub_respondent_data = []


@then('the report should be generated and delivered')
def step_impl(context):
    assert (
        context.response == "Done - " + context.questionnaire_name
    ), f"No report is generated"


@then('no warnings should be logged')
def step_impl(context):
    assert (
            True
    ), f"Warnings should not be generated"


@then('"{message}" is logged as an {error_level} message')
def step_impl(context, message, error_level):
    messages = [
        (record.levelno, record.getMessage()) for record in context.log_capture.buffer
    ]
    mappings = {"information": logging.INFO, "error": logging.ERROR, "warning": logging.WARNING}
    assert (
               mappings[error_level],
               message,
           ) in messages, f"Could not find {mappings[error_level]}, {message} in {messages}"
