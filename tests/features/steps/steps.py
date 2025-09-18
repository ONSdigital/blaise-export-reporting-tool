# mypy: disable-error-code="no-redef"

import logging
from dataclasses import fields

from behave import given, then, when

from models.mi_hub_respondent_data_model import MiHubRespondentData
from services.deliver_mi_hub_reports_service import DeliverMiHubReportsService


@given("all of the following fields are available for the Respondent Data report")
def step_impl(context):
    if context.table:
        fields = {row["field_name"]: row["value"] for row in context.table}

        context.mi_hub_respondent_data = [
            MiHubRespondentData(
                serial_number=fields.get("QID.Serial_Number", None),
                outcome_code=fields.get("QHAdmin.HOut", None),
                date_completed=fields.get("DateTimeStamp", None),
                interviewer=fields.get("QHAdmin.Interviewer[1]", None),
                mode=fields.get("Mode", None),
                postcode=fields.get("QDataBag.PostCode", None),
                gender=fields.get("QHousehold.QHHold.Person[1].Sex", None),
                date_of_birth=fields.get("QHousehold.QHHold.Person[1].tmpDoB", None),
                age=fields.get("QHousehold.QHHold.Person[1].DVAge", None),
                Case_ID=fields.get("qiD.Case_ID", None),
                ShiftNo=fields.get("qiD.ShiftNo", None),
                Interv=fields.get("qiD.Interv", None),
                CaseNotes=fields.get("notes.CaseNotes", None),
                Flight1=fields.get("flightList.Flight1", None),
                Flight2=fields.get("flightList.Flight2", None),
                Flight3=fields.get("flightList.Flight3", None),
                Flight4=fields.get("flightList.Flight4", None),
                Flight5=fields.get("flightList.Flight5", None),
                Flight6=fields.get("flightList.Flight6", None),
                Flight7=fields.get("flightList.Flight7", None),
                Flight8=fields.get("flightList.Flight8", None),
                IntDate=fields.get("qShift.IntDate", None),
                SelectionTime=fields.get("qShift.SelectionTime", None),
                DMExitTime=fields.get("dmExitTime", None),
                IntType=fields.get("qShiftSetup.QShftDet.IntType", None),
                SampInterval=fields.get("qShift.SampInterval", None),
                ShiftType=fields.get("qShift.ShiftType", None),
                Portroute=fields.get("qShift.Portroute", None),
                Baseport=fields.get("qShift.Baseport", None),
                Linecode=fields.get("qShift.Linecode", None),
                FlightNum=fields.get("qShift.FlightNum", None),
                SelectFlight=fields.get("qShift.SelectFlight", None),
                PortCode=fields.get("qShift.PortCode", None),
                PortDestination=fields.get("qShift.PortDestination", None),
                Shuttle=fields.get("qShift.Shuttle", None),
                CrossShut=fields.get("qShift.CrossShut", None),
                Vehicle=fields.get("qShift.Vehicle", None),
                IsElig=fields.get("qShift.IsElig", None),
                FerryTime=fields.get("qShift.FerryTime", None),
                Flow=fields.get("qIndiv.QNationality.Flow", None),
                DVRespnse=fields.get("qAdmin.DVRespnse", None),
                proportion=fields.get("qAdmin.proportion", None),
                response_visitbritain=fields.get("qAdmin.response_visitbritain", None),
                response_age_sex=fields.get("qAdmin.response_age_sex", None),
                response_student=fields.get("qAdmin.response_student", None),
                response_fe_trailer=fields.get("qAdmin.response_fe_trailer", None),
                response_migration_trailer=fields.get(
                    "qAdmin.response_migration_trailer", None
                ),
                DMTimeIsElig=fields.get("dmTimeIsElig", None),
                DMTimeAgeSex=fields.get("dmTimeAgeSex", None),
                UKForeign=fields.get("qIndiv.QNationality.UKForeign", None),
                StudyCheck=fields.get("qIndiv.QBStudent.StudyCheck", None),
                Expenditure=fields.get("qIndiv.QExpend.DVExpend", None),
                Age=fields.get("qIndiv.QAgeSex.Age", None),
                Sex=fields.get("qIndiv.QAgeSex.Sex", None),
            )
        ]


@given("all of the following fields are available for the both Respondent Data reports")
def step_impl(context):
    if context.table:
        fields_1 = {row["field_name_1"]: row["value_1"] for row in context.table}
        fields_2 = {row["field_name_2"]: row["value_2"] for row in context.table}

        context.mi_hub_respondent_data = [
            MiHubRespondentData(
                serial_number=fields_1.get("QID.Serial_Number", None),
                outcome_code=fields_1.get("QHAdmin.HOut", None),
                date_completed=fields_1.get("DateTimeStamp", None),
                interviewer=fields_1.get("QHAdmin.Interviewer[1]", None),
                mode=fields_1.get("Mode", None),
                postcode=fields_1.get("QDataBag.PostCode", None),
                gender=fields_1.get("QHousehold.QHHold.Person[1].Sex", None),
                date_of_birth=fields_1.get("QHousehold.QHHold.Person[1].tmpDoB", None),
                age=fields_1.get("QHousehold.QHHold.Person[1].DVAge", None),
            ),
            MiHubRespondentData(
                serial_number=fields_2.get("QID.Serial_Number", None),
                outcome_code=fields_2.get("QHAdmin.HOut", None),
                date_completed=fields_2.get("DateTimeStamp", None),
                interviewer=fields_2.get("QHAdmin.Interviewer[1]", None),
                mode=fields_2.get("Mode", None),
                postcode=fields_2.get("QDataBag.PostCode", None),
                gender=fields_2.get("QHousehold.QHHold.Person[1].Sex", None),
                date_of_birth=fields_2.get("QHousehold.QHHold.Person[1].tmpDoB", None),
                age=fields_2.get("QHousehold.QHHold.Person[1].DVAge", None),
            ),
        ]


@when("the report generation is triggered")
def step_impl(context):
    response = DeliverMiHubReportsService.upload_mi_hub_reports_to_gcp(
        questionnaire_name=context.questionnaire_name,
        mi_hub_call_history=context.mi_hub_call_history,
        mi_hub_respondent_data=context.mi_hub_respondent_data,
        google_storage=context.mock_google_storage,
    )

    context.response = response


@given("data is present in all fields")
def step_impl(context):
    for respondent_data in context.mi_hub_respondent_data:
        assert not all(
            value is None for value in vars(respondent_data).values()
        ), f"Fields are not all present"


@given('the field "{field}" is missing')
def step_impl(context, field):
    for respondent_data in context.mi_hub_respondent_data:
        assert getattr(respondent_data, field) is None, f"Field {field} is present"


@given('there is no data present in "{field}"')
def step_impl(context, field):
    for respondent_data in context.mi_hub_respondent_data:
        assert getattr(respondent_data, field) == "", f"Field {field} has data present"


@given("there is no data present in any of the fields")
def step_impl(context):
    for respondent_data in context.mi_hub_respondent_data:
        for field in fields(respondent_data):
            field_name = field.name
            field_value = getattr(respondent_data, field_name)
            assert field_value == "", f"Field {field_value} has data present"


@given("none of the following fields are available for the report")
def step_impl(context):
    context.mi_hub_respondent_data = []


@then("the report should be generated and delivered with the available fields")
def step_impl(context):
    assert (
        context.response == "Done - " + context.questionnaire_name
    ), f"No report is generated"


@then("no warnings should be logged")
def step_impl(context):
    messages = [
        (record.levelno, record.getMessage()) for record in context.log_capture.buffer
    ]
    mappings = {
        "information": logging.INFO,
        "error": logging.ERROR,
        "warning": logging.WARNING,
    }
    assert (
        mappings["warning"],
        "No respondent data for LMS2222Z",
    ) not in messages, f"Warnings present"


@then("the report should not be generated")
def step_impl(context):
    messages = [
        (record.levelno, record.getMessage()) for record in context.log_capture.buffer
    ]
    mappings = {
        "information": logging.INFO,
        "error": logging.ERROR,
        "warning": logging.WARNING,
    }
    assert (
        mappings["warning"],
        "No respondent data for LMS2222Z",
    ) in messages, (
        f"Could not find warning, No respondent data for LMS2222Z in {messages}"
    )


@then('"{message}" is logged as an {error_level} message')
def step_impl(context, message, error_level):
    messages = [
        (record.levelno, record.getMessage()) for record in context.log_capture.buffer
    ]
    mappings = {
        "information": logging.INFO,
        "error": logging.ERROR,
        "warning": logging.WARNING,
    }
    assert (
        mappings[error_level],
        message,
    ) in messages, f"Could not find {mappings[error_level]}, {message} in {messages}"
