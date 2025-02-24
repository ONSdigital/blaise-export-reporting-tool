Feature: Generate Reports

  Scenario Outline: All fields and data are available
    Given all of the following fields are available for the Respondent Data report
      | field_name                        | value           |
      | QID.Serial_Number                 | 900001          |
      | QHAdmin.HOut                      | 310             |
      | QHAdmin.Interviewer[1]            | testuser        |
      | Mode                              | testmode        |
      | QDataBag.PostCode                 | PO57 2OD        |
      | QHousehold.QHHold.Person[1].Sex   | Male            |
      | QHousehold.QHHold.Person[1].tmpDoB| 2-11-2022_3:08  |
      | QHousehold.QHHold.Person[1].DVAge | 2               |
      | DateTimeStamp                     | 2-11-2022_9:20  |
    And data is present in all fields
    When the report generation is triggered
    Then the report should be generated and delivered with the available fields
    And no warnings should be logged
    Examples:
      | field_name   |
      | 0            |

Scenario Outline: Some fields are missing and all data is available
    Given all of the following fields are available for the Respondent Data report
      | field_name                        | value           |
      | QID.Serial_Number                 | 900001          |
      | QHAdmin.HOut                      | 310             |
      | QHAdmin.Interviewer[1]            | testuser        |
      | Mode                              | testmode        |
      | QHousehold.QHHold.Person[1].Sex   | Male            |
      | QHousehold.QHHold.Person[1].DVAge | 2               |
      | DateTimeStamp                     | 2-11-2022_9:20  |
    And the field "postcode" is missing
    When the report generation is triggered
    Then the report should be generated and delivered with the available fields
    And no warnings should be logged
    And "Done - LMS2222Z" is logged as an information message
    Examples:
      | field_name   |
      | 0            |

  Scenario Outline: All fields are missing
    Given none of the following fields are available for the report
      | field_name                        |
      | QID.Serial_Number                 |
      | QHAdmin.HOut                      |
      | QHAdmin.Interviewer[1]            |
      | Mode                              |
      | QDataBag.PostCode                 |
      | QHousehold.QHHold.Person[1].Sex   |
      | QHousehold.QHHold.Person[1].tmpDoB|
      | QHousehold.QHHold.Person[1].DVAge |
      | DateTimeStamp                     |
    When the report generation is triggered
    Then the report should not be generated
    And "No respondent data for LMS2222Z" is logged as an warning message
    Examples:
      | field_name   |
      | 0            |

  Scenario Outline: All fields are available but some data is missing
    Given all of the following fields are available for the Respondent Data report
      | field_name                        | value           |
      | QID.Serial_Number                 | 900001          |
      | QHAdmin.HOut                      | 310             |
      | QHAdmin.Interviewer[1]            | testuser        |
      | Mode                              | testmode        |
      | QDataBag.PostCode                 |                 |
      | QHousehold.QHHold.Person[1].Sex   | Male            |
      | QHousehold.QHHold.Person[1].tmpDoB|                 |
      | QHousehold.QHHold.Person[1].DVAge | 2               |
      | DateTimeStamp                     | 2-11-2022_9:20  |
    And data is present in all fields
    And there is no data present in "postcode"
    When the report generation is triggered
    Then the report should be generated and delivered with the available fields
    And no warnings should be logged
    And "Done - LMS2222Z" is logged as an information message
    Examples:
      | field_name   |
      | 0            |

  Scenario Outline: All fields are available but no data is present
    Given all of the following fields are available for the Respondent Data report
      | field_name                        | value           |
      | QID.Serial_Number                 |                 |
      | QHAdmin.HOut                      |                 |
      | QHAdmin.Interviewer[1]            |                 |
      | Mode                              |                 |
      | QDataBag.PostCode                 |                 |
      | QHousehold.QHHold.Person[1].Sex   |                 |
      | QHousehold.QHHold.Person[1].tmpDoB|                 |
      | QHousehold.QHHold.Person[1].DVAge |                 |
      | DateTimeStamp                     |                 |
      | qiD.Case_ID                       |                 |
      | qiD.ShiftNo                       |                 |
      | qiD.Interv                        |                 |
      | notes.CaseNotes                   |                 |
      | flightList.Flight1                |                 |
      | flightList.Flight2                |                 |
      | flightList.Flight3                |                 |
      | flightList.Flight4                |                 |
      | flightList.Flight5                |                 |
      | flightList.Flight6                |                 |
      | flightList.Flight7                |                 |
      | flightList.Flight8                |                 |
      | qShift.IntDate                    |                 |
      | qShift.SelectionTime              |                 |
      | dmExitTime                        |                 |
      | qShiftSetup.QShftDet.IntType      |                 |
      | qShift.SampInterval               |                 |
      | qShift.ShiftType                  |                 |
      | qShift.Portroute                  |                 |
      | qShift.Baseport                   |                 |
      | qShift.Linecode                   |                 |
      | qShift.FlightNum                  |                 |
      | qShift.DVFlightNum                |                 |
      | qShift.PortCode                   |                 |
      | qShift.PortDestination            |                 |
      | qShift.Shuttle                    |                 |
      | qShift.CrossShut                  |                 |
      | qShift.Vehicle                    |                 |
      | qShift.IsElig                     |                 |
      | qShift.FerryTime                  |                 |
      | qIndiv.QNationality.Flow          |                 |
      | qAdmin.DVRespnse                  |                 |
      | qAdmin.proportion                 |                 |
      | qAdmin.response_visitbritain      |                 |
      | qAdmin.response_age_sex           |                 |
      | qAdmin.response_student           |                 |
      | qAdmin.response_fe_trailer        |                 |
      | qAdmin.response_migration_trailer |                 |
      | dmTimeIsElig                      |                 |
      | dmTimeAgeSex                      |                 |
      | qIndiv.QNationality.UKForeign     |                 |
      | qIndiv.QBStudent.StudyCheck       |                 |
      | qIndiv.QExpend.DVExpend           |                 |
      | qIndiv.QAgeSex.Age                |                 |
      | qIndiv.QAgeSex.Sex                |                 |
    And there is no data present in any of the fields
    When the report generation is triggered
    Then the report should not be generated
    And "No respondent data for LMS2222Z" is logged as an warning message
    Examples:
      | field_name   |
      | 0            |