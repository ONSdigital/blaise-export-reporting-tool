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
    Then data is present in all fields
    When the report generation is triggered
    Then the report should be generated and delivered
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
      | QDataBag.PostCode                 |                 |
      | QHousehold.QHHold.Person[1].Sex   | Male            |
      | QHousehold.QHHold.Person[1].tmpDoB|                 |
      | QHousehold.QHHold.Person[1].DVAge | 2               |
      | DateTimeStamp                     | 2-11-2022_9:20  |
    Then data is present in all fields
    Then the field "postcode" is missing
    And the field "date_of_birth" is missing
    When the report generation is triggered
    Then the report should be generated and delivered
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
    Then the report should be generated and delivered
    And "No respondent data for LMS2222Z" is logged as an warning message
    Examples:
      | field_name   |
      | 0            |