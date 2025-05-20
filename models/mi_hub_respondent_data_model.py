from dataclasses import dataclass, fields


@dataclass
class MiHubRespondentData:
    serial_number: str
    outcome_code: str
    date_completed: str
    interviewer: str
    mode: str
    postcode: str
    gender: str
    date_of_birth: str
    age: str
    Case_ID: str
    ShiftNo: str
    Interv: str
    CaseNotes: str
    Flight1: str
    Flight2: str
    Flight3: str
    Flight4: str
    Flight5: str
    Flight6: str
    Flight7: str
    Flight8: str
    IntDate: str
    SelectionTime: str
    DMExitTime: str
    IntType: str
    SampInterval: str
    ShiftType: str
    Portroute: str
    Baseport: str
    Linecode: str
    FlightNum: str
    BShift: str
    SelectFlight: str
    PortCode: str
    PortDestination: str
    Shuttle: str
    CrossShut: str
    Vehicle: str
    IsElig: str
    FerryTime: str
    Flow: str
    DVRespnse: str
    proportion: str
    response_visitbritain: str
    response_age_sex: str
    response_student: str
    response_fe_trailer: str
    response_migration_trailer: str
    DMTimeIsElig: str
    DMTimeAgeSex: str
    UKForeign: str
    StudyCheck: str
    Expenditure: str
    Age: str
    Sex: str

    @classmethod
    def fields(cls):
        return [field.name for field in fields(cls)]
