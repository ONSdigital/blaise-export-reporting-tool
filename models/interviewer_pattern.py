from dataclasses import dataclass


@dataclass
class InterviewerPatternReport:
    HoursWorked: str
    CallTime: int
    HoursOnCallsPercentage: str
    AverageCallsPerHour: int
    RespondentsInterviewed: int
    HouseholdsCompletedSuccessfully: str
    AverageRespondentsInterviewedPerHour: int
    NoContactsPercentage: str
    AppointmentsForContactsPercentage: str
