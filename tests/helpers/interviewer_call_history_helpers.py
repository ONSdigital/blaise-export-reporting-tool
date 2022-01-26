from google.cloud import datastore


def entity_builder(key, interviewer, start_time, end_time, outcome, call_result):
    entity = datastore.Entity(
        datastore.Key("CallHistory", key, project="test")
    )
    entity["interviewer"] = interviewer
    entity["call_start_time"] = start_time
    entity["call_end_time"] = end_time
    entity["outcome_code"] = outcome
    entity["call_result"] = call_result
    return entity