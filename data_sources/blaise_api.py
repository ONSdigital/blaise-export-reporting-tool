import requests


def get_list_of_installed_instruments(config):
    print("Getting list of installed instruments")
    response = requests.get(f"http://{config.blaise_api_url}/api/v1/serverparks/gusty/instruments")
    instrument_list = response.json()
    print(f"Found {len(instrument_list)} instruments installed")
    return instrument_list


def get_instrument_data(instrument_name, config, fields):
    fields_to_get = []
    for field in fields:
        fields_to_get.append(("fieldIds", field))

    print(f"Getting instrument data for instrument {instrument_name}")

    try:
        response = requests.get(
            f"http://{config.blaise_api_url}/api/v1/serverparks/gusty/instruments/{instrument_name}/report",
            params=fields_to_get,
        )
        if response.status_code != 200:
            return []
        data = response.json()
        reporting_data = data.get("reportingData")
        if len(reporting_data) == 0:
            return []

        for case in reporting_data:
            case["questionnaire_name"] = instrument_name

        return reporting_data
    except ConnectionResetError:
        print("connection error :( ")
        return []
