# Blaise Export Reporting Tool ![Ernie](.github/bert.png)

[![codecov](https://codecov.io/gh/ONSdigital/blaise-export-reporting-tool/branch/main/graph/badge.svg)](https://codecov.io/gh/ONSdigital/blaise-export-reporting-tool)
[![CI status](https://github.com/ONSdigital/blaise-export-reporting-tool/workflows/Test%20coverage%20report/badge.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/workflows/Test%20coverage%20report/badge.svg)
<img src="https://img.shields.io/github/release/ONSdigital/blaise-export-reporting-tool.svg?style=flat-square">
[![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/pulls)
[![Github last commit](https://img.shields.io/github/last-commit/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/commits)
[![Github contributors](https://img.shields.io/github/contributors/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/graphs/contributors)

This project extracts management information data from the Blaise CATI database and via calls to our [RESTful API](https://github.com/ONSdigital/blaise-api-rest). Persists data in [Datastore](https://cloud.google.com/datastore/docs/) if necessary. Reports can be generated from this data and delivered to an on-prem share or via the [Management Information Reports "Ernie" UI](https://github.com/ONSdigital/blaise-management-information-reports).

### Services

This repository has two services.

- Cloud Function Python application to extract the CATI data and Blaise response data and store it in Datastore.
- App Engine Python Flask application to query Datastore for reports to be displayed in the [Management Information Reports "Ernie" UI](https://github.com/ONSdigital/blaise-management-information-reports).

![Flow](.github/bert-ernie-flow.png)

### Reports

#### Interviewer Call History

Get call history for a specified interviewer (interviewer login name) within a date range provided in the URL parameters. Returns a JSON list of call history entries.

```http request
GET /api/reports/call-history/<interviewer>?start-date=2021-05-01&end-date=2021-06-01
Content-Type: application/json
```

### Local Setup

Clone the project locally:

```shell
git clone https://github.com/ONSdigital/blaise-export-reporting-tool.git
```

Setup virtual environment:

macOS:
```shell
python3 -m venv venv  
source venv/bin/activate
```
Windows:
```shell
python -m venv venv  
venv\Scripts\activate
```

Install poetry:
```shell
pip install poetry
```

Install dependencies:
```shell
poetry install
```

# TODO
gcloud auth login
gcloud config set project ons-blaise-v2-dev-blah
env vars...
gcloud compute start-iap-tunnel restapi-1 80 --local-host-port=localhost:90 --zone europe-west2-a
open sql to your network?
change funct called in main to run cloud functions
setting project in code...

Authenticate application with GCP project:
```shell
gcloud auth application-default login
```

Run application:
```shell
python main.py
```

You should now be able to call the application via [localhost:5011](http://localhost:5011). 

### Run Tests

```shell
poetry run python -m pytest
```
