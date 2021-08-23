# Blaise Export Reporting Tool ![Ernie](.github/bert.png)

[![codecov](https://codecov.io/gh/ONSdigital/blaise-export-reporting-tool/branch/main/graph/badge.svg)](https://codecov.io/gh/ONSdigital/blaise-export-reporting-tool)
[![CI status](https://github.com/ONSdigital/blaise-export-reporting-tool/workflows/Test%20coverage%20report/badge.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/workflows/Test%20coverage%20report/badge.svg)
<img src="https://img.shields.io/github/release/ONSdigital/blaise-export-reporting-tool.svg?style=flat-square">
[![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/pulls)
[![Github last commit](https://img.shields.io/github/last-commit/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/commits)
[![Github contributors](https://img.shields.io/github/contributors/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/graphs/contributors)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/ONSdigital/blaise-export-reporting-tool.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/ONSdigital/blaise-export-reporting-tool/alerts/)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/ONSdigital/blaise-export-reporting-tool.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/ONSdigital/blaise-export-reporting-tool/context:python)

This project contains several services for extracting and delivering management information.

### Services

- Cloud Function (upload_call_history) to extract call history data from the Blaise CATI database, merge this with some
  questionnaire response data using our [Blaise RESTful API](https://github.com/ONSdigital/blaise-api-rest) and store
  this data in [Datastore](https://cloud.google.com/datastore/docs/) for 12 months. This data is then used for various
  reports.
- Flask application to provide report API endpoints for querying the data stored in Datastore, the CATI database, and
  the questionnaire response data. Some of these endpoints also take care of data cleansing and summary calculations.
  These endpoints are called by
  the [Management Information Reports "Ernie" UI](https://github.com/ONSdigital/blaise-management-information-reports).
- Cloud Function (deliver_mi_hub_reports) to extract real time data for several reports from the CATI database and
  questionnaire response data, format these into CSV files and delivery them to a storage bucket so they can be picked
  up by NiFi for on-prem delivery.

![Flow](.github/bert-ernie-flow.png)

### Reports

#### Interviewer Call History

API endpoint. View an interviewers call history over a given date range.

```http request
GET /api/reports/call-history/<interviewer>?start-date=<date>&end-date=<date>
Content-Type: application/json
```

#### Interviewer Call Pattern

API endpoint. Analyse productivity of an interviewer over a given date range. Uses call history data to produce
productivity metrics.

```http request
GET /api/reports/call-pattern/<interviewer>?start-date=<date>&end-date=<date>
Content-Type: application/json
```

#### Appointment Resource Planning

API endpoint. View the number of interview appointments scheduled for a given date.

```http request
GET /api/reports/appointment-resource-planning/<date>
Content-Type: application/json
```

#### MI HUB Call History

CSV file delivered on-prem. All call history from active questionnaires.

#### MI HUB Respondent Data

CSV file delivered on-prem. Subset of respondent data from active questionnaires.

### Local Setup

Clone the project locally:

```shell
git clone https://github.com/ONSdigital/blaise-export-reporting-tool.git
```

Setup a virtual environment:

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

Authenticate with GCP:
```shell
gcloud auth login
```

Set your GCP project:
```shell
gcloud config set project ons-blaise-v2-dev-sandbox123
```

Authenticate the application with your GCP project:
```shell
gcloud auth application-default login
```

Open a tunnel to our Blaise RESTful API in your GCP project:
```shell
gcloud compute start-iap-tunnel restapi-1 80 --local-host-port=localhost:90 --zone europe-west2-a
```

Create an .env file in the root of the project and add the following environment variables:

| Variable | Description | Example |
| --- | --- | --- |
| GCLOUD_PROJECT | The GCP project the application will use. | ons-blaise-v2-dev-sandbox123 |
| MYSQL_HOST | The host address of the MySQL instance where reports will get CATI data. Consider opening the MySQL instance in your GCP project to your local network. | 1.3.3.7 |
| MYSQL_USER | The username for the MySQL instance. | blaise |
| MYSQL_PASSWORD | The password for the MySQL instance. | BadPassword123 |
| MYSQL_DATABASE | The database to use on the MySQL instance. | cati |
| BLAISE_API_URL | The RESTful API URL the application will use to get questionnaire response data. | localhost:90 |
| NIFI_STAGING_BUCKET | The storage bucket where data will be delivered. In formal environments this will be picked up by NiFi for on-prem delivery. | ons-blaise-v2-dev-sandbox123-nifi-staging |

```shell
GCLOUD_PROJECT="ons-blaise-v2-dev-sandbox123"
MYSQL_HOST="1.3.3.7"
MYSQL_USER="blaise"
MYSQL_PASSWORD="BadPassword123"
MYSQL_DATABASE="cati"
BLAISE_API_URL="localhost:90"
NIFI_STAGING_BUCKET="ons-blaise-v2-dev-sandbox123-nifi-staging"
```

Run the Flask application:

```shell
python main.py
```

You should now be able to call the Flask application report endpoints via localhost:5011. Examples:

```http
http://localhost:5011/api/reports/call-history-status
```

```http
http://localhost:5011/api/reports/call-history/rich?start-date=2021-01-01&end-date=2022-01-01
```

```http
http://localhost:5011/api/reports/call-pattern/rich?start-date=2021-01-01&end-date=2022-01-01
```

```http
http://localhost:5011/api/reports/appointment-resource-planning/2021-01-01
```

Run the "upload_call_history" Cloud Function:

```shell
python -c "from main import upload_call_history; upload_call_history(None, None)"
```

Run the "deliver_mi_hub_reports" Cloud Function:

```shell
python -c "from main import deliver_mi_hub_reports; deliver_mi_hub_reports(None, None)"
```

Run Tests

```shell
poetry run python -m pytest
```
