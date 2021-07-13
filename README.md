# Blaise Export Reporting Tool ![Ernie](.github/bert.png)

[![codecov](https://codecov.io/gh/ONSdigital/blaise-export-reporting-tool/branch/main/graph/badge.svg)](https://codecov.io/gh/ONSdigital/blaise-export-reporting-tool)
[![CI status](https://github.com/ONSdigital/blaise-export-reporting-tool/workflows/Test%20coverage%20report/badge.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/workflows/Test%20coverage%20report/badge.svg)
<img src="https://img.shields.io/github/release/ONSdigital/blaise-export-reporting-tool.svg?style=flat-square">
[![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/pulls)
[![Github last commit](https://img.shields.io/github/last-commit/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/commits)
[![Github contributors](https://img.shields.io/github/contributors/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/graphs/contributors)

This project contains several services for extracting and delivering management information.

### Services

- Cloud Function (upload_call_history) to extract call history data from the Blaise CATI database, merge this with some questionnaire response data using our Blaise [Blaise RESTful API](https://github.com/ONSdigital/blaise-api-rest) and storing this data in [Datastore](https://cloud.google.com/datastore/docs/) for 12 months. 
- Flask application to provide endpoints for querying the data stored in Datastore. Some endpoints also take care of data cleansing and summary calculations. These endpoints are called by the [Management Information Reports "Ernie" UI](https://github.com/ONSdigital/blaise-management-information-reports).
- Cloud Function (deliver_mi_hub_reports) to extract data for several reports from CATI and questionnaire responses, format these into CSVs and delivery them to a storage bucket so they can be picked up by NiFi for on-prem delivery.

![Flow](.github/bert-ernie-flow.png)

### Reports

#### Interviewer Call History

Get call history for a specified interviewer (interviewer login name) within a date range provided in the URL parameters. Returns a JSON list of call history entries.

```http request
GET /api/reports/call-history/<interviewer>?start-date=<date>&end-date=<date>
Content-Type: application/json
```

#### Interviewer Call Pattern

blah blah...

```http request
GET /api/reports/call-pattern/<interviewer>?start-date=<date>&end-date=<date>
Content-Type: application/json
```

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
gcloud config set project ons-blaise-v2-dev
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
| GCLOUD_PROJECT | The GCP project the application will use. | ons-blaise-v2-dev |
| MYSQL_HOST | The host address of the MySQL instance where reports will get data. Consider opening the MySQL instance in your GCP project to your network. | 1.3.3.7 |
| MYSQL_USER | The username for the MySQL instance. | blaise |
| MYSQL_PASSWORD | The password for the MySQL instance. | BadPassword123 |
| MYSQL_DATABASE | The database to use on the MySQL instance. | cati |
| BLAISE_API_URL | The RESTful API the application will use to get data for reports. | localhost:90 |
| NIFI_STAGING_BUCKET | The bucket where data will be delivered. | ons-blaise-v2-dev-nifi-staging |

```shell
GCLOUD_PROJECT="ons-blaise-v2-dev"
MYSQL_HOST="1.3.3.7"
MYSQL_USER="blaise"
MYSQL_PASSWORD="BadPassword123"
MYSQL_DATABASE="cati"
BLAISE_API_URL="localhost:90"
NIFI_STAGING_BUCKET="ons-blaise-v2-dev-nifi-staging"
```

Run the Flask application:
```shell
python main.py
```

You should now be able to call the Flask application report endpoints via [localhost:5011](http://localhost:5011). Examples:

```http
http://localhost:5011/api/reports/call-history/rich?start-date=2021-01-01&end-date=2022-01-01
```

```http
http://localhost:5011/api/reports/call-pattern/rich?start-date=2021-01-01&end-date=2022-01-01
```

Run the "deliver_mi_hub_reports" Cloud Function:

```shell
python -c "from main import deliver_mi_hub_reports; deliver_mi_hub_reports(None, None)"
```

### Run Tests

```shell
poetry run python -m pytest
```
