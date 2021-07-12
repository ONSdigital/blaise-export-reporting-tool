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

Open a tunnel to our RESTful API in your GCP project:
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
| MYSQL_DATABASE | The database to use on the MySQL instance. | blaise |
| BLAISE_API_URL | The RESTful API the application will use to get data for reports. | localhost:90 |
| NIFI_STAGING_BUCKET | The bucket where data will be delivered. | ons-blaise-v2-dev-nifi-staging |

```shell
GCLOUD_PROJECT="ons-blaise-v2-dev"
MYSQL_HOST="1.3.3.7"
MYSQL_USER="blaise"
MYSQL_PASSWORD="BadPassword123"
MYSQL_DATABASE="blaise"
BLAISE_API_URL="localhost:90"
NIFI_STAGING_BUCKET="ons-blaise-v2-dev-nifi-staging"
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
