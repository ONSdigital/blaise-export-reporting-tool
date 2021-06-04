# Blaise Export Reporting Tool

[![codecov](https://codecov.io/gh/ONSdigital/blaise-export-reporting-tool/branch/main/graph/badge.svg)](https://codecov.io/gh/ONSdigital/blaise-export-reporting-tool)
[![CI status](https://github.com/ONSdigital/blaise-export-reporting-tool/workflows/Test%20coverage%20report/badge.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/workflows/Test%20coverage%20report/badge.svg)
<img src="https://img.shields.io/github/release/ONSdigital/blaise-export-reporting-tool.svg?style=flat-square" alt="Nisra Case Mover release verison">
[![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/pulls)
[![Github last commit](https://img.shields.io/github/last-commit/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/commits)
[![Github contributors](https://img.shields.io/github/contributors/ONSdigital/blaise-export-reporting-tool.svg)](https://github.com/ONSdigital/blaise-export-reporting-tool/graphs/contributors)

Extract data from Blaise CATI database and [Blaise API](https://github.com/ONSdigital/blaise-api-rest) and store in [GCP Firestore in Datastore](https://cloud.google.com/datastore/docs/) to generate reports for Telephone Operations.

Accompanying service [Ernie Reporting UI](https://github.com/ONSdigital/blaise-management-information-reports) for users to fill in form for queries and display results from this service.

![Bert from sesame street](https://vignette.wikia.nocookie.net/vsbattles/images/c/c2/Bert.gif/revision/latest?cb=20160922094917)

### Services

This repository has two services.

- CloudFunction python service to extract the CATI data and Blaise data and store in Firestore in Datastore.
- App engine Flask application to query Datastore for reports to be displayed in [Ernie Reporting UI](https://github.com/ONSdigital/blaise-management-information-reports)

![Process flow diagram of Bert and Ernie](https://user-images.githubusercontent.com/38406765/120787780-9e96a000-c527-11eb-9065-a91efb0b24b5.png)

### Queries

#### Get interviewer call history report
Get call history for specified interview (interviewer login name) within a date range provided in the url. Returns JSON list of call history entries.
```http request
GET /api/reports/call-history/<interviewer>?start-date=2021-05-01&end-date=2021-06-01
Content-Type: application/json

```

### Local Setup

Clone the project locally:

```shell
git clone https://github.com/ONSdigital/blaise-export-reporting-tool.git
```

Install poetry:
```shell
pip install poetry
```

Run poetry install
```shell
poetry install
```

Run poetry install
```shell
poetry install
```

Run BERTs Flask application, this will run on [localhost:5011](http://localhost:5011) by default. 
```shell
python main.py
```

