service: bert-ui
runtime: python37

vpc_access_connector:
  name: projects/_PROJECT_ID/locations/europe-west2/connectors/vpcconnect

env_variables:
  MYSQL_HOST: _MYSQL_HOST
  MYSQL_USER: _MYSQL_USER
  MYSQL_PASSWORD: _MYSQL_PASSWORD
  MYSQL_DATABASE: _MYSQL_DATABASE
  BLAISE_API_URL: _BLAISE_API_URL

basic_scaling:
  idle_timeout: 10m
  max_instances: 10

handlers:
- url: /.*
  script: auto
  secure: always
  redirect_http_response_code: 301
