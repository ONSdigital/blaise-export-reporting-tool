service: bert
runtime: python313

instance_class: F4

vpc_access_connector:
  name: _VPC_CONNECTOR

env_variables:
  MYSQL_HOST: _MYSQL_HOST
  MYSQL_USER: _MYSQL_USER
  MYSQL_PASSWORD: _MYSQL_PASSWORD
  MYSQL_DATABASE: _MYSQL_DATABASE
  BLAISE_API_URL: _BLAISE_API_URL
  NIFI_STAGING_BUCKET: _NIFI_STAGING_BUCKET

automatic_scaling:
  min_instances: _MIN_INSTANCES
  max_instances: _MAX_INSTANCES
  target_cpu_utilization: _TARGET_CPU_UTILIZATION

handlers:
- url: /.*
  script: auto
  secure: always
  redirect_http_response_code: 301
