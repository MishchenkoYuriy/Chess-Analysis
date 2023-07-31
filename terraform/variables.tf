variable "project" {
  description = "GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources from https://cloud.google.com/about/locations"
  default = "europe-west1"
  type = string
}

variable "BQ_DATASET_MARTS" {
  description = "BigQuery Dataset for dbt, marts represent business-defined entities"
  default = "marts"
  type = string
}

variable "BQ_DATASET_STAGING" {
  description = "BigQuery Dataset for dbt, staging is a preparation layer"
  default = "staging"
  type = string
}

variable "BQ_DATASET_INTERMEDIATE" {
  description = "BigQuery Dataset for dbt, intermediate used for CTE mainly (intermediate transformation steps)"
  default = "intermediate"
  type = string
}