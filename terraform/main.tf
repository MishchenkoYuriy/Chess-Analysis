terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online --
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS --
}


# DWH: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "marts" {
  dataset_id = var.BQ_DATASET_MARTS
  project    = var.project
  location   = var.region
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = var.BQ_DATASET_STAGING
  project    = var.project
  location   = var.region
}

resource "google_bigquery_dataset" "intermediate" {
  dataset_id = var.BQ_DATASET_INTERMEDIATE
  project    = var.project
  location   = var.region
}