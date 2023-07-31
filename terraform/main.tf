terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
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