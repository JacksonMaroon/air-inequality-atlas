#!/usr/bin/env Rscript

source("R/helpers_io.R")
assert_project_root()

message("Running ETL pipeline...")

run_step <- function(s) {
  message("\n---\nStep: ", s)
  sys.source(s, envir = new.env(parent = globalenv()))
}

# Always run non-AQS steps first.
run_step("data-raw/01_get_county_geo.R")
run_step("data-raw/04_pull_places.R")
run_step("data-raw/05_pull_svi.R")

# AQS: either use existing output, aggregate from cache, or pull+aggregate (requires creds).
aqs_out <- proj_path("data", "aqs_county_year.parquet")
aqs_cache <- proj_path("data-raw", "aqs", "annual")
aqs_has_creds <- Sys.getenv("AQS_EMAIL") != "" && Sys.getenv("AQS_KEY") != ""

if (file.exists(aqs_out)) {
  message("\nFound existing data/aqs_county_year.parquet; skipping AQS pull + aggregate.")
} else if (dir.exists(aqs_cache) &&
  length(list.files(aqs_cache, pattern = "\\.json(\\.gz)?$", recursive = TRUE)) > 0) {
  message("\nFound AQS cache under data-raw/aqs/annual; aggregating to data/aqs_county_year.parquet.")
  run_step("data-raw/03_aggregate_aqs_county_year.R")
} else if (aqs_has_creds) {
  run_step("data-raw/02_pull_aqs_annual.R")
  run_step("data-raw/03_aggregate_aqs_county_year.R")
} else {
  stop(
    "Missing AQS outputs and credentials.\n",
    "Either:\n",
    "  1) Set AQS_EMAIL and AQS_KEY, then rerun, or\n",
    "  2) Provide data/aqs_county_year.parquet (precomputed), or\n",
    "  3) Populate data-raw/aqs/annual cache (JSON) and rerun.\n",
    call. = FALSE
  )
}

run_step("data-raw/06_build_analytic_mart.R")

message("\nETL complete.")
