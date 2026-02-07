# Air Inequality Atlas (R/Shiny)

County-level U.S. dashboard integrating:

* EPA AQS annual pollution summaries (PM2.5 `88101`, ozone `44201`) aggregated to county-year
* CDC PLACES county modeled health outcomes (MRP estimates) via CDC open data (`swc5-untb`)
* CDC/ATSDR Social Vulnerability Index (SVI) 2022 county percentiles
* U.S. Census cartographic boundary county geometries (CB 2023 500k)

## Guardrails (Required Framing)

* No “real-time” air quality claims: AQS is not real-time (use AirNow for real-time).
* No county-level PLACES trend claims: PLACES estimates are not intended for county trend inference.
* Avoid causal language: use “associated with / co-varies with.”

## Repo Structure

* `data-raw/`: reproducible ETL scripts (no API calls in Shiny runtime)
* `data/`: app-ready Parquet/RDS outputs
* `R/`: Shiny modules + helper functions
* `www/`: CSS + glossary/methods assets

## Setup

This project uses `renv` for reproducible R dependencies.

```bash
cd /Users/jacksonmaroon/air-inequality-atlas
Rscript -e 'renv::restore()'
```

## Secrets (AQS API)

Set these environment variables before running AQS ETL:

* `AQS_EMAIL`
* `AQS_KEY`

Example (zsh):

```bash
export AQS_EMAIL="you@example.com"
export AQS_KEY="YOUR_AQS_KEY"
```

## Run ETL

Run from the project root:

```bash
cd /Users/jacksonmaroon/air-inequality-atlas
Rscript data-raw/run_all.R
```

Notes:

* AQS pulls (2012–2023, PM2.5 + ozone, 50 states + DC) can take a long time due to rate limits.
* Raw AQS responses are cached under `data-raw/aqs/` (gitignored).
* By default, AQS cache files are written as gzip-compressed JSON (`.json.gz`) to reduce disk usage.
  Set `AQS_CACHE_GZIP=0` to write plain `.json` instead.

## Run The App

After ETL outputs exist in `data/`:

```bash
cd /Users/jacksonmaroon/air-inequality-atlas
Rscript -e 'shiny::runApp(\"app.R\", launch.browser = TRUE)'
```

## Outputs Produced By ETL

* `data/geo_county_simplified.rds` (sf; simplified geometries for Leaflet)
* `data/county_master.parquet` (attributes; join spine)
* `data/aqs_county_year.parquet` (county-year pollution)
* `data/places_county.parquet` (county snapshot outcomes)
* `data/svi_county_2022.parquet` (SVI percentiles)
* `data/county_analytic.parquet` (joined analytic mart + derived metrics)
* `data/data_dictionary.parquet` (column dictionary)
* `data/vintages.json` (data vintages)
