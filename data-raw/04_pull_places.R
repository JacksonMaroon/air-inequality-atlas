suppressPackageStartupMessages({
  source("R/helpers_io.R")
  source("R/helpers_metrics.R")
})

assert_project_root()
require_pkgs(c("httr2", "jsonlite", "dplyr", "tidyr", "arrow"))

BASE <- "https://data.cdc.gov/resource/swc5-untb.json"
MEASURES <- c("CASTHMA", "COPD", "CSMOKING", "OBESITY")
VALUE_TYPE <- "Age-adjusted prevalence"

socrata_get_all <- function(url, query, limit = 50000) {
  out <- list()
  offset <- 0
  repeat {
    q <- c(query, list(`$limit` = limit, `$offset` = offset))
    resp <- httr2::request(url) |>
      httr2::req_url_query(!!!q) |>
      httr2::req_headers(`User-Agent` = "air-inequality-atlas (ETL; R)") |>
      httr2::req_perform()
    txt <- httr2::resp_body_string(resp)
    chunk <- jsonlite::fromJSON(txt, simplifyVector = TRUE)
    if (length(chunk) == 0) break
    out[[length(out) + 1]] <- chunk
    if (nrow(chunk) < limit) break
    offset <- offset + limit
  }
  dplyr::bind_rows(out)
}

message("Detecting latest PLACES year available...")
year_df <- socrata_get_all(
  BASE,
  query = list(`$select` = "distinct year", `$order` = "year DESC"),
  limit = 10
)
places_year <- max(as.integer(year_df$year), na.rm = TRUE)
if (!is.finite(places_year)) stop("Unable to determine PLACES year from dataset.", call. = FALSE)
message("Using PLACES year: ", places_year)

where <- paste0(
  "year = '", places_year, "'",
  " AND data_value_type = '", VALUE_TYPE, "'",
  " AND measureid IN ('", paste(MEASURES, collapse = "','"), "')"
)

message("Pulling PLACES county rows (filtered)...")
raw <- socrata_get_all(
  BASE,
  query = list(
    `$select` = paste(
      c(
        "locationid",
        "locationname",
        "stateabbr",
        "measureid",
        "data_value",
        "low_confidence_limit",
        "high_confidence_limit",
        "year",
        "data_value_type"
      ),
      collapse = ","
    ),
    `$where` = where
  ),
  limit = 50000
)

if (nrow(raw) == 0) stop("PLACES query returned 0 rows.", call. = FALSE)

df <- raw |>
  dplyr::transmute(
    fips5 = sprintf("%05d", as.integer(.data$locationid)),
    measureid = as.character(.data$measureid),
    data_value = as.numeric(.data$data_value),
    lcl = as.numeric(.data$low_confidence_limit),
    ucl = as.numeric(.data$high_confidence_limit)
  ) |>
  dplyr::filter(nchar(.data$fips5) == 5)

# Keep only 50 states + DC (exclude territories / PR)
keep_states <- state_crosswalk()$state_fips2
df <- df |>
  dplyr::filter(substr(.data$fips5, 1, 2) %in% keep_states)

wide <- df |>
  tidyr::pivot_wider(
    id_cols = "fips5",
    names_from = "measureid",
    values_from = c("data_value", "lcl", "ucl"),
    names_sep = "__"
  )

out <- wide |>
  dplyr::transmute(
    fips5 = .data$fips5,
    places_year = as.integer(places_year),
    asthma_prev = .data$data_value__CASTHMA,
    asthma_lcl = .data$lcl__CASTHMA,
    asthma_ucl = .data$ucl__CASTHMA,
    copd_prev = .data$data_value__COPD,
    copd_lcl = .data$lcl__COPD,
    copd_ucl = .data$ucl__COPD,
    smoking_prev = .data$data_value__CSMOKING,
    obesity_prev = .data$data_value__OBESITY
  )

out_path <- proj_path("data", "places_county.parquet")
write_parquet(out, out_path)
message("Wrote: ", out_path)
