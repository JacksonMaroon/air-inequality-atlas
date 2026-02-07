suppressPackageStartupMessages({
  source("R/helpers_io.R")
  source("R/helpers_metrics.R")
})

assert_project_root()
require_pkgs(c("dplyr", "jsonlite", "arrow"))

paths <- list(
  county_master = proj_path("data", "county_master.parquet"),
  aqs = proj_path("data", "aqs_county_year.parquet"),
  places = proj_path("data", "places_county.parquet"),
  svi = proj_path("data", "svi_county_2022.parquet")
)
missing <- names(paths)[!vapply(paths, file.exists, logical(1))]
if (length(missing) > 0) {
  stop(
    "Missing required input(s): ",
    paste(missing, collapse = ", "),
    "\nRun ETL steps 01–05 first.",
    call. = FALSE
  )
}

county_master <- read_parquet(paths$county_master)
aqs <- read_parquet(paths$aqs)
places <- read_parquet(paths$places)
svi <- read_parquet(paths$svi)

# Enforce fixed output schema for county_analytic.parquet.
county_master <- county_master |>
  dplyr::select(fips5, state_abbr, county_name, region, division)

places_years <- unique(places$places_year)
if (length(places_years) != 1) stop("Expected exactly 1 places_year in places_county.", call. = FALSE)
places_year <- as.integer(places_years[[1]])

aqs_years <- sort(unique(as.integer(aqs$year)))
anchor_year <- nearest_year(places_year, aqs_years)

message("PLACES year: ", places_year)
message("AQS anchor year: ", anchor_year)

aqs_anchor <- aqs |>
  dplyr::filter(.data$year == anchor_year) |>
  dplyr::transmute(
    fips5 = .data$fips5,
    pm25_mean_ugm3_anchor = .data$pm25_mean_ugm3,
    pm25_monitors_n_anchor = .data$pm25_monitors_n,
    ozone_mean_ppb_anchor = .data$ozone_mean_ppb,
    ozone_monitors_n_anchor = .data$ozone_monitors_n
  )

df <- county_master |>
  dplyr::left_join(aqs_anchor, by = "fips5") |>
  dplyr::left_join(places, by = "fips5") |>
  dplyr::left_join(svi, by = "fips5") |>
  dplyr::mutate(
    pollution_anchor_year = as.integer(anchor_year),
    has_pm25_anchor = dplyr::coalesce(as.integer(.data$pm25_monitors_n_anchor), 0L) > 0L,
    has_ozone_anchor = dplyr::coalesce(as.integer(.data$ozone_monitors_n_anchor), 0L) > 0L
  )

df <- df |>
  dplyr::mutate(
    z_pm25_anchor = zscore(.data$pm25_mean_ugm3_anchor),
    z_ozone_anchor = zscore(.data$ozone_mean_ppb_anchor),
    z_asthma = zscore(.data$asthma_prev),
    z_svi = zscore(.data$svi_overall),
    cbi = cbi_compute(.data$z_pm25_anchor, .data$z_ozone_anchor, .data$z_asthma, .data$z_svi),
    cbi_decile = dplyr::if_else(is.finite(.data$cbi), dplyr::ntile(.data$cbi, 10L), NA_integer_)
  )

if (nrow(df) != nrow(county_master)) stop("Join changed county row count (duplication likely).", call. = FALSE)
if (anyDuplicated(df$fips5) > 0) stop("Duplicate fips5 after joins.", call. = FALSE)

out_path <- proj_path("data", "county_analytic.parquet")
out <- df |>
  dplyr::select(
    fips5,
    state_abbr,
    county_name,
    region,
    division,
    pollution_anchor_year,
    pm25_mean_ugm3_anchor,
    ozone_mean_ppb_anchor,
    places_year,
    asthma_prev,
    asthma_lcl,
    asthma_ucl,
    copd_prev,
    copd_lcl,
    copd_ucl,
    smoking_prev,
    obesity_prev,
    svi_overall,
    svi_theme1,
    svi_theme2,
    svi_theme3,
    svi_theme4,
    has_pm25_anchor,
    has_ozone_anchor,
    z_pm25_anchor,
    z_ozone_anchor,
    z_asthma,
    z_svi,
    cbi,
    cbi_decile
  )
write_parquet(out, out_path)

computed_vintage <- paste0("Computed using AQS ", anchor_year, " + PLACES ", places_year, " + SVI 2022")

dict <- data.frame(
  column = c(
    "fips5",
    "state_abbr",
    "county_name",
    "region",
    "division",
    "pollution_anchor_year",
    "pm25_mean_ugm3_anchor",
    "ozone_mean_ppb_anchor",
    "places_year",
    "asthma_prev",
    "asthma_lcl",
    "asthma_ucl",
    "copd_prev",
    "copd_lcl",
    "copd_ucl",
    "smoking_prev",
    "obesity_prev",
    "svi_overall",
    "svi_theme1",
    "svi_theme2",
    "svi_theme3",
    "svi_theme4",
    "has_pm25_anchor",
    "has_ozone_anchor",
    "z_pm25_anchor",
    "z_ozone_anchor",
    "z_asthma",
    "z_svi",
    "cbi",
    "cbi_decile"
  ),
  label = c(
    "County FIPS (5-digit)",
    "State abbreviation",
    "County name (Census NAMELSAD/NAME)",
    "Census region",
    "Census division",
    "Pollution anchor year (AQS year aligned to PLACES)",
    "PM2.5 annual mean (AQS; county-year aggregated; anchor year)",
    "Ozone annual mean (AQS; county-year aggregated; anchor year; ppb)",
    "PLACES snapshot year",
    "Asthma prevalence (PLACES; age-adjusted; %)",
    "Asthma low CI bound (PLACES; %)",
    "Asthma high CI bound (PLACES; %)",
    "COPD prevalence (PLACES; age-adjusted; %)",
    "COPD low CI bound (PLACES; %)",
    "COPD high CI bound (PLACES; %)",
    "Smoking prevalence (PLACES; age-adjusted; %)",
    "Obesity prevalence (PLACES; age-adjusted; %)",
    "SVI overall percentile (0-1)",
    "SVI Theme 1 percentile (0-1)",
    "SVI Theme 2 percentile (0-1)",
    "SVI Theme 3 percentile (0-1)",
    "SVI Theme 4 percentile (0-1)",
    "Has PM2.5 coverage in anchor year (monitor count > 0)",
    "Has ozone coverage in anchor year (monitor count > 0)",
    "PM2.5 z-score (national; anchor year)",
    "Ozone z-score (national; anchor year)",
    "Asthma z-score (national; PLACES snapshot)",
    "SVI z-score (national; SVI 2022)",
    "Compound Burden Index (z-score composite)",
    "CBI decile (1=lowest burden, 10=highest)"
  ),
  units = c(
    NA, NA, NA, NA, NA,
    "year",
    "ug/m3",
    "ppb",
    "year",
    "percent",
    "percent",
    "percent",
    "percent",
    "percent",
    "percent",
    "percent",
    "percent",
    "0-1",
    "0-1",
    "0-1",
    "0-1",
    "0-1",
    NA,
    NA,
    "z-score",
    "z-score",
    "z-score",
    "z-score",
    "z-score",
    "decile"
  ),
  source = c(
    "Census",
    "Census",
    "Census",
    "Census",
    "Census",
    "Derived",
    "EPA AQS annualData/byState",
    "EPA AQS annualData/byState",
    "CDC PLACES (swc5-untb)",
    "CDC PLACES (swc5-untb)",
    "CDC PLACES (swc5-untb)",
    "CDC PLACES (swc5-untb)",
    "CDC PLACES (swc5-untb)",
    "CDC PLACES (swc5-untb)",
    "CDC PLACES (swc5-untb)",
    "CDC PLACES (swc5-untb)",
    "CDC PLACES (swc5-untb)",
    "CDC/ATSDR SVI 2022",
    "CDC/ATSDR SVI 2022",
    "CDC/ATSDR SVI 2022",
    "CDC/ATSDR SVI 2022",
    "CDC/ATSDR SVI 2022",
    "Derived",
    "Derived",
    "Derived",
    "Derived",
    "Derived",
    "Derived",
    "Derived",
    "Derived"
  ),
  vintage = c(
    "CB 2023",
    "CB 2023",
    "CB 2023",
    "CB 2023",
    "CB 2023",
    paste0("AQS nearest to PLACES ", places_year),
    paste0("AQS annual ", anchor_year),
    paste0("AQS annual ", anchor_year),
    paste0("PLACES ", places_year),
    paste0("PLACES ", places_year),
    paste0("PLACES ", places_year),
    paste0("PLACES ", places_year),
    paste0("PLACES ", places_year),
    paste0("PLACES ", places_year),
    paste0("PLACES ", places_year),
    paste0("PLACES ", places_year),
    paste0("PLACES ", places_year),
    "SVI 2022",
    "SVI 2022",
    "SVI 2022",
    "SVI 2022",
    "SVI 2022",
    computed_vintage,
    computed_vintage,
    computed_vintage,
    computed_vintage,
    computed_vintage,
    computed_vintage,
    computed_vintage,
    computed_vintage
  ),
  notes = c(
    NA,
    NA,
    NA,
    NA,
    NA,
    "Selected as nearest available AQS year to PLACES snapshot year",
    "County-year aggregated from monitor summaries; completeness filter applied when available",
    "Converted to ppb when AQS units are ppm",
    "PLACES is model-based; do not use for county-level trend inference",
    "Model-based estimate; not for county trend inference",
    "Model-based estimate; not for county trend inference",
    "Model-based estimate; not for county trend inference",
    "Model-based estimate; not for county trend inference",
    "Model-based estimate; not for county trend inference",
    "Model-based estimate; not for county trend inference",
    "Model-based estimate; not for county trend inference",
    "Model-based estimate; not for county trend inference",
    "Percentile rank; not comparable across SVI years",
    "Percentile rank; not comparable across SVI years",
    "Percentile rank; not comparable across SVI years",
    "Percentile rank; not comparable across SVI years",
    "Percentile rank; not comparable across SVI years",
    "TRUE when monitor count > 0 in the anchor year",
    "TRUE when monitor count > 0 in the anchor year",
    "Standardized nationally (mean 0, sd 1) on non-missing values",
    "Standardized nationally (mean 0, sd 1) on non-missing values",
    "Standardized nationally (mean 0, sd 1) on non-missing values",
    "Standardized nationally (mean 0, sd 1) on non-missing values",
    "Composite of z-scores: 0.4*PM2.5 + 0.2*ozone + 0.2*asthma + 0.2*SVI",
    "Deciles computed nationally on non-missing CBI"
  ),
  stringsAsFactors = FALSE
)

dict_path <- proj_path("data", "data_dictionary.parquet")
write_parquet(dict, dict_path)

vintages <- list(
  build_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
  geometry = list(source = "cb_2023_us_county_500k", year = 2023),
  aqs = list(years = aqs_years, anchor_year = anchor_year, params = c(pm25 = "88101", ozone = "44201")),
  places = list(dataset = "swc5-untb", places_year = places_year, value_type = "Age-adjusted prevalence"),
  svi = list(year = 2022, file = "SVI_2022_US_county.csv")
)

vintages_path <- proj_path("data", "vintages.json")
jsonlite::write_json(vintages, vintages_path, pretty = TRUE, auto_unbox = TRUE)

message("Wrote: ", out_path)
message("Wrote: ", dict_path)
message("Wrote: ", vintages_path)
