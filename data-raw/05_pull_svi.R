suppressPackageStartupMessages({
  source("R/helpers_io.R")
  source("R/helpers_metrics.R")
})

assert_project_root()
require_pkgs(c("dplyr", "arrow"))

SVI_URL <- "https://svi.cdc.gov/Documents/Data/2022/csv/states_counties/SVI_2022_US_county.csv"
out_path <- proj_path("data", "svi_county_2022.parquet")

ensure_dir(proj_path("data-raw", "cache"))
csv_path <- proj_path("data-raw", "cache", "SVI_2022_US_county.csv")
if (!file.exists(csv_path)) {
  message("Downloading SVI 2022 county CSV...")
  utils::download.file(SVI_URL, csv_path, mode = "wb", quiet = TRUE)
}

message("Reading SVI CSV...")
dt <- utils::read.csv(
  csv_path,
  fileEncoding = "UTF-8-BOM",
  stringsAsFactors = FALSE,
  check.names = FALSE
)

needed <- c("FIPS", "RPL_THEMES", "RPL_THEME1", "RPL_THEME2", "RPL_THEME3", "RPL_THEME4")
missing <- setdiff(needed, names(dt))
if (length(missing) > 0) {
  stop("SVI CSV missing expected columns: ", paste(missing, collapse = ", "), call. = FALSE)
}

st_xwalk <- state_crosswalk()
keep_states <- st_xwalk$state_fips2

out <- dt |>
  dplyr::transmute(
    fips5 = sprintf("%05d", as.integer(.data$FIPS)),
    svi_overall = as.numeric(.data$RPL_THEMES),
    svi_theme1 = as.numeric(.data$RPL_THEME1),
    svi_theme2 = as.numeric(.data$RPL_THEME2),
    svi_theme3 = as.numeric(.data$RPL_THEME3),
    svi_theme4 = as.numeric(.data$RPL_THEME4)
  ) |>
  dplyr::filter(nchar(.data$fips5) == 5) |>
  dplyr::filter(substr(.data$fips5, 1, 2) %in% keep_states)

write_parquet(out, out_path)
message("Wrote: ", out_path)
