suppressPackageStartupMessages({
  source("R/helpers_io.R")
  source("R/helpers_metrics.R")
})

assert_project_root()
require_pkgs(c("dplyr", "jsonlite", "arrow"))

in_dir <- proj_path("data-raw", "aqs", "annual")
if (!dir.exists(in_dir)) {
  stop("Missing AQS cache dir: ", in_dir, "\nRun data-raw/02_pull_aqs_annual.R first.", call. = FALSE)
}

files <- list.files(in_dir, pattern = "\\.json(\\.gz)?$", recursive = TRUE, full.names = TRUE)
if (length(files) == 0) {
  stop("No AQS JSON files found under: ", in_dir, call. = FALSE)
}

parse_one <- function(path) {
  rel <- sub(paste0("^", in_dir, "/?"), "", path)
  parts <- strsplit(rel, "/")[[1]]
  if (length(parts) < 3) return(NULL)

  param <- parts[[1]]
  state <- parts[[2]]
  year <- as.integer(sub("\\.json(\\.gz)?$", "", parts[[3]]))

  con <- if (grepl("\\.gz$", path)) gzfile(path, open = "rb") else file(path, open = "rb")
  txt <- paste(readLines(con, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
  close(con)
  j <- jsonlite::fromJSON(txt, simplifyVector = TRUE)
  dat <- j$Data
  if (is.null(dat) || (is.data.frame(dat) && nrow(dat) == 0)) return(NULL)

  dat <- as.data.frame(dat, stringsAsFactors = FALSE)

  # Derive parameter from payload when present (supports multi-param requests).
  if ("parameter_code" %in% names(dat)) {
    dat$param <- sprintf("%05d", as.integer(dat$parameter_code))
  } else {
    dat$param <- as.character(param)
  }
  dat$year <- as.integer(year)
  dat$state_fips2_from_path <- as.character(state)

  needed <- c("state_code", "county_code", "arithmetic_mean", "param", "year")
  missing <- setdiff(needed, names(dat))
  if (length(missing) > 0) {
    stop("AQS annualData schema missing required fields in ", path, ": ", paste(missing, collapse = ", "), call. = FALSE)
  }

  # Choose weight column once per file.
  weight_col <- NULL
  if ("observation_count" %in% names(dat)) {
    weight_col <- "observation_count"
  } else if ("valid_day_count" %in% names(dat)) {
    weight_col <- "valid_day_count"
  }

  units_col <- if ("units_of_measure" %in% names(dat)) "units_of_measure" else NULL
  site_id <- NULL
  if ("site_number" %in% names(dat)) site_id <- "site_number"
  if (is.null(site_id) && "site" %in% names(dat)) site_id <- "site"

  df <- dat |>
    dplyr::transmute(
      year = as.integer(.data$year),
      param = as.character(.data$param),
      state_code = sprintf("%02d", as.integer(.data$state_code)),
      county_code = sprintf("%03d", as.integer(.data$county_code)),
      fips5 = paste0(.data$state_code, .data$county_code),
      arithmetic_mean = as.numeric(.data$arithmetic_mean),
      weight = if (is.null(weight_col)) 1 else as.numeric(.data[[weight_col]]),
      percent_complete = if ("percent_complete" %in% names(dat)) as.numeric(.data$percent_complete) else NA_real_,
      units = if (is.null(units_col)) NA_character_ else as.character(.data[[units_col]]),
      site = if (is.null(site_id)) NA_character_ else as.character(.data[[site_id]])
    )

  if ("percent_complete" %in% names(dat)) {
    df <- df |>
      dplyr::filter(is.na(.data$percent_complete) | .data$percent_complete >= 75)
  }

  # Convert ozone to ppb (AQS often returns ppm)
  df <- df |>
    dplyr::mutate(
      arithmetic_mean_converted = dplyr::case_when(
        .data$param == "44201" & !is.na(.data$units) &
          grepl("ppm|parts per million", tolower(.data$units)) ~ .data$arithmetic_mean * 1000,
        TRUE ~ .data$arithmetic_mean
      )
    )

  has_site <- !all(is.na(df$site))

  df |>
    dplyr::filter(is.finite(.data$arithmetic_mean_converted)) |>
    dplyr::group_by(.data$fips5, .data$year, .data$param) |>
    dplyr::summarise(
      mean_value = weighted_mean_na(.data$arithmetic_mean_converted, .data$weight),
      monitors_n = if (has_site) dplyr::n_distinct(.data$site) else dplyr::n(),
      obs_total = sum(dplyr::if_else(is.finite(.data$weight) & .data$weight > 0, .data$weight, 0), na.rm = TRUE),
      pct_complete_mean = if ("percent_complete" %in% names(dat)) mean(.data$percent_complete, na.rm = TRUE) else NA_real_,
      .groups = "drop"
    )
}

message("Parsing AQS JSON cache (", length(files), " files)...")
dfs <- lapply(files, parse_one)
dfs <- dfs[!vapply(dfs, is.null, logical(1))]
if (length(dfs) == 0) stop("No AQS rows found after parsing.", call. = FALSE)

aqs <- dplyr::bind_rows(dfs)
pm25 <- aqs |>
  dplyr::filter(.data$param == "88101") |>
  dplyr::transmute(
    fips5 = .data$fips5,
    year = .data$year,
    pm25_mean_ugm3 = .data$mean_value,
    pm25_monitors_n = as.integer(.data$monitors_n),
    pm25_obs_total = .data$obs_total,
    pm25_pct_complete_mean = .data$pct_complete_mean
  )

oz <- aqs |>
  dplyr::filter(.data$param == "44201") |>
  dplyr::transmute(
    fips5 = .data$fips5,
    year = .data$year,
    ozone_mean_ppb = .data$mean_value,
    ozone_monitors_n = as.integer(.data$monitors_n),
    ozone_obs_total = .data$obs_total,
    ozone_pct_complete_mean = .data$pct_complete_mean
  )

out <- dplyr::full_join(pm25, oz, by = c("fips5", "year")) |>
  dplyr::arrange(.data$fips5, .data$year)

out_path <- proj_path("data", "aqs_county_year.parquet")
write_parquet(out, out_path)
message("Wrote: ", out_path)
