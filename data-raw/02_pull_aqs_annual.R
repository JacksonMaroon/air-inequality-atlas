suppressPackageStartupMessages({
  source("R/helpers_io.R")
  source("R/helpers_metrics.R")
})

assert_project_root()
require_pkgs(c("httr2", "jsonlite"))

aqs_email <- Sys.getenv("AQS_EMAIL")
aqs_key <- Sys.getenv("AQS_KEY")
if (aqs_email == "" || aqs_key == "") {
  stop(
    "Missing AQS credentials. Set env vars AQS_EMAIL and AQS_KEY before running AQS ETL.",
    call. = FALSE
  )
}

YEARS <- 2012:2023
PARAMS <- c("88101", "44201")
STATES <- state_crosswalk()$state_fips2

out_dir <- proj_path("data-raw", "aqs", "annual")
ensure_dir(out_dir)

# When TRUE (default), request multiple params in a single call to reduce total
# request count and runtime (still respecting the 10/min limit).
combined_params <- Sys.getenv("AQS_COMBINED_PARAMS", "1") != "0"

# Cache format. Raw annualData payloads can be large across years/states, so we
# gzip-compress by default to reduce disk use (still "raw JSON", just compressed).
cache_gzip <- Sys.getenv("AQS_CACHE_GZIP", "1") != "0"
cache_ext <- if (cache_gzip) "json.gz" else "json"

aqs_get_json <- function(url, query) {
  req <- httr2::request(url) |>
    httr2::req_url_query(!!!query) |>
    httr2::req_headers(`User-Agent` = "air-inequality-atlas (ETL; R)") |>
    # Fail fast on slow/stalled transfers; we'll retry with backoff.
    httr2::req_timeout(180) |>
    httr2::req_options(low_speed_time = 60, low_speed_limit = 1)

  transient_status <- c(408, 425, 429, 500, 502, 503, 504)

  for (i in seq_len(6)) {
    resp <- tryCatch(
      httr2::req_perform(req),
      error = function(e) e
    )

    if (!inherits(resp, "error")) {
      return(httr2::resp_body_string(resp))
    }

    # Retry on network/curl timeouts and on known transient HTTP statuses.
    status <- tryCatch(httr2::resp_status(resp$resp), error = function(e) NA_integer_)
    is_transient <- is.na(status) || status %in% transient_status

    if (!is_transient || i == 6) stop(resp)

    Sys.sleep(min(60, 2^i))
  }

  stop("Unreachable: AQS retry loop exhausted unexpectedly.", call. = FALSE)
}

endpoint <- "https://aqs.epa.gov/data/api/annualData/byState"

for (year in YEARS) {
  bdate <- paste0(year, "0101")
  edate <- paste0(year, "1231")

  for (state in STATES) {
    if (combined_params) {
      param <- paste(PARAMS, collapse = ",")
      state_dir <- file.path(out_dir, "multi", state)
      ensure_dir(state_dir)
      out_file <- file.path(state_dir, paste0(year, ".", cache_ext))

      if (file.exists(out_file) && file.info(out_file)$size > 0) next

      message("AQS annualData/byState: year=", year, " state=", state, " params=", param)

      txt <- aqs_get_json(endpoint, list(
        email = aqs_email,
        key = aqs_key,
        param = param,
        bdate = bdate,
        edate = edate,
        state = state
      ))

      tmp_file <- paste0(out_file, ".tmp")
      if (cache_gzip) {
        con <- gzfile(tmp_file, open = "wb")
        writeLines(txt, con = con, useBytes = TRUE)
        close(con)
      } else {
        writeLines(txt, tmp_file, useBytes = TRUE)
      }
      file.rename(tmp_file, out_file)

      # Basic header validation (don't fail on 'no data', but do on hard failures)
      j <- jsonlite::fromJSON(txt, simplifyVector = TRUE)
      status <- tryCatch(j$Header$status, error = function(e) NA_character_)
      if (!is.na(status) && grepl("^failed", tolower(status))) {
        stop("AQS request failed (status: ", status, ") for ", out_file, call. = FALSE)
      }

      # Rate-limit guardrail: <= 10 req / minute
      Sys.sleep(6)
    } else {
      for (param in PARAMS) {
        state_dir <- file.path(out_dir, param, state)
        ensure_dir(state_dir)
        out_file <- file.path(state_dir, paste0(year, ".", cache_ext))

        if (file.exists(out_file) && file.info(out_file)$size > 0) next

        message("AQS annualData/byState: year=", year, " state=", state, " param=", param)

        txt <- aqs_get_json(endpoint, list(
          email = aqs_email,
          key = aqs_key,
          param = param,
          bdate = bdate,
          edate = edate,
          state = state
        ))

        if (cache_gzip) {
          con <- gzfile(out_file, open = "wb")
          writeLines(txt, con = con, useBytes = TRUE)
          close(con)
        } else {
          writeLines(txt, out_file, useBytes = TRUE)
        }

        # Basic header validation (don't fail on 'no data', but do on hard failures)
        j <- jsonlite::fromJSON(txt, simplifyVector = TRUE)
        status <- tryCatch(j$Header$status, error = function(e) NA_character_)
        if (!is.na(status) && grepl("^failed", tolower(status))) {
          stop("AQS request failed (status: ", status, ") for ", out_file, call. = FALSE)
        }

        # Rate-limit guardrail: <= 10 req / minute
        Sys.sleep(6)
      }
    }
  }
}

message("AQS annual pull complete (cached under data-raw/aqs/annual/).")
