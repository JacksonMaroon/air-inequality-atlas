assert_project_root <- function() {
  # `data-raw/` is used for ETL scripts + caches, but it is not required for
  # running the deployed Shiny app (and is typically excluded from deployment
  # bundles via `.rscignore`).
  needed <- c("R", "data", "www")
  missing <- needed[!dir.exists(needed)]
  if (length(missing) > 0) {
    stop(
      "Run this from the project root (missing dirs: ",
      paste(missing, collapse = ", "),
      "). Current working directory: ",
      getwd(),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

proj_path <- function(...) {
  assert_project_root()
  file.path(getwd(), ...)
}

ensure_dir <- function(path) {
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

require_pkgs <- function(pkgs) {
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    stop(
      "Missing required R packages: ",
      paste(missing, collapse = ", "),
      "\nInstall them (recommended): renv::restore()",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

read_parquet <- function(path) {
  require_pkgs(c("arrow"))
  arrow::read_parquet(path)
}

write_parquet <- function(x, path) {
  require_pkgs(c("arrow"))
  ensure_dir(dirname(path))
  arrow::write_parquet(x, path)
  invisible(path)
}

# Normalize an sf::st_bbox() style bbox to fitBounds args.
# Returns c(lng1, lat1, lng2, lat2) or NULL if inputs are unusable.
bbox_to_lnglat <- function(bb) {
  if (is.null(bb)) return(NULL)
  needed <- c("xmin", "ymin", "xmax", "ymax")
  if (!all(needed %in% names(bb))) return(NULL)

  vals <- suppressWarnings(as.numeric(bb[needed]))
  if (length(vals) != 4 || any(!is.finite(vals))) return(NULL)

  # Default: assume bbox is x=lng, y=lat.
  a <- c(lng1 = vals[[1]], lat1 = vals[[2]], lng2 = vals[[3]], lat2 = vals[[4]])
  # Alternate: some environments can surface axis-order confusion for EPSG:4326.
  b <- c(lng1 = vals[[2]], lat1 = vals[[1]], lng2 = vals[[4]], lat2 = vals[[3]])

  is_valid <- function(v) {
    all(is.finite(v)) &&
      all(v[c("lat1", "lat2")] >= -90 & v[c("lat1", "lat2")] <= 90) &&
      all(v[c("lng1", "lng2")] >= -180 & v[c("lng1", "lng2")] <= 180)
  }

  if (is_valid(a)) return(a)
  if (is_valid(b)) return(b)
  NULL
}

leaflet_fit_bounds_safe <- function(map,
                                    bb,
                                    fallback_lng = -98.35,
                                    fallback_lat = 39.5,
                                    fallback_zoom = 4) {
  args <- bbox_to_lnglat(bb)
  if (is.null(args)) {
    return(leaflet::setView(map, lng = fallback_lng, lat = fallback_lat, zoom = fallback_zoom))
  }

  leaflet::fitBounds(map, args[["lng1"]], args[["lat1"]], args[["lng2"]], args[["lat2"]])
}
