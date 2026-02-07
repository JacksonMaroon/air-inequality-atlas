suppressPackageStartupMessages({
  source("R/helpers_io.R")
  source("R/helpers_metrics.R")
})

assert_project_root()
require_pkgs(c("sf", "dplyr", "arrow"))

GEO_URL <- "https://www2.census.gov/geo/tiger/GENZ2023/shp/cb_2023_us_county_500k.zip"
SIMPLIFY_TOL_M <- 2000 # meters (in EPSG:3857)

out_rds <- proj_path("data", "geo_county_simplified.rds")
out_master <- proj_path("data", "county_master.parquet")

ensure_dir(proj_path("data-raw", "cache"))
ensure_dir(proj_path("data-raw", "tmp"))

zip_path <- proj_path("data-raw", "cache", "cb_2023_us_county_500k.zip")
if (!file.exists(zip_path)) {
  message("Downloading county boundaries...")
  utils::download.file(GEO_URL, zip_path, mode = "wb", quiet = TRUE)
}

tmp_dir <- proj_path("data-raw", "tmp", "cb_2023_us_county_500k")
if (dir.exists(tmp_dir)) unlink(tmp_dir, recursive = TRUE, force = TRUE)
dir.create(tmp_dir, recursive = TRUE, showWarnings = FALSE)
utils::unzip(zip_path, exdir = tmp_dir)

shp <- list.files(tmp_dir, pattern = "\\.shp$", full.names = TRUE)
if (length(shp) != 1) stop("Expected exactly 1 .shp in the census zip", call. = FALSE)

message("Reading shapefile...")
geo <- sf::st_read(shp[[1]], quiet = TRUE)

needed_cols <- c("GEOID", "STATEFP", "STUSPS", "NAME")
missing_cols <- setdiff(needed_cols, names(geo))
if (length(missing_cols) > 0) {
  stop("Unexpected shapefile schema; missing: ", paste(missing_cols, collapse = ", "), call. = FALSE)
}

county_name_col <- if ("NAMELSAD" %in% names(geo)) "NAMELSAD" else "NAME"

st_xwalk <- state_crosswalk()
keep_states <- st_xwalk$state_fips2

message("Filtering to 50 states + DC...")
geo <- geo |>
  dplyr::filter(.data$STATEFP %in% keep_states) |>
  dplyr::mutate(
    fips5 = as.character(.data$GEOID),
    county_name = as.character(.data[[county_name_col]]),
    state_abbr = as.character(.data$STUSPS),
    state_fips2 = as.character(.data$STATEFP),
    label = paste0(.data$county_name, ", ", .data$state_abbr)
  )

idx <- match(geo$state_fips2, st_xwalk$state_fips2)
geo$region <- st_xwalk$region[idx]
geo$division <- st_xwalk$division[idx]

# Keep only the columns we actually need (reduces RDS size + makes joins safer).
geo <- geo |>
  dplyr::select(
    fips5, county_name, state_abbr, state_fips2, region, division, label,
    geometry
  )

message("Simplifying geometries for Leaflet...")
geo_3857 <- sf::st_transform(geo, 3857)
geo_simpl_3857 <- sf::st_simplify(geo_3857, dTolerance = SIMPLIFY_TOL_M, preserveTopology = TRUE)
geo_simpl <- sf::st_transform(geo_simpl_3857, 4326)

# Some simplifications can introduce invalid geometries; make valid for safer mapping.
geo_simpl <- sf::st_make_valid(geo_simpl)

ensure_dir(dirname(out_rds))
saveRDS(geo_simpl, out_rds)

master <- geo_simpl |>
  sf::st_drop_geometry() |>
  dplyr::as_tibble() |>
  dplyr::select(fips5, county_name, state_abbr, state_fips2, region, division, label)

if (anyDuplicated(master$fips5) > 0) stop("Duplicate fips5 detected in county_master", call. = FALSE)
if (nrow(master) < 3000 || nrow(master) > 4000) {
  stop("Unexpected county count after filtering: ", nrow(master), call. = FALSE)
}

write_parquet(master, out_master)
message("Wrote: ", out_rds)
message("Wrote: ", out_master)

# Clean up extracted shapefile to save disk space (it can always be re-unzipped).
unlink(tmp_dir, recursive = TRUE, force = TRUE)
